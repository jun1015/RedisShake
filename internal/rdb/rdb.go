package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"github.com/alibaba/RedisShake/internal/rdb/types"
	"github.com/alibaba/RedisShake/internal/utils"
	"io"
	"os"
	"strconv"
)

const (
	kFlagFunction2 = 245  // function library data
	kFlagFunction  = 246  // old function library data for 7.0 rc1 and rc2
	kFlagModuleAux = 247  // Module auxiliary data.
	kFlagIdle      = 0xf8 // LRU idle time.
	kFlagFreq      = 0xf9 // LFU frequency.
	kFlagAUX       = 0xfa // RDB aux field.
	kFlagResizeDB  = 0xfb // Hash table resize hint.
	kFlagExpireMs  = 0xfc // Expire time in milliseconds.
	kFlagExpire    = 0xfd // Old expire time in seconds.
	kFlagSelect    = 0xfe // DB number of the following keys.
	kEOF           = 0xff // End of the RDB file.
)

type Loader struct {
	replStreamDbId int // https://github.com/alibaba/RedisShake/pull/430#issuecomment-1099014464

	nowDBId  int
	expireAt uint64

	filPath string
	ch      chan *entry.Entry
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

func (ld *Loader) ParseRDB() int {
	fp, err := os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. file_path=[%s], error=[%s]", ld.filPath, err)
	}
	rd := bufio.NewReader(fp)
	//magic + version
	buf := make([]byte, 9)
	_, err = io.ReadFull(rd, buf)
	if err != nil {
		log.PanicError(err)
	}
	if !bytes.Equal(buf[:5], []byte("REDIS")) {
		log.Panicf("verify magic string, invalid file format. bytes=[%v]", buf[:5])
	}
	version, err := strconv.Atoi(string(buf[5:]))
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("RDB version: %d", version)

	// read entries
	ld.parseRDBEntry(rd)

	return ld.replStreamDbId
}

func (ld *Loader) parseRDBEntry(rd *bufio.Reader) {
	// read one entry
	for true {
		typeByte := structure.ReadByte(rd)
		switch typeByte {
		case kFlagIdle:
			idle := structure.ReadLength(rd)
			log.Infof("RDB idle: %d", idle)
		case kFlagFreq:
			freq := structure.ReadByte(rd)
			log.Infof("RDB freq: %d", freq)
		case kFlagAUX:
			key := structure.ReadString(rd)
			value := structure.ReadString(rd)
			if key == "repl-stream-db" {
				var err error
				ld.replStreamDbId, err = strconv.Atoi(value)
				if err != nil {
					log.PanicError(err)
				}
				log.Infof("RDB repl-stream-db: %d", ld.replStreamDbId)
			} else {
				log.Infof("RDB AUX fields. key=[%s], value=[%s]", key, value)
			}
		case kFlagResizeDB:
			dbSize := structure.ReadLength(rd)
			expireSize := structure.ReadLength(rd)
			log.Infof("RDB resize db. db_size=[%d], expire_size=[%d]", dbSize, expireSize)
		case kFlagExpireMs:
			ld.expireAt = structure.ReadUint64(rd)
			log.Debugf("RDB expire at %d", ld.expireAt)
		case kFlagExpire:
			ld.expireAt = uint64(structure.ReadUint32(rd)) * 1000
			log.Debugf("RDB expire at %d", ld.expireAt)
		case kFlagSelect:
			dbid := structure.ReadLength(rd)
			ld.nowDBId = int(dbid)
			log.Debugf("RDB select db, DbId=[%d]", dbid)
		case kEOF:
			return
		default:
			key := structure.ReadString(rd)
			var value bytes.Buffer
			anotherReader := io.TeeReader(rd, &value)
			o := types.ParseObject(anotherReader, typeByte, key)
			if uint64(value.Len()) > config.Config.Advanced.TargetRedisProtoMaxBulkLen {
				cmds := o.Rewrite()
				for _, cmd := range cmds {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = cmd
					ld.ch <- e
				}
				if ld.expireAt != 0 {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = []string{"PEXPIREAT", key, strconv.FormatUint(ld.expireAt, 10)}
					ld.ch <- e
				}
			} else {
				e := entry.NewEntry()
				e.IsBase = true
				e.DbId = ld.nowDBId
				v := ld.createValueDump(typeByte, value.Bytes())
				e.Argv = []string{"restore", key, strconv.FormatUint(ld.expireAt, 10), v}
				if config.Config.Advanced.RDBRestoreCommandBehavior == "rewrite" {
					e.Argv = append(e.Argv, "replace")
				}
				if ld.expireAt != 0 {
					e.Argv = append(e.Argv, "absttl")
				}
				ld.ch <- e
			}
			ld.expireAt = 0
		}
	}
}

func (ld *Loader) createValueDump(typeByte byte, val []byte) string {
	var b bytes.Buffer
	c := utils.NewDigest()
	w := io.MultiWriter(&b, c)
	_, _ = w.Write([]byte{typeByte})
	_, _ = w.Write(val)
	_ = binary.Write(w, binary.LittleEndian, uint16(6))
	_ = binary.Write(w, binary.LittleEndian, c.Sum64())
	return b.String()
}
