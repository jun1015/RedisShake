# redis-shake

[![CI](https://github.com/alibaba/RedisShake/actions/workflows/ci.yml/badge.svg?branch=v3)](https://github.com/alibaba/RedisShake/actions/workflows/ci.yml)

## 特性

* 支持 Redis 原生数据结构
* 支持源端为单机实例，目的端为单机或集群实例
* 测试在 5.0、6.0 和 7.0
* 支持使用 lua 自定义过滤规则

# 文档

## 安装

### 从 Release 下载安装

unstable 版本，暂不支持。

### 从源码编译

下载源码后，运行 `sh build.sh` 命令编译。

```shell
sh build.sh
```

## 运行

1. 编辑 redis-shake.toml，修改其中的 source 与 target 配置项
2. 启动 redis-shake：

```shell
./bin/redis-shake redis-shake.toml
```

3. 观察数据同步情况

## 数据过滤

redis-shake 支持使用 lua 自定义过滤规则，可以实现对数据进行过滤。
具体参照 `filter/*.lua` 文件。

redis-shake 启动命令：

```shell
./bin/redis-shake redis-shake.toml filter/xxx.lua
```

# 贡献

## Redis Module 支持

1. 在 `internal/rdb/types` 下添加相关类型。
2. 在 `scripts/commands` 下添加相关命令，并使用脚本生成 `table.go` 文件，移动至 `internal/commands` 目录。
3. 在 `test/cases` 下添加相关测试用例。
4. 在 `README.md` 中添加相关文档并自豪地留下你的名字。

# 感谢
