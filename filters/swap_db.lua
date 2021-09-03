--- function name must be `filter`
---
--- arguments:
--- @id number: the sequence of the cmd
--- @is_base boolean: whether the command is decoded from dump.rdb file
--- @group string: the group of cmd
--- @cmd_name string: cmd name
--- @keys table: keys of the command
--- @slots table: slots of the command
--- @db_id: database id
--- @timestamp_ms number: timestamp in milliseconds, 0 if not available

--- return:
--- @code number:
--- * 0: allow
--- * 1: disallow
--- * 2: error occurred
--- @db_id number: redirection database id

--- dbid: 0 -> 1
--- dbid: 1 -> 0
--- dbid: others -> drop
function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if db_id == 0 then
        -- print("db_id is 0, redirect to 1")
        return 0, 1
    elseif db_id == 1 then
        -- print("db_id is 1, redirect to 0")
        return 0, 0
    else
        return 1, db_id
    end
end