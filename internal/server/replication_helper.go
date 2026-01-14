package server

// isWriteCommand 检查是否是写命令
func isWriteCommand(cmd string) bool {
	writeCommands := map[string]bool{
		"SET": true, "SETEX": true, "PSETEX": true, "SETNX": true,
		"GETSET": true, "MSET": true, "MSETNX": true,
		"INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true,
		"INCRBYFLOAT": true, "APPEND": true, "SETRANGE": true,
		"DEL": true, "EXPIRE": true, "EXPIREAT": true,
		"PEXPIRE": true, "PEXPIREAT": true, "PERSIST": true,
		"RENAME": true, "RENAMENX": true,
		"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
		"LSET": true, "LTRIM": true, "LINSERT": true, "LREM": true,
		"RPOPLPUSH": true, "LPUSHX": true, "RPUSHX": true,
		"HSET": true, "HDEL": true, "HMSET": true, "HSETNX": true,
		"HINCRBY": true, "HINCRBYFLOAT": true,
		"SADD": true, "SREM": true, "SPOP": true, "SMOVE": true,
		"SINTERSTORE": true, "SUNIONSTORE": true, "SDIFFSTORE": true,
		"ZADD": true, "ZREM": true, "ZINCRBY": true,
	}
	return writeCommands[cmd]
}
