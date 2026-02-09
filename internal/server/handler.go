package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

type Handler struct {
	Db          *store.BotreonStore
	Cluster     *cluster.Cluster
	Replication *replication.ReplicationManager
	Backup      *backup.BackupManager
	PubSub      *store.PubSubManager
}

// ServeTCP 监听并处理连接
func (h *Handler) ServeTCP(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go h.handleConnection(conn)
	}
}

func (h *Handler) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("新连接建立")
	defer func() {
		logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("连接关闭")
		if err := conn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close connection")
		}
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer func() {
		if err := writer.Flush(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to flush writer")
		}
	}()

	// 设置 TCP_NODELAY 以减少延迟
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to set TCP_NODELAY")
		}
	}

	for {
		req, err := proto.ReadRESP(reader)
		if err != nil {
			// 连接关闭或读取错误，直接返回
			// 不发送错误响应，因为连接可能已关闭
			// 这可能是正常的连接关闭（如 redis-benchmark 完成测试后关闭连接）
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("读取请求失败")
			return
		}
		args := req.Args
		if len(args) == 0 {
			logger.Logger.Warn().Str("remote_addr", remoteAddr).Msg("收到空命令")
			_ = proto.WriteRESP(writer, proto.NewError("ERR no command"))
			if err := writer.Flush(); err != nil {
				logger.Logger.Debug().Err(err).Msg("failed to flush writer")
			}
			continue
		}
		cmd := strings.ToUpper(string(args[0]))
		logger.Logger.Debug().
			Str("remote_addr", remoteAddr).
			Str("command", cmd).
			Int("arg_count", len(args)-1).
			Msg("执行命令")

		resp := h.executeCommand(cmd, args[1:])
		if resp == nil {
			// 如果响应为 nil，返回错误
			logger.Logger.Error().
				Str("remote_addr", remoteAddr).
				Str("command", cmd).
				Msg("命令执行返回 nil")
			resp = proto.NewError("ERR internal error")
		}

		// 如果是主节点且是写命令，传播到从节点
		if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) {
			// 传播命令（排除复制相关命令）
			if cmd != "REPLICAOF" && cmd != "PSYNC" && cmd != "REPLCONF" {
				h.Replication.PropagateCommand(req.Args)
			}
		}

		logger.Logger.Debug().
			Str("remote_addr", remoteAddr).
			Str("command", cmd).
			Str("response_type", getResponseType(resp)).
			Msg("发送响应")

		if err := proto.WriteRESP(writer, resp); err != nil {
			// 写入错误，连接可能已关闭
			logger.Logger.Warn().
				Str("remote_addr", remoteAddr).
				Err(err).
				Msg("写入响应失败")
			return
		}
		if err := writer.Flush(); err != nil {
			// 刷新错误，连接可能已关闭
			logger.Logger.Warn().
				Str("remote_addr", remoteAddr).
				Err(err).
				Msg("刷新缓冲区失败")
			return
		}
	}
}

// getResponseType 获取响应类型（用于日志）
func getResponseType(resp proto.RESP) string {
	switch resp.(type) {
	case *proto.SimpleString:
		return "SimpleString"
	case *proto.BulkString:
		return "BulkString"
	case proto.Error:
		return "Error"
	case proto.Integer:
		return "Integer"
	case *proto.Array:
		return "Array"
	default:
		return "Unknown"
	}
}

func (h *Handler) executeCommand(cmd string, args [][]byte) proto.RESP {
	switch cmd {
	// 连接命令
	case "PING":
		return proto.NewSimpleString("PONG")

	case "ECHO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ECHO' command")
		}
		return proto.NewBulkString(args[0])

	// String命令
	case "SET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SET' command")
		}
		key, value := string(args[0]), string(args[1])
		if err := h.Db.Set(key, value); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'GET' command")
		}
		key := string(args[0])
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(value))

	case "SETEX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETEX' command")
		}
		key, value := string(args[0]), string(args[2])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR invalid integer")
		}
		if err := h.Db.SetEX(key, value, seconds); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "PSETEX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'PSETEX' command")
		}
		key, value := string(args[0]), string(args[2])
		milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR invalid integer")
		}
		if err := h.Db.PSETEX(key, value, milliseconds); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "SETNX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SETNX' command")
		}
		key, value := string(args[0]), string(args[1])
		success, err := h.Db.SetNX(key, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "GETSET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GETSET' command")
		}
		key, value := string(args[0]), string(args[1])
		oldValue, err := h.Db.GetSet(key, value)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(oldValue))

	case "MGET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'MGET' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		values, err := h.Db.MGet(keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(values))
		for i, v := range values {
			if v == "" {
				results[i] = nil
			} else {
				results[i] = []byte(v)
			}
		}
		return &proto.Array{Args: results}

	case "MSET":
		if len(args) < 2 || len(args)%2 != 0 {
			return proto.NewError("ERR wrong number of arguments for 'MSET' command")
		}
		pairs := make([]string, len(args))
		for i, arg := range args {
			pairs[i] = string(arg)
		}
		if err := h.Db.MSet(pairs...); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "MSETNX":
		if len(args) < 2 || len(args)%2 != 0 {
			return proto.NewError("ERR wrong number of arguments for 'MSETNX' command")
		}
		pairs := make([]string, len(args))
		for i, arg := range args {
			pairs[i] = string(arg)
		}
		success, err := h.Db.MSetNX(pairs...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "INCR":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'INCR' command")
		}
		key := string(args[0])
		value, err := h.Db.INCR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "INCRBY":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'INCRBY' command")
		}
		key := string(args[0])
		increment, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.INCRBY(key, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "DECR":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DECR' command")
		}
		key := string(args[0])
		value, err := h.Db.DECR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "DECRBY":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'DECRBY' command")
		}
		key := string(args[0])
		decrement, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.DECRBY(key, decrement)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "INCRBYFLOAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'INCRBYFLOAT' command")
		}
		key := string(args[0])
		increment, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		value, err := h.Db.INCRBYFLOAT(key, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))

	case "APPEND":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'APPEND' command")
		}
		key, value := string(args[0]), string(args[1])
		length, err := h.Db.APPEND(key, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(length))

	case "STRLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'STRLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.StrLen(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(length))

	case "GETRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'GETRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.Atoi(string(args[1]))
		end, err2 := strconv.Atoi(string(args[2]))
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.GetRange(key, start, end)
		if err != nil {
			return proto.NewBulkString([]byte(""))
		}
		return proto.NewBulkString([]byte(value))

	case "SETRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETRANGE' command")
		}
		key, value := string(args[0]), string(args[2])
		offset, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		length, err := h.Db.SetRange(key, offset, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(length))

	// 通用键管理命令
	case "DEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DEL' command")
		}
		count := 0
		for _, arg := range args {
			key := string(arg)
			if err := h.Db.Del(key); err == nil {
				count++
			}
		}
		return proto.NewInteger(int64(count))

	case "EXISTS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'EXISTS' command")
		}
		count := 0
		for _, arg := range args {
			key := string(arg)
			exists, err := h.Db.Exists(key)
			if err == nil && exists {
				count++
			}
		}
		return proto.NewInteger(int64(count))

	case "TYPE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TYPE' command")
		}
		key := string(args[0])
		keyType, err := h.Db.Type(key)
		if err != nil {
			return proto.NewSimpleString("none")
		}
		return proto.NewSimpleString(keyType)

	case "EXPIRE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'EXPIRE' command")
		}
		key := string(args[0])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		success, err := h.Db.Expire(key, seconds)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "EXPIREAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'EXPIREAT' command")
		}
		key := string(args[0])
		timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		success, err := h.Db.ExpireAt(key, timestamp)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "PEXPIRE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PEXPIRE' command")
		}
		key := string(args[0])
		milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		success, err := h.Db.PExpire(key, milliseconds)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "PEXPIREAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PEXPIREAT' command")
		}
		key := string(args[0])
		timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		success, err := h.Db.PExpireAt(key, timestamp)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "TTL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TTL' command")
		}
		key := string(args[0])
		ttl, err := h.Db.TTL(key)
		if err != nil {
			return proto.NewInteger(-2)
		}
		return proto.NewInteger(ttl)

	case "PTTL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PTTL' command")
		}
		key := string(args[0])
		pttl, err := h.Db.PTTL(key)
		if err != nil {
			return proto.NewInteger(-2)
		}
		return proto.NewInteger(pttl)

	case "PERSIST":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PERSIST' command")
		}
		key := string(args[0])
		success, err := h.Db.Persist(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "RENAME":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RENAME' command")
		}
		key, newKey := string(args[0]), string(args[1])
		if err := h.Db.Rename(key, newKey); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "RENAMENX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RENAMENX' command")
		}
		key, newKey := string(args[0]), string(args[1])
		success, err := h.Db.RenameNX(key, newKey)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "KEYS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'KEYS' command")
		}
		pattern := string(args[0])
		keys, err := h.Db.Keys(pattern)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(keys))
		for i, k := range keys {
			results[i] = []byte(k)
		}
		return &proto.Array{Args: results}

	case "SCAN":
		cursor := uint64(0)
		pattern := "*"
		count := 10
		if len(args) >= 1 {
			var err error
			cursor, err = strconv.ParseUint(string(args[0]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid cursor")
			}
		}
		if len(args) >= 3 && strings.ToUpper(string(args[1])) == "MATCH" {
			pattern = string(args[2])
		}
		if len(args) >= 5 && strings.ToUpper(string(args[3])) == "COUNT" {
			var err error
			count, err = strconv.Atoi(string(args[4]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
		}
		result, err := h.Db.Scan(cursor, pattern, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// SCAN返回格式: *2\r\n$1\r\n0\r\n*2\r\n$3\r\nkey1\r\n$3\r\nkey2\r\n
		// 由于当前Array不支持嵌套，我们返回简化的格式
		// 实际使用时需要客户端适配，或者扩展proto包支持嵌套数组
		keys := make([][]byte, len(result.Keys))
		for i, k := range result.Keys {
			keys[i] = []byte(k)
		}
		// 返回游标和键数组（简化版本，不嵌套）
		response := make([][]byte, 1+len(keys))
		response[0] = []byte(strconv.FormatUint(result.Cursor, 10))
		if len(keys) > 0 {
			copy(response[1:], keys)
		}
		return &proto.Array{Args: response}

	case "RANDOMKEY":
		key, err := h.Db.RandomKey()
		if err != nil || key == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(key))

	// List命令
	case "LPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LPUSH' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.LPush(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "RPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPUSH' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.RPush(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "LPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'LPOP' command")
		}
		key := string(args[0])
		value, err := h.Db.LPop(key)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "RPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'RPOP' command")
		}
		key := string(args[0])
		value, err := h.Db.RPop(key)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'LLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.LLen(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(length))

	case "LINDEX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LINDEX' command")
		}
		key := string(args[0])
		index, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.LIndex(key, index)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		values, err := h.Db.LRange(key, start, stop)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(values))
		for i, v := range values {
			results[i] = []byte(v)
		}
		return &proto.Array{Args: results}

	case "LSET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LSET' command")
		}
		key, value := string(args[0]), string(args[2])
		index, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		if err := h.Db.LSet(key, index, value); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "LTRIM":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LTRIM' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		if err := h.Db.LTrim(key, start, stop); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "LINSERT":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'LINSERT' command")
		}
		key, pivot, value := string(args[0]), string(args[2]), string(args[3])
		where := strings.ToUpper(string(args[1]))
		if where != "BEFORE" && where != "AFTER" {
			return proto.NewError("ERR syntax error")
		}
		count, err := h.Db.LInsert(key, where, pivot, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "LREM":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LREM' command")
		}
		key, value := string(args[0]), string(args[2])
		count, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		removed, err := h.Db.LRem(key, count, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(removed))

	case "RPOPLPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPOPLPUSH' command")
		}
		source, destination := string(args[0]), string(args[1])
		value, err := h.Db.RPopLPush(source, destination)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LPUSHX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LPUSHX' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.LPUSHX(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "RPUSHX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPUSHX' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.RPUSHX(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "BLPOP":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BLPOP' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, value, err := h.Db.BLPOP(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}

	case "BRPOP":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BRPOP' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, value, err := h.Db.BRPOP(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}

	case "BRPOPLPUSH":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'BRPOPLPUSH' command")
		}
		source, destination := string(args[0]), string(args[1])
		timeout, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		value, err := h.Db.BRPOPLPUSH(source, destination, timeout)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	// Hash命令
	case "HSET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HSET' command")
		}
		key := string(args[0])
		count := 0
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			field, value := string(args[i]), args[i+1]
			if err := h.Db.HSet(key, field, string(value)); err == nil {
				count++
			}
		}
		return proto.NewInteger(int64(count))

	case "HGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HGET' command")
		}
		key, field := string(args[0]), string(args[1])
		value, err := h.Db.HGet(key, field)
		if err != nil || value == nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString(value)

	case "HDEL":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HDEL' command")
		}
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		count, err := h.Db.HDel(key, fields...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "HLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.HLen(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(length))

	case "HGETALL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HGETALL' command")
		}
		key := string(args[0])
		data, err := h.Db.HGetAll(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, 0, len(data)*2)
		for field, value := range data {
			results = append(results, []byte(field), value)
		}
		return &proto.Array{Args: results}

	case "HEXISTS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HEXISTS' command")
		}
		key, field := string(args[0]), string(args[1])
		exists, err := h.Db.HExists(key, field)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(boolToInt(exists)))

	case "HKEYS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HKEYS' command")
		}
		key := string(args[0])
		keys, err := h.Db.HKeys(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(keys))
		for i, k := range keys {
			results[i] = []byte(k)
		}
		return &proto.Array{Args: results}

	case "HVALS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HVALS' command")
		}
		key := string(args[0])
		values, err := h.Db.HVals(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(values))
		copy(results, values)
		return &proto.Array{Args: results}

	case "HMSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return proto.NewError("ERR wrong number of arguments for 'HMSET' command")
		}
		key := string(args[0])
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			field, value := string(args[i]), string(args[i+1])
			if err := h.Db.HSet(key, field, value); err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
		}
		return proto.OK

	case "HMGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HMGET' command")
		}
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		values, err := h.Db.HMGet(key, fields...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(values))
		for i, v := range values {
			if v == nil {
				results[i] = nil
			} else {
				results[i] = v
			}
		}
		return &proto.Array{Args: results}

	case "HSETNX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HSETNX' command")
		}
		key, field, value := string(args[0]), string(args[1]), string(args[2])
		success, err := h.Db.HSetNX(key, field, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "HINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HINCRBY' command")
		}
		key, field := string(args[0]), string(args[1])
		increment, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.HIncrBy(key, field, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "HINCRBYFLOAT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HINCRBYFLOAT' command")
		}
		key, field := string(args[0]), string(args[1])
		increment, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		value, err := h.Db.HIncrByFloat(key, field, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))

	case "HSTRLEN":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HSTRLEN' command")
		}
		key, field := string(args[0]), string(args[1])
		length, err := h.Db.HStrLen(key, field)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(length))

	// Set命令
	case "SADD":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SADD' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, err := h.Db.SAdd(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "SREM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SREM' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, err := h.Db.SRem(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "SCARD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SCARD' command")
		}
		key := string(args[0])
		count, err := h.Db.SCard(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(count))

	case "SISMEMBER":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SISMEMBER' command")
		}
		key, member := string(args[0]), string(args[1])
		exists, err := h.Db.SIsMember(key, member)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(boolToInt(exists)))

	case "SMEMBERS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SMEMBERS' command")
		}
		key := string(args[0])
		members, err := h.Db.SMembers(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SPOP' command")
		}
		key := string(args[0])
		member, err := h.Db.SPop(key)
		if err != nil || member == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(member))

	case "SRANDMEMBER":
		key := string(args[0])
		if len(args) == 1 {
			// SRANDMEMBER key - return single member
			member, err := h.Db.SRandMember(key)
			if err != nil || member == "" {
				return proto.NewBulkString(nil)
			}
			return proto.NewBulkString([]byte(member))
		}
		// SRANDMEMBER key count - return array of members
		count, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		members, err := h.Db.SRandMemberN(key, count)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SMOVE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SMOVE' command")
		}
		source, destination, member := string(args[0]), string(args[1]), string(args[2])
		success, err := h.Db.SMove(source, destination, member)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "SINTER":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SINTER' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		members, err := h.Db.SInter(keys...)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SUNION":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SUNION' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		members, err := h.Db.SUnion(keys...)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SDIFF":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SDIFF' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		members, err := h.Db.SDiff(keys...)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SINTERSTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SINTERSTORE' command")
		}
		destination := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		count, err := h.Db.SInterStore(destination, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "SUNIONSTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SUNIONSTORE' command")
		}
		destination := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		count, err := h.Db.SUnionStore(destination, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "SDIFFSTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SDIFFSTORE' command")
		}
		destination := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		count, err := h.Db.SDiffStore(destination, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	// SortedSet命令 - 由于代码太长，这里只实现主要命令
	case "ZADD":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZADD' command")
		}
		key := string(args[0])
		members := make([]store.ZSetMember, 0)
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			score, err := strconv.ParseFloat(string(args[i]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			member := string(args[i+1])
			members = append(members, store.ZSetMember{Member: member, Score: score})
		}
		if err := h.Db.ZAdd(key, members); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(len(members)))

	case "ZREM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZREM' command")
		}
		key := string(args[0])
		count := 0
		for i := 1; i < len(args); i++ {
			member := string(args[i])
			if err := h.Db.ZRem(key, member); err == nil {
				count++
			}
		}
		return proto.NewInteger(int64(count))

	case "ZCARD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZCARD' command")
		}
		key := string(args[0])
		count, err := h.Db.ZCard(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(count)

	case "ZSCORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZSCORE' command")
		}
		key, member := string(args[0]), string(args[1])
		score, exists, err := h.Db.ZScore(key, member)
		if err != nil || !exists {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", score)))

	case "ZRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		// 检查是否有 WITHSCORES 选项
		withScores := false
		if len(args) >= 4 && strings.ToUpper(string(args[3])) == "WITHSCORES" {
			withScores = true
		}
		members, err := h.Db.ZRange(key, start, stop)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, 0)
		if withScores {
			// 有 WITHSCORES：返回 member 和 score 的交替数组
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			// 没有 WITHSCORES：只返回 member 列表
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZREVRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		// 检查是否有 WITHSCORES 选项
		withScores := false
		if len(args) >= 4 && strings.ToUpper(string(args[3])) == "WITHSCORES" {
			withScores = true
		}
		members, err := h.Db.ZRevRange(key, start, stop)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, 0)
		if withScores {
			// 有 WITHSCORES：返回 member 和 score 的交替数组
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			// 没有 WITHSCORES：只返回 member 列表
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZRANK":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANK' command")
		}
		key, member := string(args[0]), string(args[1])
		rank, err := h.Db.ZRank(key, member)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewInteger(rank)

	case "ZREVRANK":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANK' command")
		}
		key, member := string(args[0]), string(args[1])
		rank, err := h.Db.ZRevRank(key, member)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewInteger(rank)

	case "ZCOUNT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZCOUNT' command")
		}
		key := string(args[0])
		min, err1 := strconv.ParseFloat(string(args[1]), 64)
		max, err2 := strconv.ParseFloat(string(args[2]), 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		count, err := h.Db.ZCount(key, min, max)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(count)

	case "ZINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZINCRBY' command")
		}
		key, member := string(args[0]), string(args[2])
		increment, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		score, err := h.Db.ZIncrBy(key, member, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", score)))

	// Cluster命令
	case "CLUSTER":
		if h.Cluster == nil {
			return proto.NewError("ERR This instance has cluster support disabled")
		}
		if len(args) == 0 {
			return proto.NewError("ERR wrong number of arguments for 'CLUSTER' command")
		}
		clusterCmd := cluster.NewClusterCommands(h.Cluster)
		subcommandArgs := make([]string, len(args))
		for i, arg := range args {
			subcommandArgs[i] = string(arg)
		}
		result, err := clusterCmd.HandleCommand(subcommandArgs)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 根据返回类型转换
		switch v := result.(type) {
		case string:
			return proto.NewSimpleString(v)
		case int64:
			return proto.NewInteger(v)
		case []string:
			// 对于CLUSTER NODES，返回多行字符串
			return proto.NewSimpleString(strings.Join(v, "\n"))
		case []interface{}:
			// 对于CLUSTER SLOTS，返回数组
			// 简化处理：转换为字符串数组
			strs := make([][]byte, len(v))
			for i, item := range v {
				strs[i] = []byte(fmt.Sprintf("%v", item))
			}
			return &proto.Array{Args: strs}
		default:
			return proto.NewSimpleString(fmt.Sprintf("%v", v))
		}

	// CONFIG 命令（用于 redis-benchmark 兼容性）
	case "CONFIG":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'CONFIG' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "GET":
			// CONFIG GET 返回键值对数组
			// 格式: [key1, value1, key2, value2, ...]
			// 返回一些基本配置以兼容 redis-benchmark
			if len(args) == 1 || (len(args) >= 2 && string(args[1]) == "*") {
				// CONFIG GET 或 CONFIG GET * - 返回所有配置
				configs := []string{
					"save", "",
					"appendonly", "no",
					"maxmemory", "0",
					"maxmemory-policy", "noeviction",
				}
				results := make([][]byte, len(configs))
				for i, cfg := range configs {
					results[i] = []byte(cfg)
				}
				return &proto.Array{Args: results}
			} else if len(args) >= 2 {
				// CONFIG GET key - 返回特定配置
				key := string(args[1])
				var value string
				switch strings.ToLower(key) {
				case "save":
					value = ""
				case "appendonly":
					value = "no"
				case "maxmemory":
					value = "0"
				case "maxmemory-policy":
					value = "noeviction"
				default:
					value = ""
				}
				return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}
			} else {
				return proto.NewError("ERR wrong number of arguments for 'CONFIG GET' command")
			}
		case "SET":
			// CONFIG SET 返回 OK（简化实现，不实际设置）
			return proto.OK
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
		}

	// 复制命令
	case "REPLICAOF":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'REPLICAOF' command")
		}
		host := string(args[0])
		port := string(args[1])
		if host == "NO" && port == "ONE" {
			// 停止复制
			replication.StopSlaveReplication(h.Replication)
			return proto.OK
		}
		// 启动复制
		masterAddr := fmt.Sprintf("%s:%s", host, port)
		if err := replication.StartSlaveReplication(h.Replication, h.Db, masterAddr); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "PSYNC":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PSYNC' command")
		}
		replId := string(args[0])
		offset, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR invalid offset")
		}
		// 处理PSYNC（主节点端）
		result, err := replication.HandlePSync(h.Replication, replId, offset)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 获取当前连接（需要从连接中获取）
		// 简化实现，实际应该从连接上下文获取
		if result.FullResync {
			// 发送FULLRESYNC响应
			response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", result.ReplId, result.Offset)
			return proto.NewSimpleString(strings.TrimSpace(response))
		} else {
			// 发送CONTINUE响应
			response := fmt.Sprintf("+CONTINUE %s\r\n", result.ReplId)
			return proto.NewSimpleString(strings.TrimSpace(response))
		}

	case "REPLCONF":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'REPLCONF' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "LISTENING-PORT":
			// REPLCONF listening-port <port>
			// 记录从节点的监听端口
			return proto.OK
		case "CAPA":
			// REPLCONF capa <capability>
			// 记录从节点的能力
			return proto.OK
		case "ACK":
			// REPLCONF ACK <offset>
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'REPLCONF ACK' command")
			}
			offset, err := strconv.ParseInt(string(args[1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid offset")
			}
			// 更新从节点的ACK偏移量
			// 简化实现
			_ = offset
			return proto.OK
		case "GETACK":
			// REPLCONF GETACK *
			// 返回当前复制偏移量
			offset := h.Replication.GetMasterReplOffset()
			return &proto.Array{Args: [][]byte{
				[]byte("REPLCONF"),
				[]byte("ACK"),
				[]byte(strconv.FormatInt(offset, 10)),
			}}
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
		}

	// INFO命令
	case "INFO":
		section := ""
		if len(args) >= 1 {
			section = strings.ToUpper(string(args[0]))
		}
		info := h.buildInfoResponse(section)
		return proto.NewBulkString([]byte(info))

	// 备份命令
	case "SAVE":
		if h.Backup == nil {
			return proto.NewError("ERR backup not enabled")
		}
		if err := h.Backup.Save(); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "BGSAVE":
		if h.Backup == nil {
			return proto.NewError("ERR backup not enabled")
		}
		if err := h.Backup.BGSave(); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewSimpleString("Background saving started")

	case "LASTSAVE":
		if h.Backup == nil {
			return proto.NewError("ERR backup not enabled")
		}
		lastSave := h.Backup.LastSave()
		return proto.NewInteger(lastSave)

	case "DBSIZE":
		keys, err := h.Db.Keys("*")
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(len(keys)))

	case "FLUSHDB":
		err := h.Db.FlushDB()
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "FLUSHALL":
		err := h.Db.FlushDB()
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	// Pub/Sub命令
	case "PUBLISH":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PUBLISH' command")
		}
		channel := string(args[0])
		message := args[1]
		count := h.PubSub.Publish(channel, message)
		return proto.NewInteger(int64(count))

	case "SUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SUBSCRIBE' command")
		}
		// 简化实现：返回订阅确认
		channels := make([]string, len(args))
		for i, arg := range args {
			channels[i] = string(arg)
		}
		// 实际应该创建订阅者并持续发送消息
		// 这里简化处理
		return &proto.Array{Args: [][]byte{
			[]byte("subscribe"),
			[]byte(channels[0]),
			[]byte("1"),
		}}

	case "PSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PSUBSCRIBE' command")
		}
		// 简化实现
		patterns := make([]string, len(args))
		for i, arg := range args {
			patterns[i] = string(arg)
		}
		return &proto.Array{Args: [][]byte{
			[]byte("psubscribe"),
			[]byte(patterns[0]),
			[]byte("1"),
		}}

	case "UNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		// 简化实现
		return &proto.Array{Args: [][]byte{
			[]byte("unsubscribe"),
		}}

	case "PUNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		// 简化实现
		return &proto.Array{Args: [][]byte{
			[]byte("punsubscribe"),
		}}

	case "PUBSUB":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PUBSUB' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "CHANNELS":
			pattern := "*"
			if len(args) >= 2 {
				pattern = string(args[1])
			}
			channels := h.PubSub.GetChannels(pattern)
			results := make([][]byte, len(channels))
			for i, ch := range channels {
				results[i] = []byte(ch)
			}
			return &proto.Array{Args: results}
		case "NUMSUB":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'PUBSUB NUMSUB' command")
			}
			results := make([][]byte, 0)
			for i := 1; i < len(args); i++ {
				channel := string(args[i])
				count := h.PubSub.GetSubscriberCount(channel)
				results = append(results, []byte(channel), []byte(strconv.FormatInt(int64(count), 10)))
			}
			return &proto.Array{Args: results}
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
		}

	default:
		return proto.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
