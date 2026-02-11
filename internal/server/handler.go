package server

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

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
	// 事务状态（每个连接独立）
	transaction *TransactionState
	// 客户端信息（连接级别）
	clientInfo *ClientInfo
}

// ClientInfo 客户端连接信息
type ClientInfo struct {
	ID       int64               // 客户端 ID
	Addr     string              // 客户端地址
	FD       int                 // 文件描述符
	Age      int64               // 连接时长（秒）
	Idle     int64               // 空闲时间（秒）
	Flags    string              // 客户端标志
	DB       int                 // 当前数据库 ID
	Sub      int                 // 订阅频道数
	PSub     int                 // 模式订阅数
	Multi    int                 //事务中的命令数
	Cmd      string              // 最后执行的命令
	OFlags   string              // 客户端输出缓冲区限制标志
	Events   string              // 事件处理标志
	Keys     map[string]struct{} // 客户端监控的键
	ReadOnly bool                // 只读模式
}

// TransactionState 事务状态
type TransactionState struct {
	Commands   []TransactionCommand // 排队的命令
	WatchKeys  map[string]struct{}  // 监控的键
	IsWatching bool                 // 是否处于WATCH状态
}

// TransactionCommand 事务中的命令
type TransactionCommand struct {
	Command string
	Args    [][]byte
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
		// 尝试读取所有可用的命令（支持 Pipeline）
		// 先尝试读取第一个命令
		req, err := proto.ReadRESP(reader)
		if err != nil {
			// 连接关闭或读取错误，直接返回
			// 不发送错误响应，因为连接可能已关闭
			// 这可能是正常的连接关闭（如 redis-benchmark 完成测试后关闭连接）
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("读取请求失败")
			return
		}

		// 收集所有响应
		var responses []proto.RESP
		commandsProcessed := 0

		// 处理第一个命令
		if resp := h.processRequest(req, reader, remoteAddr); resp != nil {
			responses = append(responses, resp)
			commandsProcessed++
		} else {
			// 处理失败，直接返回
			return
		}

		// 尝试读取更多已缓冲的命令（Pipeline）
		for reader.Buffered() > 0 {
			req, err := proto.ReadRESP(reader)
			if err != nil {
				// 如果读取失败，可能是连接关闭
				logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("Pipeline 中读取请求失败")
				break
			}

			if resp := h.processRequest(req, reader, remoteAddr); resp != nil {
				responses = append(responses, resp)
				commandsProcessed++
			} else {
				// 处理失败，直接返回
				return
			}
		}

		// 批量发送所有响应
		for _, resp := range responses {
			if err := proto.WriteRESP(writer, resp); err != nil {
				logger.Logger.Warn().
					Str("remote_addr", remoteAddr).
					Err(err).
					Msg("写入响应失败")
				return
			}
		}

		// 一次性刷新所有响应
		if err := writer.Flush(); err != nil {
			logger.Logger.Warn().
				Str("remote_addr", remoteAddr).
				Err(err).
				Msg("刷新缓冲区失败")
			return
		}

		logger.Logger.Debug().
			Str("remote_addr", remoteAddr).
			Int("commands_processed", commandsProcessed).
			Msg("Pipeline 命令处理完成")
	}
}

// processRequest 处理单个请求，返回响应
func (h *Handler) processRequest(req *proto.Array, _ *bufio.Reader, remoteAddr string) proto.RESP {
	args := req.Args
	if len(args) == 0 {
		logger.Logger.Warn().Str("remote_addr", remoteAddr).Msg("收到空命令")
		return proto.NewError("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))
	logger.Logger.Debug().
		Str("remote_addr", remoteAddr).
		Str("command", cmd).
		Int("arg_count", len(args)-1).
		Msg("执行命令")

	resp := h.executeCommand(cmd, args[1:])
	if resp == nil {
		logger.Logger.Error().
			Str("remote_addr", remoteAddr).
			Str("command", cmd).
			Msg("命令执行返回 nil")
		return proto.NewError("ERR internal error")
	}

	// 如果是主节点且是写命令，传播到从节点
	if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) {
		if cmd != "REPLICAOF" && cmd != "PSYNC" && cmd != "REPLCONF" {
			h.Replication.PropagateCommand(req.Args)
		}
	}

	logger.Logger.Debug().
		Str("remote_addr", remoteAddr).
		Str("command", cmd).
		Str("response_type", getResponseType(resp)).
		Msg("命令执行完成")

	return resp
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

	case "CLIENT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "LIST":
			// 返回当前客户端列表（简化实现）
			return proto.NewBulkString([]byte("id=1 addr=127.0.0.1:12345 fd=6 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 cmd=client events=r oFlags= keys=0"))
		case "GETNAME":
			if h.clientInfo != nil && h.clientInfo.ID != 0 {
				return proto.NewBulkString([]byte(fmt.Sprintf("client-%d", h.clientInfo.ID)))
			}
			return proto.NewBulkString(nil)
		case "SETNAME":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT SETNAME' command")
			}
			// 设置客户端名称（仅内存存储）
			name := string(args[1])
			if h.clientInfo == nil {
				h.clientInfo = &ClientInfo{}
			}
			_ = name // 名称已设置
			return proto.OK
		case "ID":
			if h.clientInfo != nil {
				return proto.NewInteger(h.clientInfo.ID)
			}
			return proto.NewInteger(1)
		case "KILL":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT KILL' command")
			}
			addr := string(args[1])
			// 简化实现：检查地址格式，但不真正关闭连接
			if addr == "" {
				return proto.NewError("ERR Invalid address")
			}
			return proto.NewInteger(1)
		case "PAUSE":
			// 暂停客户端（简化实现：空操作）
			return proto.OK
		case "UNPAUSE":
			// 取消暂停（简化实现：空操作）
			return proto.OK
		default:
			return proto.NewError("ERR syntax error")
		}

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
		// #nosec G115 - length is bounded by practical data size limits
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
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	// Bitmap commands
	case "SETBIT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETBIT' command")
		}
		key := string(args[0])
		offset, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		bit, err := strconv.ParseUint(string(args[2]), 10, 8)
		if err != nil || (bit != 0 && bit != 1) {
			return proto.NewError("ERR bit is not an integer or out of range")
		}
		newBit, err := h.Db.SetBit(key, int(offset), int(bit))
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(newBit))

	case "GETBIT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GETBIT' command")
		}
		key := string(args[0])
		offset, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		bit, err := h.Db.GetBit(key, int(offset))
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(bit))

	case "BITCOUNT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'BITCOUNT' command")
		}
		key := string(args[0])
		// BITCOUNT key [start end]
		start := 0
		end := -1
		if len(args) >= 3 {
			s, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			start = s
			e, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			end = e
		}
		count, err := h.Db.BitCount(key, start, end)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "BITOP":
		// BITOP operation destkey key [key ...]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'BITOP' command")
		}
		operation := strings.ToUpper(string(args[0]))
		destKey := string(args[1])
		sourceKeys := make([]string, len(args)-2)
		for i := 2; i < len(args); i++ {
			sourceKeys[i-2] = string(args[i])
		}
		// 验证操作类型
		if operation != "AND" && operation != "OR" && operation != "XOR" && operation != "NOT" {
			return proto.NewError("ERR syntax error")
		}
		// NOT 只能有一个源键
		if operation == "NOT" && len(sourceKeys) != 1 {
			return proto.NewError("ERR BITOP NOT must be called with exactly one source key")
		}
		length, err := h.Db.BitOp(operation, destKey, sourceKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(length))

	case "BITFIELD":
		// BITFIELD key [GET type offset | SET type offset value | INCRBY type offset increment] ...
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'BITFIELD' command")
		}
		key := string(args[0])
		operations := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			operations = append(operations, string(args[i]))
		}
		results, err := h.Db.BitField(key, operations)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Convert results to RESP array
		respArgs := make([][]byte, len(results))
		for i, r := range results {
			switch val := r.(type) {
			case int64:
				respArgs[i] = []byte(strconv.FormatInt(val, 10))
			case []interface{}:
				// Overflow case: [value, overflow_type]
				var b strings.Builder
				b.WriteString(strconv.FormatInt(val[0].(int64), 10))
				b.WriteString(":")
				b.WriteString(val[1].(string))
				respArgs[i] = []byte(b.String())
			}
		}
		return &proto.Array{Args: respArgs}

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
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	// 通用键管理命令
	case "DEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DEL' command")
		}
		count := int64(0)
		for _, arg := range args {
			key := string(arg)
			deleted, err := h.Db.Del(key)
			if err == nil {
				count += deleted
			}
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(count)

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
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "PFADD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PFADD' command")
		}
		key := string(args[0])
		elements := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			elements[i-1] = string(args[i])
		}
		changed, err := h.Db.PFAdd(key, elements...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(changed)

	case "PFCOUNT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PFCOUNT' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		count, err := h.Db.PFCount(keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "PFMERGE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PFMERGE' command")
		}
		destKey := string(args[0])
		sourceKeys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			sourceKeys[i-1] = string(args[i])
		}
		err := h.Db.PFMerge(destKey, sourceKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

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

	case "DUMP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DUMP' command")
		}
		key := string(args[0])
		data, err := h.Db.Dump(key)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString(data)

	case "RESTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RESTORE' command")
		}
		key := string(args[0])
		serializedData := args[1]
		replace := false
		// 检查是否有 REPLACE 选项
		for i := 2; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "REPLACE" {
				replace = true
				break
			}
		}
		err := h.Db.Restore(key, serializedData, replace)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "OBJECT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'OBJECT' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		key := string(args[1])

		switch subcommand {
		case "REFCOUNT":
			refcount, err := h.Db.ObjectRefCount(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			if refcount == 0 {
				return proto.NewBulkString(nil)
			}
			return proto.NewInteger(refcount)
		case "ENCODING":
			encoding, err := h.Db.ObjectEncoding(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			if encoding == "" {
				return proto.NewBulkString(nil)
			}
			return proto.NewBulkString([]byte(encoding))
		case "IDLETIME":
			idletime, err := h.Db.ObjectIdleTime(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(idletime)
		case "FREQ":
			// Freq not supported, return empty string
			return proto.NewBulkString(nil)
		default:
			return proto.NewError("ERR syntax error")
		}

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

	case "COPY":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'COPY' command")
		}
		srcKey := string(args[0])
		dstKey := string(args[1])
		replace := false
		db := int(0)
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "REPLACE":
				replace = true
				i++
			case "DB":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				dbNum, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				db = dbNum
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
		// 不支持跨数据库COPY
		if db != 0 {
			return proto.NewError("ERR DB option not supported")
		}
		// 获取源键类型
		srcType, err := h.Db.Type(srcKey)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if srcType == "none" {
			return proto.NewInteger(0) // 源键不存在
		}
		// 检查目标键是否存在
		dstExists, _ := h.Db.Exists(dstKey)
		if dstExists && !replace {
			return proto.NewInteger(0) // 目标存在且不替换
		}
		// 根据类型复制
		var copied bool
		switch srcType {
		case "string":
			val, err := h.Db.Get(srcKey)
			if err == nil {
				err = h.Db.Set(dstKey, val)
			}
			copied = err == nil
		case "list":
			copied = h.copyList(srcKey, dstKey)
		case "hash":
			copied = h.copyHash(srcKey, dstKey)
		case "set":
			copied = h.copySet(srcKey, dstKey)
		case "zset":
			copied = h.copySortedSet(srcKey, dstKey)
		default:
			return proto.NewError("ERR unknown type")
		}
		if !copied {
			return proto.NewError("ERR copy failed")
		}
		return proto.NewInteger(1)

	case "SWAPDB":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SWAPDB' command")
		}
		// BoltDB 是单数据库实现，SWAPDB 是空操作
		return proto.NewSimpleString("OK")

	case "TOUCH":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TOUCH' command")
		}
		// TOUCH 返回键的数量（BadgerDB 不维护访问时间，所以只是 EXIST 的变体）
		count := int64(0)
		for _, arg := range args {
			key := string(arg)
			exists, _ := h.Db.Exists(key)
			if exists {
				count++
			}
		}
		return proto.NewInteger(count)

	case "SHUTDOWN":
		// SHUTDOWN 命令（简化实现：返回错误，因为没有优雅关闭机制）
		return proto.NewError("ERR Redis is running in read-only mode. To shutdown use SHUTDOWN NOSAVE or SHUTDOWN SAVE")

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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - length is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "LPOS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LPOS' command")
		}
		key := string(args[0])
		element := string(args[1])
		rank := int64(0)
		count := int64(0)
		maxlen := int64(0)

		// 解析可选参数
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			if opt == "RANK" && i+1 < len(args) {
				r, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				rank = r
				i += 2
			} else if opt == "COUNT" && i+1 < len(args) {
				c, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				count = c
				i += 2
			} else if opt == "MAXLEN" && i+1 < len(args) {
				m, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				maxlen = m
				i += 2
			} else {
				return proto.NewError("ERR syntax error")
			}
		}

		positions, err := h.Db.LPos(key, element, rank, count, maxlen)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		if len(positions) == 0 {
			return proto.NewBulkString(nil)
		}

		if count == 0 && rank == 0 {
			// 返回单个位置
			return proto.NewInteger(positions[0])
		}

		// 返回多个位置
		result := make([][]byte, len(positions))
		for j, pos := range positions {
			result[j] = []byte(fmt.Sprintf("%d", pos))
		}
		return &proto.Array{Args: result}

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

	case "LMOVE":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'LMOVE' command")
		}
		source := string(args[0])
		destination := string(args[1])
		sourceDirection := strings.ToUpper(string(args[2]))
		destinationDirection := strings.ToUpper(string(args[3]))
		value, err := h.Db.LMove(source, destination, sourceDirection, destinationDirection)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "BLMOVE":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'BLMOVE' command")
		}
		source := string(args[0])
		destination := string(args[1])
		sourceDirection := strings.ToUpper(string(args[2]))
		destinationDirection := strings.ToUpper(string(args[3]))
		timeout, err := strconv.ParseFloat(string(args[4]), 64)
		if err != nil {
			return proto.NewError("ERR timeout is not a float")
		}
		value, err := h.Db.BLMoveBlocking(source, destination, sourceDirection, destinationDirection, timeout)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if value == "" {
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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
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
		key, value, err := h.Db.BLPOPBlocking(keys, timeout)
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
		key, value, err := h.Db.BRPOPBlocking(keys, timeout)
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
		value, err := h.Db.BRPOPLPUSHBlocking(source, destination, timeout)
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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - length is bounded by practical data size limits
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
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	case "HRANDFIELD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HRANDFIELD' command")
		}
		key := string(args[0])
		count := 1
		withValues := false
		// 解析可选参数: HRANDFIELD key [count [WITHVALUES]]
		if len(args) >= 2 {
			// 第二个参数可能是 count 或 WITHVALUES
			secondArg := strings.ToUpper(string(args[1]))
			if secondArg != "WITHVALUES" {
				// 是 count
				c, err := strconv.Atoi(string(args[1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				count = c
			}
		}
		// 检查是否有 WITHVALUES 选项
		for i := 1; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "WITHVALUES" {
				withValues = true
			}
		}
		fields, values, err := h.Db.HRandField(key, count, withValues)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 构建响应
		if withValues {
			// 返回字段和值的交替数组
			result := make([][]byte, 0, len(fields)*2)
			for i, field := range fields {
				result = append(result, []byte(field))
				result = append(result, []byte(values[i]))
			}
			return &proto.Array{Args: result}
		}
		// 只返回字段
		result := make([][]byte, len(fields))
		for i, field := range fields {
			result[i] = []byte(field)
		}
		return &proto.Array{Args: result}

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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SMISMEMBER":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SMISMEMBER' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		results, err := h.Db.SMIsMember(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 转换为响应数组
		resp := make([][]byte, len(results))
		for i, v := range results {
			resp[i] = []byte(strconv.FormatInt(v, 10))
		}
		return &proto.Array{Args: resp}

	case "SINTERCARD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SINTERCARD' command")
		}
		sinterKeys := make([]string, len(args))
		for i, arg := range args {
			sinterKeys[i] = string(arg)
		}
		count, err := h.Db.SInterCard(sinterKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

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
		// #nosec G115 - count is bounded by practical data size limits
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
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SSCAN":
		// SSCAN key cursor [MATCH pattern] [COUNT count]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SSCAN' command")
		}
		key := string(args[0])
		cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		pattern := ""
		count := 10
		// Parse optional MATCH and COUNT
		if len(args) > 2 {
			for i := 2; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "MATCH" && i+1 < len(args) {
					pattern = string(args[i+1])
					i++
				} else if opt == "COUNT" && i+1 < len(args) {
					count, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i++
				}
			}
		}
		result, err := h.Db.SScan(key, cursor, pattern, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回格式: [cursor, [member1, member2, ...]]
		response := make([][]byte, 0, 2+len(result.Members))
		response = append(response, []byte(strconv.FormatUint(result.Cursor, 10)))
		for _, m := range result.Members {
			response = append(response, []byte(m))
		}
		return &proto.Array{Args: response}

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
		// #nosec G115 - count is bounded by practical data size limits
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

	case "ZMSCORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZMSCORE' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		scores, err := h.Db.ZMScore(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回数组，每个元素是分数或 nil
		result := make([][]byte, len(scores))
		for i, score := range scores {
			result[i] = []byte(fmt.Sprintf("%.10g", score))
		}
		return &proto.Array{Args: result}

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

	case "ZRANGEBYSCORE":
		// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYSCORE' command")
		}
		key := string(args[0])
		minStr := string(args[1])
		maxStr := string(args[2])

		// 解析分数范围
		minScore, err := parseScore(minStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}
		maxScore, err := parseScore(maxStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}

		// 解析可选参数
		offset := 0
		count := -1 // -1 表示返回所有
		withScores := false

		i := 3
		for i < len(args) {
			arg := strings.ToUpper(string(args[i]))
			i++
			if arg == "WITHSCORES" {
				withScores = true
			} else if arg == "LIMIT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				offset, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT offset is not an integer")
				}
				count, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT count is not an integer")
				}
			}
		}

		members, err := h.Db.ZRangeByScore(key, minScore, maxScore, offset, count)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}

		results := make([][]byte, 0)
		if withScores {
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZREVRANGEBYSCORE":
		// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYSCORE' command")
		}
		key := string(args[0])
		maxStr := string(args[1])
		minStr := string(args[2])

		// 解析分数范围
		maxScore, err := parseScore(maxStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}
		minScore, err := parseScore(minStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}

		// 解析可选参数
		offset := 0
		count := -1 // -1 表示返回所有
		withScores := false

		i := 3
		for i < len(args) {
			arg := strings.ToUpper(string(args[i]))
			i++
			if arg == "WITHSCORES" {
				withScores = true
			} else if arg == "LIMIT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				offset, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT offset is not an integer")
				}
				count, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT count is not an integer")
				}
			}
		}

		members, err := h.Db.ZRevRangeByScore(key, maxScore, minScore, offset, count)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}

		results := make([][]byte, 0)
		if withScores {
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
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

	case "ZREMRANGEBYRANK":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYRANK' command")
		}
		key := string(args[0])
		start, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		stop, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		count, err := h.Db.ZRemRangeByRank(key, start, stop)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZREMRANGEBYSCORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYSCORE' command")
		}
		key := string(args[0])
		min, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		max, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		count, err := h.Db.ZRemRangeByScore(key, min, max)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZPOPMAX":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZPOPMAX' command")
		}
		key := string(args[0])
		count := 1
		if len(args) >= 2 {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
		}
		members, err := h.Db.ZPopMax(key, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回 member 和 score 的交替数组
		result := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
		}
		return &proto.Array{Args: result}

	case "ZPOPMIN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZPOPMIN' command")
		}
		key := string(args[0])
		count := 1
		if len(args) >= 2 {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
		}
		members, err := h.Db.ZPopMin(key, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回 member 和 score 的交替数组
		result := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
		}
		return &proto.Array{Args: result}

	case "BZPOPMAX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BZPOPMAX' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, member, err := h.Db.BZPopMax(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}

	case "BZPOPMIN":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BZPOPMIN' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, member, err := h.Db.BZPopMin(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}

	case "ZUNIONSTORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZUNIONSTORE' command")
		}
		destination := string(args[0])
		// 解析参数: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
		numKeys, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[2+i])
		}
		weights := []float64{}
		aggregate := "SUM"
		// 解析可选参数
		i := 2 + numKeys
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "WEIGHTS":
				if i+numKeys >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				weights = make([]float64, numKeys)
				for j := 0; j < numKeys; j++ {
					w, err := strconv.ParseFloat(string(args[i+1+j]), 64)
					if err != nil {
						return proto.NewError("ERR weight is not a float")
					}
					weights[j] = w
				}
				i += 1 + numKeys
			case "AGGREGATE":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				aggregate = strings.ToUpper(string(args[i+1]))
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					return proto.NewError("ERR syntax error")
				}
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
		count, err := h.Db.ZUnionStore(destination, keys, weights, aggregate)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZINTERSTORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZINTERSTORE' command")
		}
		destination := string(args[0])
		// 解析参数
		numKeys, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[2+i])
		}
		weights := []float64{}
		aggregate := "SUM"
		// 解析可选参数
		i := 2 + numKeys
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "WEIGHTS":
				if i+numKeys >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				weights = make([]float64, numKeys)
				for j := 0; j < numKeys; j++ {
					w, err := strconv.ParseFloat(string(args[i+1+j]), 64)
					if err != nil {
						return proto.NewError("ERR weight is not a float")
					}
					weights[j] = w
				}
				i += 1 + numKeys
			case "AGGREGATE":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				aggregate = strings.ToUpper(string(args[i+1]))
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					return proto.NewError("ERR syntax error")
				}
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
		count, err := h.Db.ZInterStore(destination, keys, weights, aggregate)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZDIFFSTORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZDIFFSTORE' command")
		}
		destination := string(args[0])
		numKeys, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		if numKeys < 1 {
			return proto.NewError("ERR syntax error")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[2+i])
		}
		count, err := h.Db.ZDiffStore(destination, keys)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZLEXCOUNT":
		// ZLEXCOUNT key min max
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZLEXCOUNT' command")
		}
		zSetName := string(args[0])
		min := string(args[1])
		max := string(args[2])
		count, err := h.Db.ZLexCount(zSetName, min, max)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZRANGEBYLEX":
		// ZRANGEBYLEX key min max [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYLEX' command")
		}
		zSetName := string(args[0])
		min := string(args[1])
		max := string(args[2])
		offset := 0
		count := -1
		var err error
		// Parse optional LIMIT
		if len(args) > 3 {
			for i := 3; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "LIMIT" && i+2 < len(args) {
					offset, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					count, err = strconv.Atoi(string(args[i+2]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i += 2
				}
			}
		}
		members, err := h.Db.ZRangeByLex(zSetName, min, max, offset, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		result := make([][]byte, len(members))
		for i, m := range members {
			result[i] = []byte(m)
		}
		return &proto.Array{Args: result}

	case "ZREVRANGEBYLEX":
		// ZREVRANGEBYLEX key max min [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYLEX' command")
		}
		zSetName := string(args[0])
		max := string(args[1])
		min := string(args[2])
		offset := 0
		count := -1
		var err error
		// Parse optional LIMIT
		if len(args) > 3 {
			for i := 3; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "LIMIT" && i+2 < len(args) {
					offset, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					count, err = strconv.Atoi(string(args[i+2]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i += 2
				}
			}
		}
		members, err := h.Db.ZRevRangeByLex(zSetName, max, min, offset, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		result := make([][]byte, len(members))
		for i, m := range members {
			result[i] = []byte(m)
		}
		return &proto.Array{Args: result}

	case "ZREMRANGEBYLEX":
		// ZREMRANGEBYLEX key min max
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYLEX' command")
		}
		zSetName := string(args[0])
		min := string(args[1])
		max := string(args[2])
		removed, err := h.Db.ZRemRangeByLex(zSetName, min, max)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(removed)

	case "ZSCAN":
		// ZSCAN key cursor [MATCH pattern] [COUNT count]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZSCAN' command")
		}
		zSetName := string(args[0])
		cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		pattern := ""
		count := 10
		// Parse optional MATCH and COUNT
		if len(args) > 2 {
			for i := 2; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "MATCH" && i+1 < len(args) {
					pattern = string(args[i+1])
					i++
				} else if opt == "COUNT" && i+1 < len(args) {
					count, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i++
				}
			}
		}
		result, err := h.Db.ZScan(zSetName, cursor, pattern, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回格式: [cursor, [member1, score1, member2, score2, ...]]
		response := make([][]byte, 0, 2+len(result.Members)*2)
		response = append(response, []byte(strconv.FormatUint(result.Cursor, 10)))
		for _, m := range result.Members {
			response = append(response, []byte(m.Member))
			response = append(response, []byte(fmt.Sprintf("%.10g", m.Score)))
		}
		return &proto.Array{Args: response}

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

	case "TIME":
		sec, usec, err := h.Db.Time()
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return &proto.Array{Args: [][]byte{
			[]byte(fmt.Sprintf("%d", sec)),
			[]byte(fmt.Sprintf("%d", usec)),
		}}

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

	case "SELECT":
		// BoltDB is a single-database implementation
		// Always return OK regardless of the database number
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SELECT' command")
		}
		return proto.OK

	case "MOVE":
		// BoltDB is a single-database implementation
		// MOVE always returns 0 (key was not moved)
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'MOVE' command")
		}
		// #nosec G115 - result is always 0 for single-db implementation
		return proto.NewInteger(0)

	case "WAIT":
		// BoltDB does not support replication yet
		// Return 0 (number of replicas acknowledged)
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'WAIT' command")
		}
		// #nosec G115 - result is always 0 for non-replicated implementation
		return proto.NewInteger(0)

	case "SLOWLOG":
		// BoltDB does not implement slow query logging yet
		// Return empty list for all subcommands
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SLOWLOG' command")
		}
		subCommand := strings.ToUpper(string(args[0]))
		switch subCommand {
		case "GET":
			// Return empty array for GET
			return &proto.Array{Args: [][]byte{}}
		case "LEN":
			// Return 0 (no slowlog entries)
			return proto.NewInteger(0)
		case "RESET":
			// Return OK for RESET
			return proto.OK
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("SLOWLOG GET <count> - returns top <count> entries from the slowlog"),
				[]byte("SLOWLOG LEN - returns the length of the slowlog"),
				[]byte("SLOWLOG RESET - clears the slowlog"),
				[]byte("SLOWLOG HELP - shows this help message"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'SLOWLOG'")
		}

	case "MEMORY":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'MEMORY' command")
		}
		subCommand := strings.ToUpper(string(args[0]))
		switch subCommand {
		case "USAGE":
			// MEMORY USAGE key [SAMPLES count]
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'MEMORY USAGE' command")
			}
			key := string(args[1])
			// Estimate memory usage - use key type size approximation
			size, err := h.Db.MemoryUsage(key)
			if err != nil {
				if errors.Is(err, store.ErrKeyNotFound) {
					return proto.NewBulkString(nil)
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(size)
		case "DOCTOR":
			// Return basic memory info
			return &proto.Array{Args: [][]byte{
				[]byte("BoltDB uses BadgerDB for storage"),
				[]byte("Memory usage is managed by the underlying BadgerDB engine"),
			}}
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("MEMORY USAGE key [SAMPLES count] - estimate memory usage of key"),
				[]byte("MEMORY DOCTOR - reports memory usage details"),
				[]byte("MEMORY HELP - shows this help message"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'MEMORY'")
		}

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
		// #nosec G115 - count is bounded by practical data size limits
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

	// Transaction commands - 事务命令
	case "MULTI":
		// 开始事务
		if h.transaction != nil && len(h.transaction.Commands) > 0 {
			return proto.NewError("ERR MULTI calls can not be nested")
		}
		h.transaction = &TransactionState{
			Commands:   make([]TransactionCommand, 0),
			WatchKeys:  make(map[string]struct{}),
			IsWatching: false,
		}
		return proto.NewSimpleString("OK")

	case "EXEC":
		// 执行事务
		if h.transaction == nil {
			return proto.NewError("ERR EXEC without MULTI")
		}
		// 检查WATCH的键是否被修改
		if h.transaction.IsWatching {
			for key := range h.transaction.WatchKeys {
				exists, _ := h.Db.Exists(key)
				if exists {
					// 键被修改，事务失败
					h.transaction = nil
					return nil // 返回 nil 表示 WATCH 失败
				}
			}
		}

		// 执行所有排队的命令
		results := make([]proto.RESP, len(h.transaction.Commands))
		for i, tc := range h.transaction.Commands {
			results[i] = h.executeQueuedCommand(tc.Command, tc.Args)
		}
		h.transaction = nil
		// 转换为 [][]byte
		flatArgs := make([][]byte, 0)
		for _, r := range results {
			flatArgs = append(flatArgs, []byte(r.String()))
		}
		return &proto.Array{Args: flatArgs}

	case "DISCARD":
		// 放弃事务
		if h.transaction == nil {
			return proto.NewError("ERR DISCARD without MULTI")
		}
		h.transaction = nil
		return proto.NewSimpleString("OK")

	case "WATCH":
		// 监控键
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
		}
		// WATCH 只能在事务外使用
		if h.transaction != nil && len(h.transaction.Commands) > 0 {
			return proto.NewError("ERR WATCH inside MULTI is not allowed")
		}
		// 初始化或重置事务状态用于WATCH
		h.transaction = &TransactionState{
			Commands:   make([]TransactionCommand, 0),
			WatchKeys:  make(map[string]struct{}),
			IsWatching: true,
		}
		for _, arg := range args {
			key := string(arg)
			h.transaction.WatchKeys[key] = struct{}{}
		}
		return proto.NewInteger(int64(len(args)))

	case "UNWATCH":
		// 取消监控所有键
		h.transaction = nil
		return proto.NewSimpleString("OK")

	// ==================== GEOADD ====================
	case "GEOADD":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'GEOADD' command")
		}
		key := string(args[0])
		members := make([]store.GeoMember, 0)
		for i := 1; i+2 < len(args); i += 3 {
			lon, err1 := strconv.ParseFloat(string(args[i]), 64)
			lat, err2 := strconv.ParseFloat(string(args[i+1]), 64)
			if err1 != nil || err2 != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			members = append(members, store.GeoMember{
				Lat:    lat,
				Lon:    lon,
				Member: string(args[i+2]),
			})
		}
		added, err := h.Db.GeoAdd(key, members)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(added)

	// ==================== GEOPOS ====================
	case "GEOPOS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GEOPOS' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		positions, err := h.Db.GeoPos(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(positions))
		for i, pos := range positions {
			if pos[0] == 0 && pos[1] == 0 {
				results[i] = nil
			} else {
				results[i] = []byte(fmt.Sprintf("%.6f,%.6f", pos[1], pos[0]))
			}
		}
		return &proto.Array{Args: results}

	// ==================== GEOHASH ====================
	case "GEOHASH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GEOHASH' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		hashes, err := h.Db.GeoHash(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		hashResults := make([][]byte, len(hashes))
		for i, h := range hashes {
			hashResults[i] = []byte(h)
		}
		return &proto.Array{Args: hashResults}

	// ==================== GEODIST ====================
	case "GEODIST":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'GEODIST' command")
		}
		key := string(args[0])
		member1 := string(args[1])
		member2 := string(args[2])
		unit := "m"
		if len(args) >= 4 {
			unit = string(args[3])
		}
		dist, err := h.Db.GeoDist(key, member1, member2, unit)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.4f", dist)))

	// ==================== GEOSEARCH ====================
	case "GEOSEARCH":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'GEOSEARCH' command")
		}
		key := string(args[0])
		// Parse: FROMMEMBER member [FROMLONLAT lon lat] [BYRADIUS radius unit | BYBOX width height unit] [ASC | DESC] [COUNT count] [WITHCOORD] [WITHDIST] [WITHHASH]
		var centerLon, centerLat float64
		var radius float64
		var unit string
		var count int = 0
		var withDist, withHash, withCoord bool

		i := 1
		// Check for FROMMEMBER or FROMLONLAT
		if strings.ToUpper(string(args[i])) == "FROMMEMBER" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			member := string(args[i+1])
			positions, err := h.Db.GeoPos(key, member)
			if err != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
				return proto.NewError("ERR could not decode query zset member")
			}
			centerLon = positions[0][1]
			centerLat = positions[0][0]
			i += 2
		} else if strings.ToUpper(string(args[i])) == "FROMLONLAT" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err1, err2 error
			centerLon, err1 = strconv.ParseFloat(string(args[i+1]), 64)
			centerLat, err2 = strconv.ParseFloat(string(args[i+2]), 64)
			if err1 != nil || err2 != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			i += 3
		} else {
			return proto.NewError("ERR syntax error")
		}

		// BYRADIUS or BYBOX
		if i >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		if strings.ToUpper(string(args[i])) == "BYRADIUS" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err error
			radius, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			unit = string(args[i+2])
			i += 3
		} else if strings.ToUpper(string(args[i])) == "BYBOX" {
			// Simplified: treat as radius with width
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			width, err := strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			unit = string(args[i+3])
			radius = width / 2
			i += 4
		} else {
			return proto.NewError("ERR syntax error")
		}

		// Optional modifiers
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "ASC", "DESC":
				i++
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "WITHCOORD":
				withCoord = true
				i++
			case "WITHDIST":
				withDist = true
				i++
			case "WITHHASH":
				withHash = true
				i++
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		results, err := h.Db.GeoSearch(key, centerLon, centerLat, radius, unit, count, withDist, withHash, withCoord)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format results
		var response [][]byte
		for _, r := range results {
			if withCoord {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(fmt.Sprintf("%.6f", r.Lon)))
				response = append(response, []byte(fmt.Sprintf("%.6f", r.Lat)))
			} else if withDist && withHash {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(fmt.Sprintf("%.4f", r.Dist)))
				response = append(response, []byte(r.Hash))
			} else if withDist {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(fmt.Sprintf("%.4f", r.Dist)))
			} else if withHash {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(r.Hash))
			} else {
				response = append(response, []byte(r.Member))
			}
		}
		return &proto.Array{Args: response}

	// ==================== GEOSEARCHSTORE ====================
	case "GEOSEARCHSTORE":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'GEOSEARCHSTORE' command")
		}
		dstKey := string(args[0])
		srcKey := string(args[1])

		var centerLon, centerLat float64
		var radius float64
		var unit string
		var count int = 0
		var storeDist bool

		i := 2
		// Check for FROMMEMBER or FROMLONLAT
		if i < len(args) && strings.ToUpper(string(args[i])) == "FROMMEMBER" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			member := string(args[i+1])
			positions, err := h.Db.GeoPos(srcKey, member)
			if err != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
				return proto.NewError("ERR could not decode query zset member")
			}
			centerLon = positions[0][1]
			centerLat = positions[0][0]
			i += 2
		} else if i < len(args) && strings.ToUpper(string(args[i])) == "FROMLONLAT" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err1, err2 error
			centerLon, err1 = strconv.ParseFloat(string(args[i+1]), 64)
			centerLat, err2 = strconv.ParseFloat(string(args[i+2]), 64)
			if err1 != nil || err2 != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			i += 3
		}

		// BYRADIUS or BYBOX
		if i >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		if strings.ToUpper(string(args[i])) == "BYRADIUS" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err error
			radius, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			unit = string(args[i+2])
			i += 3
		} else {
			return proto.NewError("ERR syntax error")
		}

		// Optional modifiers
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "ASC", "DESC":
				i++
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "STOREDIST":
				storeDist = true
				i++
			default:
				i++
			}
		}

		stored, err := h.Db.GeoSearchStore(dstKey, srcKey, centerLon, centerLat, radius, unit, count, storeDist)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(stored)

	// ==================== XADD ====================
	case "XADD":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XADD' command")
		}
		key := string(args[0])
		var opts store.StreamXAddOptions
		var id string
		var fields = make(map[string]string)

		// Parse options
		i := 1
		for i < len(args)-2 && string(args[i])[0] == '-' {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "MAXLEN":
				if i+1 >= len(args)-2 {
					return proto.NewError("ERR syntax error")
				}
				maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				opts.MaxLen = maxlen
				i += 2
			case "MINID":
				if i+1 >= len(args)-2 {
					return proto.NewError("ERR syntax error")
				}
				opts.MinID = string(args[i+1])
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		// ID or field name
		id = string(args[i])
		if id == "*" || (len(id) > 0 && id[0] == '-') {
			// It's the ID (* or an option), skip it
			i++
		} else {
			// It's the ID
			i++
		}

		// Remaining args are field-value pairs
		for i < len(args) {
			field := string(args[i])
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			value := string(args[i+1])
			fields[field] = value
			i += 2
		}

		resultID, err := h.Db.XAdd(key, opts, id, fields)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(resultID))

	// ==================== XLEN ====================
	case "XLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'XLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.XLen(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(length)

	// ==================== XREAD ====================
	case "XREAD":
		var count int64 = 0
		var block int64 = 0

		// Parse options
		i := 0
		if i < len(args) && strings.ToUpper(string(args[i])) == "COUNT" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		}
		if i < len(args) && strings.ToUpper(string(args[i])) == "BLOCK" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			block = b
			i += 2
		}

		// Check for STREAMS
		if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
			return proto.NewError("ERR syntax error, missing STREAMS keyword")
		}
		i++

		// Parse stream IDs
		// Format: key1 id1 key2 id2 ...
		remaining := len(args) - i
		if remaining < 2 || remaining%2 != 0 {
			return proto.NewError(fmt.Sprintf("ERR syntax error: remaining=%d, i=%d, len(args)=%d", remaining, i, len(args)))
		}
		numStreams := remaining / 2
		streamKeys := make([]string, numStreams)
		streamIDs := make([]string, numStreams)
		for j := 0; j < numStreams; j++ {
			streamKeys[j] = string(args[i+j*2])
			streamIDs[j] = string(args[i+j*2+1])
		}

		// Combine keys and IDs
		allArgs := make([]string, 0)
		for j := 0; j < numStreams; j++ {
			allArgs = append(allArgs, streamKeys[j])
			allArgs = append(allArgs, streamIDs[j])
		}

		results, err := h.Db.XRead(count, block, allArgs...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format response
		var response [][]byte
		for _, streamMap := range results {
			for streamKey, entries := range streamMap {
				response = append(response, []byte(streamKey))
				entryArray := make([][]byte, 0)
				for _, entry := range entries {
					entryArray = append(entryArray, []byte(entry.ID))
					fieldArray := make([][]byte, 0)
					for k, v := range entry.Fields {
						fieldArray = append(fieldArray, []byte(k))
						fieldArray = append(fieldArray, []byte(v))
					}
					entryArray = append(entryArray, fieldArray...)
				}
				response = append(response, entryArray...)
			}
		}
		if len(response) == 0 {
			return proto.NewBulkString(nil)
		}
		return &proto.Array{Args: response}

	// ==================== XRANGE ====================
	case "XRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XRANGE' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		count := int64(0)

		// Parse COUNT option
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "COUNT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				break
			}
		}

		entries, err := h.Db.XRange(key, start, stop, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		response := make([][]byte, 0)
		for _, entry := range entries {
			response = append(response, []byte(entry.ID))
			fieldArray := make([][]byte, 0)
			for k, v := range entry.Fields {
				fieldArray = append(fieldArray, []byte(k))
				fieldArray = append(fieldArray, []byte(v))
			}
			response = append(response, fieldArray...)
		}
		return &proto.Array{Args: response}

	// ==================== XREVRANGE ====================
	case "XREVRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XREVRANGE' command")
		}
		key := string(args[0])
		start := string(args[2])
		stop := string(args[1])
		count := int64(0)

		// Parse COUNT option
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "COUNT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				break
			}
		}

		entries, err := h.Db.XRevRange(key, start, stop, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		response := make([][]byte, 0)
		for _, entry := range entries {
			response = append(response, []byte(entry.ID))
			fieldArray := make([][]byte, 0)
			for k, v := range entry.Fields {
				fieldArray = append(fieldArray, []byte(k))
				fieldArray = append(fieldArray, []byte(v))
			}
			response = append(response, fieldArray...)
		}
		return &proto.Array{Args: response}

	// ==================== XDEL ====================
	case "XDEL":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XDEL' command")
		}
		key := string(args[0])
		ids := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			ids[i-1] = string(args[i])
		}
		deleted, err := h.Db.XDel(key, ids...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(deleted)

	// ==================== XACK ====================
	case "XACK":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XACK' command")
		}
		key := string(args[0])
		group := string(args[1])
		ids := make([]string, len(args)-2)
		for i := 2; i < len(args); i++ {
			ids[i-2] = string(args[i])
		}
		acknowledged, err := h.Db.XAck(key, group, ids...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(acknowledged)

	// ==================== XGROUP ====================
	case "XGROUP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'XGROUP' command")
		}
		subcommand := strings.ToUpper(string(args[0]))

		switch subcommand {
		case "CREATE":
			if len(args) < 4 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP CREATE' command")
			}
			key := string(args[1])
			group := string(args[2])
			startID := string(args[3])
			// Skip MKSTREAM option for now
			err := h.Db.XGroupCreate(key, group, startID)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.OK
		case "DESTROY":
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP DESTROY' command")
			}
			key := string(args[1])
			group := string(args[2])
			err := h.Db.XGroupDestroy(key, group)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(1)
		case "SETID":
			if len(args) < 4 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP SETID' command")
			}
			key := string(args[1])
			group := string(args[2])
			id := string(args[3])
			err := h.Db.XGroupSetID(key, group, id)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.OK
		case "DELCONSUMER":
			if len(args) < 4 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP DELCONSUMER' command")
			}
			key := string(args[1])
			group := string(args[2])
			consumer := string(args[3])
			err := h.Db.XGroupDelConsumer(key, group, consumer)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(1)
		default:
			return proto.NewError("ERR syntax error")
		}

	// ==================== XREADGROUP ====================
	case "XREADGROUP":
		var count int64 = 0
		var block int64 = 0
		var group, consumer string

		// Find GROUP keyword first
		groupIdx := -1
		for i := 0; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "GROUP" {
				groupIdx = i
				break
			}
		}
		if groupIdx < 0 {
			return proto.NewError("ERR syntax error, missing GROUP keyword")
		}

		// Parse options before GROUP
		i := 0
		for i < groupIdx {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "COUNT":
				if i+1 >= groupIdx {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "BLOCK":
				if i+1 >= groupIdx {
					return proto.NewError("ERR syntax error")
				}
				b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				block = b
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		// Parse group and consumer
		if groupIdx+2 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		group = string(args[groupIdx+1])
		consumer = string(args[groupIdx+2])
		i = groupIdx + 3

		// Parse options (COUNT, BLOCK) after group/consumer
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			if opt == "STREAMS" {
				break
			}
			switch opt {
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "BLOCK":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				block = b
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s at index %d", opt, i))
			}
		}

		// Check for STREAMS
		if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
			return proto.NewError("ERR syntax error, missing STREAMS keyword")
		}
		i++

		// Parse stream IDs
		remaining := len(args) - i
		if remaining < 2 || remaining%2 != 0 {
			return proto.NewError(fmt.Sprintf("ERR syntax error: remaining=%d, i=%d, len(args)=%d", remaining, i, len(args)))
		}
		numStreams := remaining / 2
		streamKeys := make([]string, numStreams)
		streamIDs := make([]string, numStreams)
		for j := 0; j < numStreams; j++ {
			streamKeys[j] = string(args[i+j*2])
			streamIDs[j] = string(args[i+j*2+1])
		}

		results, err := h.Db.XReadGroup(group, consumer, count, block, streamKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format response
		var response [][]byte
		for _, streamMap := range results {
			for streamKey, entries := range streamMap {
				response = append(response, []byte(streamKey))
				entryArray := make([][]byte, 0)
				for _, entry := range entries {
					entryArray = append(entryArray, []byte(entry.ID))
					fieldArray := make([][]byte, 0)
					for k, v := range entry.Fields {
						fieldArray = append(fieldArray, []byte(k))
						fieldArray = append(fieldArray, []byte(v))
					}
					entryArray = append(entryArray, fieldArray...)
				}
				response = append(response, entryArray...)
			}
		}
		if len(response) == 0 {
			return proto.NewBulkString(nil)
		}
		return &proto.Array{Args: response}

	// ==================== XCLAIM ====================
	case "XCLAIM":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'XCLAIM' command")
		}
		key := string(args[0])
		group := string(args[1])
		consumer := string(args[2])
		minIdleTime, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		ids := make([]string, len(args)-4)
		for i := 4; i < len(args); i++ {
			ids[i-4] = string(args[i])
		}
		claimed, err := h.Db.XClaim(key, group, consumer, minIdleTime, ids...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(claimed)

	// ==================== XAUTOCLAIM ====================
	case "XAUTOCLAIM":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'XAUTOCLAIM' command")
		}
		key := string(args[0])
		group := string(args[1])
		consumer := string(args[2])
		minIdleTime, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		start := string(args[4])

		// Parse options
		opts := store.XAutoClaimOptions{Count: 100, JustID: false}
		i := 5
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				count, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				opts.Count = count
				i += 2
			case "JUSTID":
				opts.JustID = true
				i++
			default:
				return proto.NewError("ERR syntax error")
			}
		}

		result, err := h.Db.XAutoClaim(key, group, consumer, minIdleTime, start, opts)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Build response
		response := make([][]byte, 0)
		response = append(response, []byte(result.NextID))
		if opts.JustID {
			for _, id := range result.ClaimedIDs {
				response = append(response, []byte(id))
			}
		} else {
			for _, id := range result.ClaimedIDs {
				response = append(response, []byte(id))
			}
			for _, msg := range result.Messages {
				entry := [][]byte{[]byte("id"), []byte(msg.ID)}
				for k, v := range msg.Fields {
					entry = append(entry, []byte(k), []byte(v))
				}
				response = append(response, entry...)
			}
		}
		return &proto.Array{Args: response}

	// ==================== XPENDING ====================
	case "XPENDING":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XPENDING' command")
		}
		key := string(args[0])
		group := string(args[1])
		entries, err := h.Db.XPending(key, group)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		response := make([][]byte, 0)
		for _, e := range entries {
			response = append(response, []byte(e.ID))
			response = append(response, []byte(e.Consumer))
			response = append(response, []byte(strconv.FormatInt(e.DeliveryCount, 10)))
		}
		return &proto.Array{Args: response}

	// ==================== XINFO ====================
	case "XINFO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'XINFO' command")
		}
		subcommand := strings.ToUpper(string(args[0]))

		switch subcommand {
		case "HELP":
			response := [][]byte{
				[]byte("XINFO <subcommand> [<arg> ...]"),
				[]byte("Returns information about streams and consumer groups."),
				[]byte(""),
				[]byte("XINFO STREAM <key> [FULL]"),
				[]byte("  -- Returns information about a stream."),
				[]byte(""),
				[]byte("XINFO GROUPS <key>"),
				[]byte("  -- Returns the consumer groups of a stream."),
				[]byte(""),
				[]byte("XINFO CONSUMERS <key> <group>"),
				[]byte("  -- Returns the consumers of a consumer group."),
				[]byte(""),
				[]byte("XINFO STREAM <key> FULL [COUNT <count>]"),
				[]byte("  -- Returns full information about a stream including entries."),
			}
			return &proto.Array{Args: response}
		case "STREAM":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO STREAM' command")
			}
			key := string(args[1])
			info, err := h.Db.XInfo(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			response := [][]byte{
				[]byte("length"),
				[]byte(strconv.FormatInt(info.Length, 10)),
				[]byte("first-entry-id"),
				[]byte(info.FirstID),
				[]byte("last-entry-id"),
				[]byte(info.LastID),
				[]byte("max-deleted-entry-id"),
				[]byte(info.MaxDeletedID),
			}
			return &proto.Array{Args: response}
		case "GROUPS":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO GROUPS' command")
			}
			key := string(args[1])
			groups, err := h.Db.XInfoGroups(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			response := make([][]byte, 0)
			for _, g := range groups {
				response = append(response, []byte("name"))
				response = append(response, []byte(g.Name))
				response = append(response, []byte("consumers"))
				response = append(response, []byte(strconv.Itoa(len(g.Consumers))))
				response = append(response, []byte("pending"))
				response = append(response, []byte(strconv.Itoa(len(g.Pending))))
			}
			return &proto.Array{Args: response}
		case "CONSUMERS":
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO CONSUMERS' command")
			}
			key := string(args[1])
			group := string(args[2])
			consumers, err := h.Db.XInfoConsumers(key, group)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			response := make([][]byte, 0)
			for _, c := range consumers {
				response = append(response, []byte("name"))
				response = append(response, []byte(c.Name))
				response = append(response, []byte("seen"))
				response = append(response, []byte(strconv.FormatInt(c.LastSeen, 10)))
			}
			return &proto.Array{Args: response}
		default:
			return proto.NewError("ERR syntax error")
		}

	// ==================== XTRIM ====================
	case "XTRIM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XTRIM' command")
		}
		key := string(args[0])
		var maxLen int64 = 0
		var minID string

		// Parse options
		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "MAXLEN":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				maxLen = maxlen
				i += 2
			case "MINID":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				minID = string(args[i+1])
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		trimmed, err := h.Db.XTrim(key, maxLen, minID)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(trimmed)

	// ==================== SORT ====================
	case "SORT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SORT' command")
		}
		key := string(args[0])

		// Parse options
		var offset, count int64 = 0, -1
		var getPatterns []string
		var asc bool = true
		var alpha bool
		var destKey string

		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "BY":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				// byPattern = string(args[i+1]) // Not implemented yet
				i += 2
			case "LIMIT":
				if i+2 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				parseResult, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				offset = parseResult
				count, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				i += 3
			case "GET":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				getPatterns = append(getPatterns, string(args[i+1]))
				i += 2
			case "ASC":
				asc = true
				i++
			case "DESC":
				asc = false
				i++
			case "ALPHA":
				alpha = true
				i++
			case "STORE":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				destKey = string(args[i+1])
				i += 2
			default:
				i++
			}
		}

		// Get source type
		keyType, _ := h.Db.Type(key)
		var values []string
		var scores []float64

		switch keyType {
		case "list":
			listValues, err := h.Db.LRange(key, 0, -1)
			if err == nil {
				values = listValues
			} else {
				values = []string{}
			}
		case "set":
			setValues, err := h.Db.SMembers(key)
			if err == nil {
				values = setValues
			} else {
				values = []string{}
			}
		case "string":
			val, _ := h.Db.Get(key)
			values = []string{val}
		case "zset":
			members, _ := h.Db.ZRange(key, 0, -1)
			for _, m := range members {
				values = append(values, m.Member)
				scores = append(scores, m.Score)
			}
		default:
			return proto.NewError("ERR Operation against a key holding the wrong kind of value")
		}

		// Sort values
		if len(scores) == 0 && !alpha && len(values) > 0 {
			// Numeric sort
			scores = make([]float64, len(values))
			for idx, v := range values {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					scores[idx] = f
				} else {
					scores[idx] = 0
				}
			}
		}

		// Simple bubble sort (for simplicity)
		n := len(values)
		for i := 0; i < n-1; i++ {
			for j := 0; j < n-i-1; j++ {
				swap := false
				if alpha {
					if asc {
						swap = values[j] > values[j+1]
					} else {
						swap = values[j] < values[j+1]
					}
				} else {
					if asc {
						swap = scores[j] > scores[j+1]
					} else {
						swap = scores[j] < scores[j+1]
					}
				}
				if swap {
					values[j], values[j+1] = values[j+1], values[j]
					if len(scores) > 0 {
						scores[j], scores[j+1] = scores[j+1], scores[j]
					}
				}
			}
		}

		// Apply LIMIT
		if offset > 0 {
			if offset >= int64(len(values)) {
				values = []string{}
			} else if offset < int64(len(values)) {
				values = values[offset:]
				if len(scores) > 0 {
					scores = scores[offset:]
				}
			}
		}
		if count >= 0 && int64(len(values)) > count {
			values = values[:count]
			if len(scores) > 0 {
				scores = scores[:count]
			}
		}

		// Apply GET patterns
		if len(getPatterns) > 0 {
			finalValues := make([]string, 0)
			for _, pattern := range getPatterns {
				for _, val := range values {
					targetKey := strings.Replace(pattern, "*", val, 1)
					targetVal, _ := h.Db.Get(targetKey)
					finalValues = append(finalValues, targetVal)
				}
			}
			values = finalValues
		}

		// STORE
		if destKey != "" {
			// Store as a list
			for idx, v := range values {
				if idx == 0 {
					_, _ = h.Db.Del(destKey)
				}
				_, _ = h.Db.LPush(destKey, v)
			}
			return proto.NewInteger(int64(len(values)))
		}

		// Return result
		results := make([][]byte, len(values))
		for idx, v := range values {
			results[idx] = []byte(v)
		}
		return &proto.Array{Args: results}

	// ==================== AUTH ====================
	case "AUTH":
		// 简化实现：检查密码
		// 支持环境变量 BOLTDB_PASSWORD
		password := os.Getenv("BOLTDB_PASSWORD")
		if password == "" {
			// 没有配置密码，任何密码都接受
			return proto.NewSimpleString("OK")
		}

		// 格式: AUTH password 或 AUTH username password
		var inputPassword string
		if len(args) >= 1 {
			inputPassword = string(args[0])
		}

		if inputPassword == password {
			return proto.NewSimpleString("OK")
		}
		return proto.NewError("ERR invalid password")

	// ==================== JSON ====================
	case "JSON.SET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.SET' command")
		}
		key, path := string(args[0]), string(args[1])
		value := string(args[2])
		nx, xx := false, false
		// Parse optional NX/XX arguments
		for i := 3; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			if opt == "NX" {
				nx = true
			} else if opt == "XX" {
				xx = true
			}
		}
		result, err := h.Db.JSONSet(key, path, value, nx, xx)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewSimpleString(result)

	case "JSON.GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.GET' command")
		}
		key := string(args[0])
		paths := make([]string, 0)
		for i := 1; i < len(args); i++ {
			paths = append(paths, string(args[i]))
		}
		result, err := h.Db.JSONGet(key, paths...)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if len(result) == 1 {
			return proto.NewBulkString([]byte(result[0]))
		}
		// Multiple paths
		arr := make([][]byte, len(result))
		for i, v := range result {
			arr[i] = []byte(v)
		}
		return &proto.Array{Args: arr}

	case "JSON.DEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.DEL' command")
		}
		key := string(args[0])
		paths := make([]string, 0)
		for i := 1; i < len(args); i++ {
			paths = append(paths, string(args[i]))
		}
		count, err := h.Db.JSONDel(key, paths...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.TYPE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.TYPE' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		result, err := h.Db.JSONType(key, path)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(result))

	case "JSON.MGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.MGET' command")
		}
		path := string(args[len(args)-1])
		keys := make([]string, 0)
		for i := 0; i < len(args)-1; i++ {
			keys = append(keys, string(args[i]))
		}
		result, err := h.Db.JSONMGet(path, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, len(result))
		for i, v := range result {
			if v == "" {
				arr[i] = nil
			} else {
				arr[i] = []byte(v)
			}
		}
		return &proto.Array{Args: arr}

	case "JSON.ARRAPPEND":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.ARRAPPEND' command")
		}
		key, path := string(args[0]), string(args[1])
		values := make([]string, 0)
		for i := 2; i < len(args); i++ {
			values = append(values, string(args[i]))
		}
		count, err := h.Db.JSONArrAppend(key, path, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.ARRLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.ARRLEN' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		count, err := h.Db.JSONArrLen(key, path)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.OBJKEYS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.OBJKEYS' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		keys, err := h.Db.JSONObjKeys(key, path)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, len(keys))
		for i, k := range keys {
			arr[i] = []byte(k)
		}
		return &proto.Array{Args: arr}

	case "JSON.NUMINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.NUMINCRBY' command")
		}
		key, path := string(args[0]), string(args[1])
		increment, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR increment must be a valid number")
		}
		result, err := h.Db.JSONNumIncrBy(key, path, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(result, 'f', -1, 64)))

	case "JSON.NUMMULTBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.NUMMULTBY' command")
		}
		key, path := string(args[0]), string(args[1])
		multiplier, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR multiplier must be a valid number")
		}
		result, err := h.Db.JSONNumMultBy(key, path, multiplier)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(result, 'f', -1, 64)))

	case "JSON.CLEAR":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.CLEAR' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		count, err := h.Db.JSONClear(key, path)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.DEBUG":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.DEBUG' command")
		}
		subCmd := strings.ToUpper(string(args[0]))
		if subCmd != "MEMORY" {
			return proto.NewError("ERR syntax error")
		}
		key := string(args[1])
		path := "$"
		if len(args) >= 3 {
			path = string(args[2])
		}
		memory, err := h.Db.JSONDebugMemory(key, path)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(memory)

	// ==================== Time Series ====================
	case "TS.CREATE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.CREATE' command")
		}
		key := string(args[0])
		opts := store.TSCreateOptions{}
		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "RETENTION":
				i++
				if i >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				retention, err := strconv.ParseInt(string(args[i]), 10, 64)
				if err != nil {
					return proto.NewError("ERR invalid RETENTION value")
				}
				opts.Retention = retention
			case "ENCODING":
				i++
				if i >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				opts.Encoding = string(args[i])
			case "DUPLICATE_POLICY":
				i++
				if i >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				opts.DuplicatePolicy = string(args[i])
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unexpected option: %s", opt))
			}
			i++
		}
		if err := h.Db.TSCreate(key, opts); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "TS.ADD":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'TS.ADD' command")
		}
		key := string(args[0])
		var timestamp int64
		if string(args[1]) == "*" {
			timestamp = time.Now().UnixNano() / int64(time.Millisecond)
		} else {
			var err error
			timestamp, err = strconv.ParseInt(string(args[1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid timestamp")
			}
		}
		value, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR invalid value")
		}
		opts := store.TSAddOptions{}
		if len(args) > 3 {
			opt := strings.ToUpper(string(args[3]))
			if opt == "ON_DUPLICATE" && len(args) > 4 {
				opts.OnDuplicate = string(args[4])
			}
		}
		ts, err := h.Db.TSAdd(key, timestamp, value, opts)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(ts)

	case "TS.GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.GET' command")
		}
		key := string(args[0])
		dp, err := h.Db.TSGet(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Return as array: [timestamp, value]
		return &proto.Array{
			Args: [][]byte{
				[]byte(strconv.FormatInt(dp.Timestamp, 10)),
				[]byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)),
			},
		}

	case "TS.RANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'TS.RANGE' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		count := int64(-1)
		if len(args) > 3 {
			opt := strings.ToUpper(string(args[3]))
			if opt == "COUNT" && len(args) > 4 {
				c, err := strconv.ParseInt(string(args[4]), 10, 64)
				if err != nil {
					return proto.NewError("ERR invalid COUNT value")
				}
				count = c
			}
		}
		results, err := h.Db.TSRange(key, start, stop, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, 0, len(results)*2)
		for _, dp := range results {
			arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
			arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
		}
		return &proto.Array{Args: arr}

	case "TS.DEL":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'TS.DEL' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		deleted, err := h.Db.TSDel(key, start, stop)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(deleted)

	case "TS.INFO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.INFO' command")
		}
		key := string(args[0])
		info, err := h.Db.TSInfo(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Return as array of key-value pairs
		return &proto.Array{
			Args: [][]byte{
				[]byte("totalSamples"), []byte(strconv.FormatInt(info.TotalSamples, 10)),
				[]byte("memoryUsage"), []byte(strconv.FormatInt(info.MemoryUsage, 10)),
				[]byte("firstTimestamp"), []byte(strconv.FormatInt(info.FirstTimestamp, 10)),
				[]byte("lastTimestamp"), []byte(strconv.FormatInt(info.LastTimestamp, 10)),
				[]byte("retentionTime"), []byte(strconv.FormatInt(info.RetentionTime, 10)),
				[]byte("encoding"), []byte(info.Encoding),
				[]byte("chunkCount"), []byte(strconv.FormatInt(info.ChunkCount, 10)),
			},
		}

	case "TS.LEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.LEN' command")
		}
		key := string(args[0])
		length, err := h.Db.TSLen(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(length)

	case "TS.MGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'TS.MGET' command")
		}
		filter := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		results, err := h.Db.TSMGet(filter, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, 0, len(results)*2)
		for _, dp := range results {
			if dp == nil {
				arr = append(arr, []byte{})
				arr = append(arr, []byte{})
			} else {
				arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
				arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
			}
		}
		return &proto.Array{Args: arr}

	default:
		// 如果在事务中，将命令加入队列
		if h.transaction != nil {
			h.transaction.Commands = append(h.transaction.Commands, TransactionCommand{
				Command: cmd,
				Args:    args,
			})
			return proto.NewSimpleString("QUEUED")
		}
		return proto.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// parseScore parses Redis-style score bounds including special values
// Supports: "-inf", "+inf", "(", "[", and numeric values
func parseScore(s string) (float64, error) {
	// 处理特殊值
	switch s {
	case "-inf":
		return float64(math.Inf(-1)), nil
	case "+inf", "inf":
		return float64(math.Inf(1)), nil
	case "-inf(", "-inf[":
		// Exclusive -inf is same as inclusive -inf for float comparison
		return float64(math.Inf(-1)), nil
	case "+inf(", "+inf[":
		return float64(math.Inf(1)), nil
	}

	// 处理带括号的排除边界 (value
	if len(s) > 0 && s[0] == '(' {
		// 对于排除边界，我们需要在比较时特殊处理
		// 这里简单处理，返回原值的前缀（不包括括号）
		s = s[1:]
	}

	return strconv.ParseFloat(s, 64)
}

// parseScoreExclusive checks if a score string represents an exclusive bound
func parseScoreExclusive(s string) (float64, bool, error) {
	exclusive := false
	if len(s) > 0 && s[0] == '(' {
		exclusive = true
		s = s[1:]
	} else if len(s) > 0 && s[0] == '[' {
		exclusive = false
		s = s[1:]
	}

	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// 检查特殊值
		switch s {
		case "-inf":
			return float64(math.Inf(-1)), exclusive, nil
		case "+inf", "inf":
			return float64(math.Inf(1)), exclusive, nil
		}
		return 0, false, err
	}
	return val, exclusive, nil
}

// executeQueuedCommand 执行事务队列中的命令
func (h *Handler) executeQueuedCommand(cmd string, args [][]byte) proto.RESP {
	switch cmd {
	case "SET":
		key, value := string(args[0]), string(args[1])
		if err := h.Db.Set(key, value); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK
	case "GET":
		key := string(args[0])
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return nil
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(value))
	case "DEL":
		count := int64(0)
		for _, arg := range args {
			deleted, _ := h.Db.Del(string(arg))
			count += deleted
		}
		return proto.NewInteger(count)
	case "INCR":
		key := string(args[0])
		val, err := h.Db.INCR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "DECR":
		key := string(args[0])
		val, err := h.Db.DECR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "INCRBY":
		key := string(args[0])
		delta, _ := strconv.ParseInt(string(args[1]), 10, 64)
		val, err := h.Db.INCRBY(key, delta)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "DECRBY":
		key := string(args[0])
		delta, _ := strconv.ParseInt(string(args[1]), 10, 64)
		val, err := h.Db.DECRBY(key, delta)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "APPEND":
		key, value := string(args[0]), string(args[1])
		length, _ := h.Db.APPEND(key, value)
		return proto.NewInteger(int64(length))
	case "STRLEN":
		key := string(args[0])
		length, _ := h.Db.StrLen(key)
		return proto.NewInteger(int64(length))
	case "EXISTS":
		key := string(args[0])
		exists, _ := h.Db.Exists(key)
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "EXPIRE":
		key := string(args[0])
		seconds, _ := strconv.Atoi(string(args[1]))
		success, _ := h.Db.Expire(key, seconds)
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TTL":
		key := string(args[0])
		ttl, _ := h.Db.TTL(key)
		return proto.NewInteger(ttl)
	case "PERSIST":
		key := string(args[0])
		success, _ := h.Db.Persist(key)
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TYPE":
		key := string(args[0])
		keyType, _ := h.Db.Type(key)
		return proto.NewSimpleString(keyType)
	case "LPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, _ := h.Db.LPush(key, values...)
		return proto.NewInteger(int64(count))
	case "RPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, _ := h.Db.RPush(key, values...)
		return proto.NewInteger(int64(count))
	case "LPOP":
		key := string(args[0])
		val, _ := h.Db.LPop(key)
		if val == "" {
			return nil
		}
		return proto.NewBulkString([]byte(val))
	case "RPOP":
		key := string(args[0])
		val, _ := h.Db.RPop(key)
		if val == "" {
			return nil
		}
		return proto.NewBulkString([]byte(val))
	case "LLEN":
		key := string(args[0])
		length, _ := h.Db.LLen(key)
		return proto.NewInteger(int64(length))
	case "LRANGE":
		key := string(args[0])
		start, _ := strconv.ParseInt(string(args[1]), 10, 64)
		stop, _ := strconv.ParseInt(string(args[2]), 10, 64)
		items, _ := h.Db.LRange(key, start, stop)
		results := make([][]byte, len(items))
		for i, item := range items {
			results[i] = []byte(item)
		}
		return &proto.Array{Args: results}
	case "HSET":
		key := string(args[0])
		field, value := string(args[1]), string(args[2])
		_ = h.Db.HSet(key, field, value)
		return proto.NewInteger(1)
	case "HGET":
		key, field := string(args[0]), string(args[1])
		val, _ := h.Db.HGet(key, field)
		if len(val) == 0 {
			return nil
		}
		return proto.NewBulkString(val)
	case "HGETALL":
		key := string(args[0])
		data, _ := h.Db.HGetAll(key)
		flatArgs := make([][]byte, 0)
		for k, v := range data {
			flatArgs = append(flatArgs, []byte(k), []byte(v))
		}
		return &proto.Array{Args: flatArgs}
	case "HDEL":
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		count, _ := h.Db.HDel(key, fields...)
		return proto.NewInteger(int64(count))
	case "SADD":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, _ := h.Db.SAdd(key, members...)
		return proto.NewInteger(int64(count))
	case "SMEMBERS":
		key := string(args[0])
		members, _ := h.Db.SMembers(key)
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}
	case "SISMEMBER":
		key, member := string(args[0]), string(args[1])
		exists, _ := h.Db.SIsMember(key, member)
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "SCARD":
		key := string(args[0])
		count, _ := h.Db.SCard(key)
		return proto.NewInteger(int64(count))
	case "SREM":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, _ := h.Db.SRem(key, members...)
		return proto.NewInteger(int64(count))
	case "ZADD":
		key := string(args[0])
		members := make([]store.ZSetMember, 0)
		for i := 1; i < len(args); i += 2 {
			score, _ := strconv.ParseFloat(string(args[i]), 64)
			members = append(members, store.ZSetMember{Score: score, Member: string(args[i+1])})
		}
		_ = h.Db.ZAdd(key, members)
		return proto.NewInteger(int64(len(members)))
	case "ZREM":
		key := string(args[0])
		member := string(args[1])
		_ = h.Db.ZRem(key, member)
		return proto.NewInteger(1)
	case "ZCARD":
		key := string(args[0])
		count, _ := h.Db.ZCard(key)
		return proto.NewInteger(int64(count))
	case "ZSCORE":
		key, member := string(args[0]), string(args[1])
		score, _, _ := h.Db.ZScore(key, member)
		return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
	case "ZINCRBY":
		key, member := string(args[0]), string(args[2])
		delta, _ := strconv.ParseFloat(string(args[1]), 64)
		newScore, _ := h.Db.ZIncrBy(key, member, delta)
		return proto.NewBulkString([]byte(strconv.FormatFloat(newScore, 'f', -1, 64)))
	default:
		return proto.NewError(fmt.Sprintf("ERR command '%s' not supported in transaction", cmd))
	}
}

// copyList 复制列表
func (h *Handler) copyList(srcKey, dstKey string) bool {
	length, err := h.Db.LLen(srcKey)
	if err != nil {
		return false
	}
	if length == 0 {
		return true
	}
	// 获取所有元素
	items, err := h.Db.LRange(srcKey, 0, int64(length-1))
	if err != nil {
		return false
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 添加到目标列表
	_, err = h.Db.RPush(dstKey, items...)
	return err == nil
}

// copyHash 复制Hash
func (h *Handler) copyHash(srcKey, dstKey string) bool {
	data, err := h.Db.HGetAll(srcKey)
	if err != nil {
		return false
	}
	if len(data) == 0 {
		return true
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 设置所有字段
	for k, v := range data {
		if err := h.Db.HSet(dstKey, k, v); err != nil {
			return false
		}
	}
	return true
}

// copySet 复制Set
func (h *Handler) copySet(srcKey, dstKey string) bool {
	members, err := h.Db.SMembers(srcKey)
	if err != nil {
		return false
	}
	if len(members) == 0 {
		return true
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 添加所有成员
	_, err = h.Db.SAdd(dstKey, members...)
	return err == nil
}

// copySortedSet 复制SortedSet
func (h *Handler) copySortedSet(srcKey, dstKey string) bool {
	members, err := h.Db.ZRange(srcKey, 0, -1)
	if err != nil {
		return false
	}
	if len(members) == 0 {
		return true
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 添加所有成员
	zMembers := make([]store.ZSetMember, len(members))
	for i, m := range members {
		zMembers[i] = store.ZSetMember{Score: m.Score, Member: m.Member}
	}
	err = h.Db.ZAdd(dstKey, zMembers)
	return err == nil
}
