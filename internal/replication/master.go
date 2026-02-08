package replication

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// MasterConnection 表示到主节点的连接
type MasterConnection struct {
	Addr          string
	Conn          net.Conn
	Reader        *bufio.Reader
	Writer        *bufio.Writer
	ReplOffset    int64
	ReplId        string
	mu            sync.RWMutex
	stopCh        chan struct{}
}

// NewMasterConnection 创建新的主节点连接
func NewMasterConnection(addr string) (*MasterConnection, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial master failed: %w", err)
	}

	mc := &MasterConnection{
		Addr:   addr,
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
		stopCh: make(chan struct{}),
	}

	logger.Logger.Info().
		Str("master_addr", addr).
		Msg("连接到主节点")

	return mc, nil
}

// SendCommand 发送命令到主节点
func (mc *MasterConnection) SendCommand(cmd [][]byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	req := &proto.Array{Args: cmd}
	if err := proto.WriteRESP(mc.Writer, req); err != nil {
		return fmt.Errorf("write command failed: %w", err)
	}

	if err := mc.Writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}

// ReadResponse 读取响应
func (mc *MasterConnection) ReadResponse() (proto.RESP, error) {
	mc.mu.RLock()
	reader := mc.Reader
	mc.mu.RUnlock()

	// 读取响应类型
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	// 去掉\r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	// 根据响应类型解析
	switch line[0] {
	case '+': // Simple String
		return proto.NewSimpleString(string(line[1:])), nil
	case '-': // Error
		return proto.NewError(string(line[1:])), nil
	case ':': // Integer
		// 简化处理，实际应该解析整数
		return proto.NewSimpleString(string(line[1:])), nil
	case '$': // Bulk String
		// 需要读取长度和数据
		return proto.ReadRESP(reader)
	case '*': // Array
		return proto.ReadRESP(reader)
	default:
		return proto.NewSimpleString(string(line)), nil
	}
}

// ReadBulkString 读取Bulk String(用于RDB传输)
func (mc *MasterConnection) ReadBulkString() ([]byte, error) {
	mc.mu.RLock()
	reader := mc.Reader
	mc.mu.RUnlock()

	// 读取长度行
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read bulk string length failed: %w", err)
	}

	// 去掉\r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) == 0 || line[0] != '$' {
		return nil, fmt.Errorf("invalid bulk string format")
	}

	// 解析长度
	var length int
	if _, err := fmt.Sscanf(string(line[1:]), "%d", &length); err != nil {
		return nil, fmt.Errorf("parse bulk string length failed: %w", err)
	}

	if length < 0 {
		return nil, nil // NULL bulk string
	}

	// 读取数据
	data := make([]byte, length+2) // +2 for \r\n
	if _, err := reader.Read(data); err != nil {
		return nil, fmt.Errorf("read bulk string data failed: %w", err)
	}

	return data[:length], nil
}

// SetReplOffset 设置复制偏移量
func (mc *MasterConnection) SetReplOffset(offset int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.ReplOffset = offset
}

// GetReplOffset 获取复制偏移量
func (mc *MasterConnection) GetReplOffset() int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.ReplOffset
}

// SetReplId 设置复制ID
func (mc *MasterConnection) SetReplId(replId string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.ReplId = replId
}

// GetReplId 获取复制ID
func (mc *MasterConnection) GetReplId() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.ReplId
}

// Close 关闭连接
func (mc *MasterConnection) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	close(mc.stopCh)

	if mc.Conn != nil {
		return mc.Conn.Close()
	}
	return nil
}

// IsClosed 检查连接是否已关闭
func (mc *MasterConnection) IsClosed() bool {
	select {
	case <-mc.stopCh:
		return true
	default:
		return false
	}
}
