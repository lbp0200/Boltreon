package cluster

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Node 表示集群中的一个节点
type Node struct {
	ID       string   // 节点ID（40字符的十六进制字符串）
	Addr     string   // 节点地址，格式: "host:port"
	Flags    []string // 节点标志，如: "master", "slave", "myself", "fail"
	MasterID string   // 如果是slave，指向master的ID
	PingSent int64    // 最后一次ping发送时间（Unix时间戳，毫秒）
	PongRecv int64    // 最后一次pong接收时间（Unix时间戳，毫秒）
	Epoch    int64    // 配置纪元（config epoch）
	Slots    []SlotRange // 该节点负责的槽位范围
	mu       sync.RWMutex
}

// SlotRange 表示槽位范围
type SlotRange struct {
	Start uint32 // 起始槽位（包含）
	End   uint32 // 结束槽位（包含）
}

// NewNode 创建新节点
func NewNode(id, addr string) *Node {
	return &Node{
		ID:       id,
		Addr:     addr,
		Flags:    []string{},
		Slots:    []SlotRange{},
		PingSent: 0,
		PongRecv: 0,
		Epoch:    0,
	}
}

// AddSlotRange 添加槽位范围
func (n *Node) AddSlotRange(start, end uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Slots = append(n.Slots, SlotRange{Start: start, End: end})
}

// HasSlot 检查节点是否负责指定的槽位
func (n *Node) HasSlot(slot uint32) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, r := range n.Slots {
		if slot >= r.Start && slot <= r.End {
			return true
		}
	}
	return false
}

// GetSlotRanges 获取所有槽位范围
func (n *Node) GetSlotRanges() []SlotRange {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ranges := make([]SlotRange, len(n.Slots))
	copy(ranges, n.Slots)
	return ranges
}

// IsMaster 检查节点是否是master
func (n *Node) IsMaster() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == "master" {
			return true
		}
	}
	return false
}

// IsSlave 检查节点是否是slave
func (n *Node) IsSlave() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == "slave" {
			return true
		}
	}
	return false
}

// IsMyself 检查节点是否是当前节点
func (n *Node) IsMyself() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == "myself" {
			return true
		}
	}
	return false
}

// SetMyself 设置节点为当前节点
func (n *Node) SetMyself() {
	n.mu.Lock()
	defer n.mu.Unlock()
	// 移除旧的myself标志
	newFlags := []string{}
	for _, flag := range n.Flags {
		if flag != "myself" {
			newFlags = append(newFlags, flag)
		}
	}
	n.Flags = append(newFlags, "myself")
}

// UpdatePong 更新pong接收时间
func (n *Node) UpdatePong() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.PongRecv = time.Now().UnixMilli()
}

// UpdatePing 更新ping发送时间
func (n *Node) UpdatePing() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.PingSent = time.Now().UnixMilli()
}

// IsFailed 检查节点是否失败
func (n *Node) IsFailed() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == "fail" {
			return true
		}
	}
	// 如果超过一定时间没有收到pong，认为节点失败
	if n.PongRecv > 0 {
		elapsed := time.Now().UnixMilli() - n.PongRecv
		if elapsed > 5000 { // 5秒超时
			return true
		}
	}
	return false
}

// String 返回节点的字符串表示
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return fmt.Sprintf("%s %s", n.ID, n.Addr)
}

// GetHostPort 解析地址，返回host和port
func (n *Node) GetHostPort() (string, string, error) {
	host, port, err := net.SplitHostPort(n.Addr)
	if err != nil {
		return "", "", err
	}
	return host, port, nil
}

