package sentinel

import (
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// MasterInstance 主节点实例
type MasterInstance struct {
	mu            sync.RWMutex
	name          string
	addr          string
	quorum        int
	slaves        []*SlaveInstance
	sentinels     []*SentinelInstance
	state         string // "ok" | "odown" | "sdown" | "failover"
	lastPingTime  time.Time
	lastPongTime  time.Time
	downAfter     time.Duration
	stopCh        chan struct{}
}

// NewMasterInstance 创建新的主节点实例
func NewMasterInstance(name, addr string, quorum int) *MasterInstance {
	return &MasterInstance{
		name:      name,
		addr:      addr,
		quorum:    quorum,
		slaves:    make([]*SlaveInstance, 0),
		sentinels: make([]*SentinelInstance, 0),
		state:     "ok",
		downAfter: 30 * time.Second,
		stopCh:    make(chan struct{}),
	}
}

// StartMonitoring 开始监控主节点
func (mi *MasterInstance) StartMonitoring(sentinel *Sentinel) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-mi.stopCh:
			return
		case <-ticker.C:
			mi.checkMaster(sentinel)
		}
	}
}

// checkMaster 检查主节点状态
func (mi *MasterInstance) checkMaster(sentinel *Sentinel) {
	// 发送PING
	mi.mu.Lock()
	mi.lastPingTime = time.Now()
	mi.mu.Unlock()

	// 简化实现：实际应该发送PING命令并等待响应
	// 这里暂时模拟成功
	mi.mu.Lock()
	mi.lastPongTime = time.Now()
	elapsed := time.Since(mi.lastPingTime)
	mi.mu.Unlock()

	if elapsed > mi.downAfter {
		// 主节点可能下线
		mi.mu.Lock()
		if mi.state == "ok" {
			mi.state = "sdown" // 主观下线
			logger.Logger.Warn().
				Str("master_name", mi.name).
				Str("master_addr", mi.addr).
				Msg("主节点主观下线")
		}
		mi.mu.Unlock()
	} else {
		mi.mu.Lock()
		if mi.state == "sdown" {
			mi.state = "ok"
			logger.Logger.Info().
				Str("master_name", mi.name).
				Str("master_addr", mi.addr).
				Msg("主节点恢复")
		}
		mi.mu.Unlock()
	}
}

// AddSlave 添加从节点
func (mi *MasterInstance) AddSlave(slave *SlaveInstance) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.slaves = append(mi.slaves, slave)
}

// GetSlaves 获取所有从节点
func (mi *MasterInstance) GetSlaves() []*SlaveInstance {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	result := make([]*SlaveInstance, len(mi.slaves))
	copy(result, mi.slaves)
	return result
}

// AddSentinel 添加哨兵实例
func (mi *MasterInstance) AddSentinel(sentinel *SentinelInstance) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.sentinels = append(mi.sentinels, sentinel)
}

// GetState 获取状态
func (mi *MasterInstance) GetState() string {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.state
}

// SetState 设置状态
func (mi *MasterInstance) SetState(state string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.state = state
}

// GetName 获取名称
func (mi *MasterInstance) GetName() string {
	return mi.name
}

// GetAddr 获取地址
func (mi *MasterInstance) GetAddr() string {
	return mi.addr
}

// Stop 停止监控
func (mi *MasterInstance) Stop() {
	close(mi.stopCh)
}

// IsDown 检查是否下线
func (mi *MasterInstance) IsDown() bool {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.state == "sdown" || mi.state == "odown" || mi.state == "failover"
}
