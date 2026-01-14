package sentinel

import (
	"fmt"
	"sync"
	"time"

	"github.com/lbp0200/Boltreon/internal/logger"
)

// Sentinel 哨兵实例
type Sentinel struct {
	mu            sync.RWMutex
	masters       map[string]*MasterInstance
	quorum        int
	downAfter     time.Duration
	parallelSync  int
	runID         string
	configEpoch   int64
	stopCh        chan struct{}
}

// NewSentinel 创建新的哨兵实例
func NewSentinel(quorum int, downAfter time.Duration) *Sentinel {
	return &Sentinel{
		masters:      make(map[string]*MasterInstance),
		quorum:       quorum,
		downAfter:    downAfter,
		parallelSync: 1,
		runID:        generateRunID(),
		configEpoch:  0,
		stopCh:       make(chan struct{}),
	}
}

// generateRunID 生成运行ID
func generateRunID() string {
	// 简化实现，实际应该生成40字符的十六进制字符串
	return fmt.Sprintf("sentinel-%d", time.Now().UnixNano())
}

// AddMaster 添加主节点监控
func (s *Sentinel) AddMaster(name, addr string, quorum int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.masters[name]; exists {
		return fmt.Errorf("master %s already exists", name)
	}

	master := NewMasterInstance(name, addr, quorum)
	s.masters[name] = master

	logger.Logger.Info().
		Str("master_name", name).
		Str("master_addr", addr).
		Int("quorum", quorum).
		Msg("添加主节点监控")

	return nil
}

// RemoveMaster 移除主节点监控
func (s *Sentinel) RemoveMaster(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if master, exists := s.masters[name]; exists {
		master.Stop()
		delete(s.masters, name)
		logger.Logger.Info().
			Str("master_name", name).
			Msg("移除主节点监控")
		return nil
	}

	return fmt.Errorf("master %s not found", name)
}

// GetMaster 获取主节点实例
func (s *Sentinel) GetMaster(name string) *MasterInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.masters[name]
}

// GetAllMasters 获取所有主节点
func (s *Sentinel) GetAllMasters() map[string]*MasterInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*MasterInstance)
	for k, v := range s.masters {
		result[k] = v
	}
	return result
}

// Start 启动哨兵
func (s *Sentinel) Start() {
	logger.Logger.Info().Msg("启动哨兵")

	// 启动所有主节点的监控
	s.mu.RLock()
	masters := make([]*MasterInstance, 0, len(s.masters))
	for _, master := range s.masters {
		masters = append(masters, master)
	}
	s.mu.RUnlock()

	for _, master := range masters {
		go master.StartMonitoring(s)
	}
}

// Stop 停止哨兵
func (s *Sentinel) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopCh)

	for _, master := range s.masters {
		master.Stop()
	}
}

// GetRunID 获取运行ID
func (s *Sentinel) GetRunID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.runID
}

// GetConfigEpoch 获取配置纪元
func (s *Sentinel) GetConfigEpoch() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configEpoch
}

// IncrementConfigEpoch 增加配置纪元
func (s *Sentinel) IncrementConfigEpoch() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configEpoch++
	return s.configEpoch
}
