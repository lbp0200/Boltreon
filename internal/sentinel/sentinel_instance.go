package sentinel

// SentinelInstance 其他哨兵实例
type SentinelInstance struct {
	ID     string
	Addr   string
	RunID  string
	Epoch  int64
	Quorum int
}

// NewSentinelInstance 创建新的哨兵实例
func NewSentinelInstance(id, addr string) *SentinelInstance {
	return &SentinelInstance{
		ID:   id,
		Addr: addr,
	}
}
