package sentinel

import (
	"bufio"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// GossipConfig gossip协议配置
type GossipConfig struct {
	Port            int
	RunID           string
	HelloInterval   time.Duration
	PingInterval    time.Duration
	PeerTimeout     time.Duration
	MaxPeers       int
}

// DefaultGossipConfig 默认配置
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		Port:            0, // 随机端口
		HelloInterval:   2 * time.Second,
		PingInterval:    5 * time.Second,
		PeerTimeout:     30 * time.Second,
		MaxPeers:        10,
	}
}

// GossipPeer 远程哨兵对等体
type GossipPeer struct {
	Addr       string
	RunID      string
	LastSeen   time.Time
	HelloSent  bool
}

// GossipProtocol gossip协议管理器
type GossipProtocol struct {
	sentinel *Sentinel
	config   *GossipConfig
	listener net.Listener
	peers    map[string]*GossipPeer
	stopCh   chan struct{}
}

// NewGossipProtocol 创建gossip协议管理器
func NewGossipProtocol(sentinel *Sentinel, config *GossipConfig) *GossipProtocol {
	if config == nil {
		config = DefaultGossipConfig()
	}

	return &GossipProtocol{
		sentinel: sentinel,
		config:   config,
		peers:    make(map[string]*GossipPeer),
		stopCh:   make(chan struct{}),
	}
}

// Start 启动gossip协议
func (gp *GossipProtocol) Start() error {
	// 监听端口
	var err error
	addr := ":" + strconv.Itoa(gp.config.Port)
	gp.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	logger.Logger.Info().Int("port", gp.listener.Addr().(*net.TCPAddr).Port).Msg("Gossip协议监听端口已启动")

	// 启动接受连接协程
	go gp.acceptConnections()

	// 启动连接管理协程
	go gp.managePeers()

	return nil
}

// Stop 停止gossip协议
func (gp *GossipProtocol) Stop() {
	close(gp.stopCh)

	if gp.listener != nil {
		gp.listener.Close()
	}

	for _, peer := range gp.peers {
		gp.removePeer(peer.Addr)
	}
}

// GetPort 获取监听端口
func (gp *GossipProtocol) GetPort() int {
	if gp.listener == nil {
		return 0
	}
	return gp.listener.Addr().(*net.TCPAddr).Port
}

// acceptConnections 接受传入的连接
func (gp *GossipProtocol) acceptConnections() {
	for {
		select {
		case <-gp.stopCh:
			return
		default:
		}

		if tcpListener, ok := gp.listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := gp.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Logger.Error().Err(err).Msg("接受gossip连接失败")
			continue
		}

		go gp.handleConnection(conn)
	}
}

// handleConnection 处理传入连接
func (gp *GossipProtocol) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-gp.stopCh:
			return
		default:
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetDeadline(time.Now().Add(30 * time.Second))
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		gp.handleMessage(conn, line)
	}
}

// handleMessage 处理gossip消息
func (gp *GossipProtocol) handleMessage(conn net.Conn, line string) {
	parts := strings.Split(line, " ")
	if len(parts) == 0 {
		return
	}

	switch parts[0] {
	case "HELLO":
		gp.handleHello(conn, parts[1:])
	case "PING":
		gp.handlePing(conn)
	case "PONG":
		gp.handlePong(conn, parts[1:])
	case "SDOWN":
		gp.handleSdown(conn, parts[1:])
	case "MASTERS":
		gp.handleMasters(conn)
	}
}

// handleHello 处理HELLO消息
func (gp *GossipProtocol) handleHello(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		return
	}

	runID := parts[0]
	epoch, _ := strconv.ParseInt(parts[2], 10, 64)

	peerAddr := conn.RemoteAddr().String()

	// 添加或更新对等体
	gp.addOrUpdatePeer(peerAddr, runID)

	logger.Logger.Debug().
		Str("peer", peerAddr).
		Str("run_id", runID).
		Int64("epoch", epoch).
		Msg("收到HELLO消息")

	// 发送PONG响应
	response := gp.formatPong()
	gp.sendMessage(conn, response)
}

// handlePing 处理PING消息
func (gp *GossipProtocol) handlePing(conn net.Conn) {
	peerAddr := conn.RemoteAddr().String()
	gp.touchPeer(peerAddr)

	response := gp.formatPong()
	gp.sendMessage(conn, response)
}

// handlePong 处理PONG消息
func (gp *GossipProtocol) handlePong(conn net.Conn, parts []string) {
	if len(parts) < 1 {
		return
	}

	runID := parts[0]
	peerAddr := conn.RemoteAddr().String()

	gp.addOrUpdatePeer(peerAddr, runID)

	logger.Logger.Debug().
		Str("peer", peerAddr).
		Str("run_id", runID).
		Msg("收到PONG消息")
}

// handleSDOWN 处理SDOWN消息
func (gp *GossipProtocol) handleSdown(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		return
	}

	masterName := parts[0]
	reportedSdownCount, _ := strconv.Atoi(parts[1])

	// 更新主节点的sdown计数
	master := gp.sentinel.GetMaster(masterName)
	if master != nil {
		// 使用报告的sdown计数和当前计数中的较大值
		currentCount := master.GetSdownCount()
		if reportedSdownCount > currentCount {
			master.mu.Lock()
			master.sdownCount = reportedSdownCount
			master.mu.Unlock()
		}

		logger.Logger.Info().
			Str("master_name", masterName).
			Int("reported_sdown_count", reportedSdownCount).
			Int("current_sdown_count", master.GetSdownCount()).
			Int("quorum", master.GetQuorum()).
			Msg("收到SDOWN消息")

		// 检查是否达到客观下线
		if master.IsODown() {
			fm := NewFailoverManager(gp.sentinel)
			go func() {
				if err := fm.AutoFailover(masterName); err != nil {
					logger.Logger.Error().
						Str("master_name", masterName).
						Err(err).
						Msg("自动故障转移失败")
				}
			}()
		}
	}
}

// handleMasters 处理MASTERS消息
func (gp *GossipProtocol) handleMasters(conn net.Conn) {
	masters := gp.sentinel.GetAllMasters()
	data, _ := json.Marshal(masters)

	response := "MASTERS " + string(data) + "\n"
	gp.sendMessage(conn, response)
}

// addOrUpdatePeer 添加或更新对等体
func (gp *GossipProtocol) addOrUpdatePeer(addr, runID string) {
	if _, exists := gp.peers[addr]; !exists && len(gp.peers) >= gp.config.MaxPeers {
		return
	}

	gp.peers[addr] = &GossipPeer{
		Addr:     addr,
		RunID:    runID,
		LastSeen: time.Now(),
	}
}

// touchPeer 更新对等体的最后活跃时间
func (gp *GossipProtocol) touchPeer(addr string) {
	if peer, exists := gp.peers[addr]; exists {
		peer.LastSeen = time.Now()
	}
}

// removePeer 移除对等体
func (gp *GossipProtocol) removePeer(addr string) {
	delete(gp.peers, addr)
}

// managePeers 管理对等体连接
func (gp *GossipProtocol) managePeers() {
	ticker := time.NewTicker(gp.config.HelloInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gp.stopCh:
			return
		case <-ticker.C:
			gp.sendHellos()
		}
	}
}

// sendHellos 发送HELLO消息到所有已知对等体
func (gp *GossipProtocol) sendHellos() {
	for addr, peer := range gp.peers {
		if time.Since(peer.LastSeen) > gp.config.PeerTimeout {
			gp.removePeer(addr)
			continue
		}

		if peer.HelloSent {
			continue
		}

		if err := gp.sendHello(addr); err != nil {
			logger.Logger.Warn().Str("peer", addr).Err(err).Msg("发送HELLO消息失败")
		} else {
			peer.HelloSent = true
		}
	}
}

// sendHello 发送HELLO消息到指定地址
func (gp *GossipProtocol) sendHello(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	response := gp.formatHello()
	return gp.sendMessage(conn, response)
}

// formatHello 格式化HELLO消息
func (gp *GossipProtocol) formatHello() string {
	port := gp.GetPort()
	epoch := gp.sentinel.GetConfigEpoch()
	return "HELLO " + gp.config.RunID + " " + strconv.Itoa(port) + " " + strconv.FormatInt(epoch, 10) + "\n"
}

// formatPong 格式化PONG消息
func (gp *GossipProtocol) formatPong() string {
	return "PONG " + gp.config.RunID + "\n"
}

// sendMessage 发送消息
func (gp *GossipProtocol) sendMessage(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message))
	return err
}

// BroadcastSdown 广播SDOWN消息到所有对等体
func (gp *GossipProtocol) BroadcastSdown(masterName string, sdownCount int) {
	message := "SDOWN " + masterName + " " + strconv.Itoa(sdownCount) + "\n"

	for addr := range gp.peers {
		go func(peerAddr string) {
			conn, err := net.DialTimeout("tcp", peerAddr, 5*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			gp.sendMessage(conn, message)
		}(addr)
	}
}

// GetPeersCount 获取对等体数量
func (gp *GossipProtocol) GetPeersCount() int {
	return len(gp.peers)
}
