package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/Boltreon/internal/proto"
	"github.com/lbp0200/Boltreon/internal/store"
)

type Handler struct {
	Db *store.BoltreonStore
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
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		req, err := proto.ReadRESP(reader)
		if err != nil {
			fmt.Fprintln(conn, proto.Error("ERR connection closed"))
			return
		}
		resp := h.executeCommand(req.Args)
		if err := proto.WriteRESP(conn, resp); err != nil {
			return
		}
	}
}

func (h *Handler) executeCommand(args [][]byte) proto.RESP {
	if len(args) == 0 {
		return proto.Error("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "PING":
		return proto.NewSimpleString("OK")
	case "SET":
		if len(args) < 3 {
			return proto.Error("ERR wrong number of arguments for 'SET' command")
		}
		key, value := args[1], args[2]
		ttl := time.Duration(0)
		i := 3
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			if opt == "EX" && i+1 < len(args) {
				sec, _ := strconv.Atoi(string(args[i+1])) // 忽略 err，防崩溃
				ttl = time.Duration(sec) * time.Second
				i += 2
			} else if opt == "PX" && i+1 < len(args) {
				ms, _ := strconv.Atoi(string(args[i+1]))
				ttl = time.Duration(ms) * time.Millisecond
				i += 2
			} else {
				i++ // 跳过未知
			}
		}
		if err := h.Db.SetWithTTL(key, value, ttl); err != nil {
			return proto.Error(fmt.Sprintf("ERR %v", err))
		}
		return proto.SimpleString("OK")
	case "GET":
		if len(args) != 2 {
			return proto.Error("ERR wrong number of arguments for 'GET' command")
		}
		val, err := h.Db.Get(args[1])
		if err == badger.ErrKeyNotFound {
			return proto.BulkString(nil)
		}
		if err != nil {
			return proto.Error(fmt.Sprintf("ERR %v", err))
		}
		return proto.BulkString(val)
	case "DEL":
		if len(args) < 2 {
			return proto.Error("ERR wrong number of arguments for 'DEL' command")
		}
		cnt := 0
		for i := 1; i < len(args); i++ {
			if err := h.Db.Del(string(args[i])); err == nil {
				cnt++
			}
		}
		return proto.Integer(int64(cnt))
	case "EXISTS":
		if len(args) < 2 {
			return proto.Error("ERR wrong number of arguments for 'EXISTS' command")
		}
		cnt := 0
		for i := 1; i < len(args); i++ {
			if _, err := h.Db.Get(args[i]); err == nil {
				cnt++
			}
		}
		return proto.Integer(int64(cnt))
	default:
		return proto.Error(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}
