package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/Boltreon/internal/proto"
	"github.com/lbp0200/Boltreon/internal/store"
)

type Handler struct {
	db *store.DB // Badger 封装
}

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
		req, err := proto.ReadRESP(reader) // 解析 RESP
		if err != nil {
			return
		}
		resp := h.executeCommand(req.Args) // 执行命令
		proto.WriteRESP(conn, resp)        // 写回 RESP
	}
}

func (h *Handler) executeCommand(args [][]byte) *proto.RESP {
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "SET":
		if len(args) < 3 {
			return proto.Error("ERR wrong number of arguments")
		}
		key, value := args[1], args[2]
		if err := h.db.Set(key, value); err != nil {
			return proto.Error(fmt.Sprintf("ERR %v", err))
		}
		return proto.SimpleString("OK")
	case "GET":
		if len(args) != 2 {
			return proto.Error("ERR wrong number of arguments")
		}
		key := args[1]
		val, err := h.db.Get(key)
		if err == badger.ErrKeyNotFound {
			return proto.BulkString(nil)
		}
		if err != nil {
			return proto.Error(fmt.Sprintf("ERR %v", err))
		}
		return proto.BulkString(val)
	// TODO: 加 DEL, EXISTS 等
	default:
		return proto.Error(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}
