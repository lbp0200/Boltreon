package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

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
			fmt.Fprintln(conn, proto.NewError("ERR connection closed"))
			return
		}
		args := req.Args
		if len(args) == 0 {
			resp := proto.NewError("ERR no command")
			if err := proto.WriteRESP(conn, resp); err != nil {
				return
			}
			continue
		}
		cmd := strings.ToUpper(string(args[0]))
		switch cmd {
		case "PING":
			resp := proto.OK
			if err := proto.WriteRESP(conn, resp); err != nil {
				return
			}
		case "SET":
			if len(args) < 3 {
				resp := proto.NewError("ERR wrong number of arguments for 'SET' command")
				if err := proto.WriteRESP(conn, resp); err != nil {
					return
				}
			}
			key, value := args[1], args[2]
			if err := h.Db.Set(string(key), string(value)); err != nil {
				resp := proto.NewError(fmt.Sprintf("ERR %v", err))
				if err := proto.WriteRESP(conn, resp); err != nil {
					return
				}
			}
			resp := proto.OK
			if err := proto.WriteRESP(conn, resp); err != nil {
				return
			}
		}
	}
}
