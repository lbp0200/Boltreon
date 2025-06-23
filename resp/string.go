package resp

import (
	"PumbaaDB/store"
	"fmt"
	"net"
	"strconv"
	"time"
)

func HandleSet(conn net.Conn, args [][]byte, store *store.BadgerStore) {
	if len(args) < 2 {
		conn.Write(Encode(fmt.Errorf("ERR wrong number of arguments")))
		return
	}
	var ttl time.Duration
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			switch string(args[i]) {
			case "EX":
				if i+1 >= len(args) {
					conn.Write(Encode(fmt.Errorf("ERR syntax error")))
					return
				}
				sec, _ := strconv.Atoi(string(args[i+1]))
				ttl = time.Duration(sec) * time.Second
				i++
			}
		}
	}
	var err error
	if ttl > 0 {
		err = store.SetWithTTL(args[0], args[1], ttl)
	} else {
		err = store.Set(args[0], args[1])
	}
	if err != nil {
		conn.Write(Encode(err))
	} else {
		conn.Write(Encode("OK"))
	}
}
