package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// 简单的调试客户端，用于测试服务器响应
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run debug_client.go <command> [args...]")
		fmt.Println("Example: go run debug_client.go CONFIG GET *")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Printf("Error connecting: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// 构建命令
	args := make([][]byte, len(os.Args)-1)
	for i, arg := range os.Args[1:] {
		args[i] = []byte(arg)
	}
	req := &proto.Array{Args: args}

	// 发送命令
	fmt.Printf("Sending: %s\n", req.String())
	if err := proto.WriteRESP(conn, req); err != nil {
		fmt.Printf("Error writing: %v\n", err)
		os.Exit(1)
	}

	// 读取响应
	reader := bufio.NewReader(conn)
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		fmt.Printf("Error reading: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Response: %s\n", resp.String())
}
