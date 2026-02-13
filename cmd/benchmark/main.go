package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// BoltDBBenchmark runs redis-benchmark against BoltDB server
func main() {
	dbPath := flag.String("dir", "/tmp/bolt_bench", "badger dir")
	logLevel := flag.String("log-level", "ERROR", "log level")
	clients := flag.Int("c", 50, "number of concurrent clients")
	requests := flag.Int("n", 100000, "total number of requests")
	dataSize := flag.Int("d", 100, "data size in bytes")
	flag.Parse()

	// 清理旧数据
	_ = os.RemoveAll(*dbPath)
	_ = os.MkdirAll(*dbPath, 0755)

	fmt.Println("==============================================")
	fmt.Println("BoltDB Benchmark Results")
	fmt.Println("==============================================")

	// 启动 BoltDB 服务器 (使用固定端口)
	fmt.Println("Starting BoltDB server...")
	boltCmd := exec.Command("./build/boltDB",
		"-addr", ":6388",
		"-dir", *dbPath,
		"-log-level", *logLevel,
	)

	var boltStdout, boltStderr bytes.Buffer
	boltCmd.Stdout = &boltStdout
	boltCmd.Stderr = &boltStderr

	if err := boltCmd.Start(); err != nil {
		fmt.Printf("Failed to start BoltDB: %v\n", err)
		fmt.Println("Make sure to build boltDB first: go build -o ./build/boltDB ./cmd/boltDB/main.go")
		os.Exit(1)
	}
	defer func() {
		_ = boltCmd.Process.Kill()
		_, _ = boltCmd.Process.Wait()
	}()

	// 等待服务器启动
	time.Sleep(2 * time.Second)

	// 验证服务器已启动
	port := "6388"
	cmd := exec.Command("redis-cli", "-p", port, "PING")
	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to connect to BoltDB on port %s: %v\n", port, err)
		fmt.Printf("BoltDB stdout: %s\n", boltStdout.String())
		fmt.Printf("BoltDB stderr: %s\n", boltStderr.String())
		os.Exit(1)
	}

	fmt.Printf("Server: BoltDB 127.0.0.1:%s\n", port)
	fmt.Printf("Clients: %d | Data Size: %d bytes | Requests: %d\n", *clients, *dataSize, *requests)
	fmt.Println("==============================================")
	fmt.Println()

	// 测试单个命令
	testCommands := []string{"PING", "SET", "GET"}
	var totalRequests int64
	var totalTime time.Duration

	for _, testCmd := range testCommands {
		fmt.Printf("Testing %s...\n", testCmd)

		benchmarkCmd := exec.Command("redis-benchmark",
			"-h", "127.0.0.1",
			"-p", port,
			"-t", testCmd,
			"-c", strconv.Itoa(*clients),
			"-d", strconv.Itoa(*dataSize),
			"-n", strconv.Itoa(*requests),
		)

		var benchStdout bytes.Buffer
		benchmarkCmd.Stdout = &benchStdout
		benchmarkCmd.Stderr = nil

		cmdStart := time.Now()
		if err := benchmarkCmd.Run(); err != nil {
			fmt.Printf("  %s test failed: %v\n", testCmd, err)
			continue
		}
		cmdTime := time.Since(cmdStart)
		totalTime += cmdTime

		output := benchStdout.String()
		lines := strings.Split(output, "\n")

		// 提取结果
		for _, line := range lines {
			if strings.Contains(line, testCmd+"_") || strings.HasPrefix(line, "====== "+testCmd) {
				// 提取 rps
				for _, l := range lines {
					if strings.Contains(l, "requests per second") {
						// 解析 throughput
						parts := strings.Fields(l)
						for i, part := range parts {
							if part == "requests" && i > 0 {
								if rps, err := strconv.ParseFloat(parts[i-1], 64); err == nil {
									fmt.Printf("  %s: %.2f requests/sec\n", testCmd, rps)
								}
								break
							}
						}
						break
					}
				}
				fmt.Println("  " + line)
				for _, l := range lines {
					if strings.Contains(l, "completed") {
						fmt.Println("  " + l)
						// 统计请求数
						parts := strings.Fields(l)
						for _, part := range parts {
							if n, err := strconv.ParseInt(part, 10, 64); err == nil {
								totalRequests += n
							}
						}
						break
					}
				}
				break
			}
		}
		fmt.Println()
	}

	// 输出汇总
	fmt.Println("==============================================")
	fmt.Println("Benchmark Summary:")
	fmt.Println("----------------------------------------------")

	if totalRequests > 0 && totalTime > 0 {
		opsPerSec := float64(totalRequests) / totalTime.Seconds()
		fmt.Printf("Total requests: %d\n", totalRequests)
		fmt.Printf("Total time: %v\n", totalTime)
		fmt.Printf("Overall throughput: %.2f ops/sec\n", opsPerSec)
	}

	fmt.Println("\n==============================================")
	fmt.Println("Benchmark completed successfully!")
	fmt.Println("==============================================")
}
