// 简化 RESP，只支持 basics（Array/Bulk/Simple/Error/Integer）
package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RESP interface {
	String() string
}

type Array struct {
	Args [][]byte
}

func (a *Array) String() string {
	return "*" + strconv.Itoa(len(a.Args)) + "\r\n" + joinBulkStrings(a.Args)
}

type BulkString []byte

func (b *BulkString) String() string {
	if b == nil {
		return "$-1\r\n"
	}
	if *b == nil {
		return "$-1\r\n"
	}
	return "$" + strconv.Itoa(len(*b)) + "\r\n" + string(*b) + "\r\n"
}

type SimpleString string

func (s SimpleString) String() string { return "+" + string(s) + "\r\n" }

type Error string

func (e Error) String() string { return "-" + string(e) + "\r\n" }

type Integer int64

func (i Integer) String() string { return ":" + strconv.FormatInt(int64(i), 10) + "\r\n" }

func ReadRESP(r *bufio.Reader) (*Array, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	switch line[0] {
	case '*': // Array
		if len(line) == 1 {
			return nil, fmt.Errorf("invalid array prefix")
		}
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid array length: %s", line[1:])
		}
		args := make([][]byte, n)
		for i := 0; i < n; i++ {
			// 先读 $xxx\r\n
			lenLine, err := readLine(r)
			if err != nil {
				return nil, err
			}
			if len(lenLine) == 0 || lenLine[0] != '$' {
				return nil, fmt.Errorf("expected $, got %q", lenLine)
			}
			bulkLen, err := strconv.Atoi(string(lenLine[1:]))
			if err != nil || bulkLen < -1 {
				return nil, err
			}
			if bulkLen == -1 {
				args[i] = nil
				continue
			}

			// 读真实数据 + \r\n
			data := make([]byte, bulkLen+2) // +2 for \r\n
			_, err = io.ReadFull(r, data)
			if err != nil {
				return nil, err
			}
			args[i] = data[:bulkLen] // 去掉 \r\n
		}
		return &Array{Args: args}, nil
	}
	return nil, fmt.Errorf("unsupported RESP type: %c", line[0])
}

func WriteRESP(w io.Writer, resp RESP) error {
	_, err := fmt.Fprint(w, resp.String())
	return err
}

// helpers
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// 去掉 \r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func readBulkString(r *bufio.Reader) ([]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if line[0] != '$' {
		return nil, fmt.Errorf("expected $")
	}
	n, _ := strconv.Atoi(string(line[1:]))
	if n == -1 {
		_, _ = readLine(r) // 跳 \r\n
		return nil, nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, n))
	if _, err := io.CopyN(buf, r, int64(n)); err != nil {
		return nil, err
	}
	_, _ = readLine(r) // 跳 \r\n
	return buf.Bytes(), nil
}

func joinBulkStrings(args [][]byte) string {
	var b strings.Builder
	for _, arg := range args {
		if arg == nil {
			b.WriteString("$-1\r\n")
		} else {
			b.WriteString("$")
			b.WriteString(strconv.Itoa(len(arg)))
			b.WriteString("\r\n")
			b.Write(arg)
			b.WriteString("\r\n")
		}
	}
	return b.String()
}

// 工厂
func NewSimpleString(s string) RESP { r := SimpleString(s); return &r }
func NewBulkString(b []byte) RESP {
	if b == nil {
		var r *BulkString
		return r
	}
	r := BulkString(b)
	return &r
}
func NewError(e string) RESP  { r := Error(e); return &r }
func NewInteger(i int64) RESP { r := Integer(i); return &r }

var (
	OK = NewSimpleString("OK")
)
