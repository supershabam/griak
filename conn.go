package griak

import (
	"io"
	"net"
)

type Conn struct {
	conn net.Conn
}

func NewConn(addr string) (*Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

func (c Conn) Read() (code byte, data []byte, err error) {
	// Read message with header: <length:32> <msg_code:8> <pbmsg>
	lenbuf := make([]byte, 4)
	codebuf := make([]byte, 1)
	_, err = io.ReadFull(c.conn, lenbuf)
	if err != nil {
		return
	}
	_, err = io.ReadFull(c.conn, codebuf)
	if err != nil {
		return
	}
	code = codebuf[0]
	length := int(lenbuf[0])<<24 +
		int(lenbuf[1])<<16 +
		int(lenbuf[2])<<8 +
		int(lenbuf[3]) - 1
	data = make([]byte, length)
	_, err = io.ReadFull(c.conn, data)
	return
}

func (c Conn) Write(code byte, data []byte) error {
	// Build message with header: <length:32> <msg_code:8> <pbmsg>
	var length int32 = int32(len(data) + 1)
	lenbuf := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}
	buf := []byte{}
	buf = append(buf, lenbuf...)
	buf = append(buf, code)
	buf = append(buf, data...)
	_, err := c.conn.Write(buf)
	return err
}
