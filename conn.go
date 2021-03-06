package griak

import (
	"fmt"
	"io"
	"net"

	"code.google.com/p/goprotobuf/proto"

	"github.com/supershabam/griak/pb/riak"
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

func (c Conn) ReadMap(types, buckets, datatypes string) (map[string]string, error) {
	req := &riak.DtFetchReq{
		Type:   []byte(types),
		Bucket: []byte(buckets),
		Key:    []byte(datatypes),
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	err = c.Write(80, data)
	if err != nil {
		return nil, err
	}
	code, data, err := c.Read()
	if err != nil {
		return nil, err
	}
	if code != 81 {
		return nil, fmt.Errorf("expected code 81 not: %d", code)
	}
	resp := &riak.DtFetchResp{}
	err = proto.Unmarshal(data, resp)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, mapEntry := range resp.GetValue().GetMapValue() {
		key := string(mapEntry.GetField().GetName())
		value := string(mapEntry.GetRegisterValue())
		result[key] = value
	}
	return result, nil
}

func (c Conn) WriteMap(types, buckets, datatypes string, m map[string]string) error {
	updates := []*riak.MapUpdate{}
	for k, v := range m {
		t := new(riak.MapField_MapFieldType)
		*t = riak.MapField_REGISTER
		update := &riak.MapUpdate{
			Field: &riak.MapField{
				Name: []byte(k),
				Type: t,
			},
			RegisterOp: []byte(v),
		}
		updates = append(updates, update)
	}
	req := &riak.DtUpdateReq{
		Type:   []byte(types),
		Bucket: []byte(buckets),
		Key:    []byte(datatypes),
		Op: &riak.DtOp{
			MapOp: &riak.MapOp{
				Updates: updates,
			},
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	err = c.Write(82, data)
	if err != nil {
		return err
	}
	code, _, err := c.Read()
	if err != nil {
		return err
	}
	if code != 83 {
		return fmt.Errorf("expected code 83 not: %d", code)
	}
	return nil
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
