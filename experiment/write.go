package main

import (
	"io"
	"log"
	"net"

	"code.google.com/p/goprotobuf/proto"

	"github.com/supershabam/griak/pb/riak"
)

const RiakAddr = "104.131.63.89:8087"

func main() {
	req := &riak.RpbGetBucketReq{
		Bucket: []byte("stats.droplet.1234.cpu"),
		Type:   []byte("metricgroup"),
	}
	conn, err := net.Dial("tcp", RiakAddr)
	if err != nil {
		log.Fatal(err)
	}
	data, err := proto.Marshal(req)
	if err != nil {
		log.Fatal(err)
	}

	err = WriteRiak(conn, 19, data)
	if err != nil {
		log.Fatal(err)
	}
	_, data, err = ReadRiak(conn)
	if err != nil {
		log.Fatal(err)
	}
	resp := &riak.RpbGetBucketResp{}
	err = proto.Unmarshal(data, resp)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", resp)

}

func WriteRiak(w io.Writer, code byte, data []byte) error {
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
	_, err := w.Write(buf)
	return err
}

func ReadRiak(r io.Reader) (code byte, data []byte, err error) {
	// Read message with header: <length:32> <msg_code:8> <pbmsg>
	lenbuf := make([]byte, 4)
	codebuf := make([]byte, 1)
	_, err = io.ReadFull(r, lenbuf)
	if err != nil {
		return
	}
	_, err = io.ReadFull(r, codebuf)
	if err != nil {
		return
	}
	code = codebuf[0]
	length := int(lenbuf[0])<<24 +
		int(lenbuf[1])<<16 +
		int(lenbuf[2])<<8 +
		int(lenbuf[3]) - 1
	data = make([]byte, length)
	_, err = io.ReadFull(r, data)
	return
}
