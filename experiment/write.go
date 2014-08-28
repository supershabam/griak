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
	log.Print("HI")
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

	// Build message with header: <length:32> <msg_code:8> <pbmsg>
	// i := int32(len(pbmsg) + 1)
	// msgbuf := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), code}
	// msgbuf = append(msgbuf, pbmsg...)

	var code byte
	code = 19
	i := int32(len(data) + 1)
	msgbuf := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), code}
	msgbuf = append(msgbuf, data...)

	n, err := conn.Write(msgbuf)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %d bytes", n)
	code, data, err = ReadRiak(conn)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("code: %d", code)
	log.Printf("data: %s", data)
}

func ReadRiak(r io.Reader) (code byte, data []byte, err error) {
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
	log.Printf("reading %d", length)
	data = make([]byte, length)
	_, err = io.ReadFull(r, data)
	return
}
