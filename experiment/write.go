package main

import (
	"log"

	"code.google.com/p/goprotobuf/proto"

	"github.com/supershabam/griak"
	"github.com/supershabam/griak/pb/riak"
)

const RiakAddr = "104.131.63.89:8087"

func main() {
	// establish conn
	conn, err := griak.NewConn(RiakAddr)
	if err != nil {
		log.Fatal(err)
	}

	// make request payload
	req := &riak.DtFetchReq{
		Bucket: []byte("stats.droplet.1234.cpu"),
		Key:    []byte("1409230800000"),
		Type:   []byte("metricgroup"),
	}
	data, err := proto.Marshal(req)
	if err != nil {
		log.Fatal(err)
	}

	// write request
	var code byte = 80
	err = conn.Write(code, data)
	if err != nil {
		log.Fatal(err)
	}

	// read request
	code, data, err = conn.Read()
	if err != nil {
		log.Fatal(err)
	}
	if code != 81 {
		log.Fatalf("expected message code 10 not %d", code)
	}
	resp := &riak.DtFetchResp{}
	err = proto.Unmarshal(data, resp)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", resp)

}
