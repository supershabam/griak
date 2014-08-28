package main

import (
	"log"

	"code.google.com/p/goprotobuf/proto"

	"github.com/supershabam/griak"
	"github.com/supershabam/griak/pb/riak"
)

const RiakAddr = "104.131.63.89:8087"

func main() {
	req := &riak.RpbGetBucketReq{
		Bucket: []byte("sldkfjsltats.droplet.1234.cpu"),
		Type:   []byte("metricgroup"),
	}
	conn, err := griak.NewConn(RiakAddr)
	if err != nil {
		log.Fatal(err)
	}
	data, err := proto.Marshal(req)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Write(19, data)
	if err != nil {
		log.Fatal(err)
	}

	_, data, err = conn.Read()
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
