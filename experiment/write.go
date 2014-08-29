package main

import (
	"log"

	"github.com/supershabam/griak"
)

const RiakAddr = "104.131.63.89:8087"

func main() {
	// establish conn
	conn, err := griak.NewConn(RiakAddr)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.WriteMap("metricgroup", "l.jl.bukit", "key", map[string]string{
		"omg":         "lolerskates",
		"another key": "rewrite",
	})
	if err != nil {
		log.Fatal(err)
	}

	m, err := conn.ReadMap("metricgroup", "bukit", "key")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("m: %+v", m)
}
