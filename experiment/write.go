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

	m, err := conn.ReadMap("metricgroup", "stats.droplet.1234.cpu", "1409230800000")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("m: %+v", m)
}
