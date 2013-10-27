package main

import (
	"flag"
	"fmt"
	"github.com/tombooth/masterslave"
	"os"
	"time"
)

var (
	uri = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	key = flag.String("key", "masterslave", "Key to be used for this masterslave set")
)

func main() {

	flag.Parse()

	toSlaves, err := masterslave.Master(*uri, *key)
	if err != nil {
		fmt.Printf("Failed to setup master: %s", err)
		os.Exit(1)
	}

	for {
		toSlaves <- []byte("Hello!")
		time.Sleep(time.Second * 1)
	}

}


