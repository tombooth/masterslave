package main

import (
	"flag"
	"fmt"
	"github.com/tombooth/masterslave"
	"os"
)

var (
	uri = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	key = flag.String("key", "masterslave", "Key to be used for this masterslave set")
)

func main() {
	slave, err := masterslave.NewSlave(*uri, *key)
	if err != nil {
		fmt.Printf("Failed to setup Slave: %s", err)
		os.Exit(1)
	}

	for {
		job := <-slave.Jobs
		fmt.Println("Received job: %q", string(job.Payload))
		job.Done()
	}
}


