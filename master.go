package masterslave

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func Master(uri, key string) (chan []byte, error) {

	connection, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	if err := channel.ExchangeDeclare(
		key,          // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	/*if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	ack, nack := channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

	defer confirmOne(ack, nack)*/

	toSlaves := make(chan []byte)

	go func() {
		for {
			message := <-toSlaves

			if err = channel.Publish(
				key,   // publish to an exchange
				key, // routing to 0 or more queues
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					Headers:         amqp.Table{},
					ContentType:     "text/plain",
					ContentEncoding: "",
					Body:            message,
					DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
					Priority:        0,              // 0-9
					// a bunch of application/implementation-specific fields
				},
			); err != nil {
				log.Printf("Error Exchange Publish: %s", err)
			}
		}
	}()

	return toSlaves, nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(ack, nack chan uint64) {
	log.Printf("waiting for confirmation of one publishing")

	select {
	case tag := <-ack:
		log.Printf("confirmed delivery with delivery tag: %d", tag)
	case tag := <-nack:
		log.Printf("failed delivery of delivery tag: %d", tag)
	}
}
