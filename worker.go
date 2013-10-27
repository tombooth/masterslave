package masterslave

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Job struct {
	Payload []byte
	delivery amqp.Delivery
}

func (job *Job) Done() {
	job.delivery.Ack(false)
}

func (job *Job) Failed() {
	job.delivery.Nack(false, true)
}

type Slave struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
	Jobs    chan *Job
}

func NewSlave(uri, key string) (*Slave, error) {
	slave := &Slave{
		conn:    nil,
		channel: nil,
		tag:     key,
		done:    make(chan error),
		Jobs:    make(chan *Job),
	}

	var err error

	log.Printf("dialing %q", uri)
	slave.conn, err = amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-slave.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	slave.channel, err = slave.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", key)
	if err = slave.channel.ExchangeDeclare(
		key,     // name of the exchange
		"direct", // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", key)
	queue, err := slave.channel.QueueDeclare(
		key, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = slave.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		key,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", slave.tag)
	deliveries, err := slave.channel.Consume(
		queue.Name, // name
		slave.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries, slave.Jobs, slave.done)

	return slave, nil
}

func (slave *Slave) Shutdown() error {
	// will close() the deliveries channel
	if err := slave.channel.Cancel(slave.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := slave.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-slave.done
}

func handle(deliveries <-chan amqp.Delivery, jobs chan *Job, done chan error) {
	for d := range deliveries {
		job := &Job {
			Payload: d.Body,
			delivery: d,
		}

		jobs <- job
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}
