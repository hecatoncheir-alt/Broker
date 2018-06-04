package broker

import (
	"fmt"
	"log"
	"os"

	"github.com/nats-io/go-nats"
)

// EventData is a struct of event for send or receive from broker
type EventData struct {
	Message     string
	Data        string
	APIVersion  string
	ServiceName string
	ClientID    string
}

// New constructor for Broker
func New(apiVersion, serviceName string) *Broker {
	broker := Broker{}
	broker.APIVersion = apiVersion
	broker.ServiceName = serviceName
	broker.Log = log.New(os.Stdout, "Broker: ", 3)
	return &broker
}

// Broker is a object of message stream
type Broker struct {
	IP          string
	APIVersion  string
	ServiceName string
	Connection  *nats.EncodedConn
	Port        int
	Log         *log.Logger
}

// connectToMessageBroker method for connect to message broker
func (broker *Broker) connectToMessageBroker(host string, port int) *nats.EncodedConn {

	if host != "" && string(port) != "" {
		broker.IP = host
		broker.Port = port
	}

	natsURL := fmt.Sprintf("nats://%v:%v", host, port)
	connection, err := nats.Connect(natsURL)
	if err != nil {
		broker.Log.Print("Could not connect to message broker")
		log.Fatalf(err.Error())
	}

	encodedConnection, err := nats.NewEncodedConn(connection, nats.JSON_ENCODER)
	if err != nil {
		broker.Log.Print("Could not encode connection of message broker")
	}

	return encodedConnection
}

// Connect to message broker for publish events
func (broker *Broker) Connect(host string, port int) error {
	connection := broker.connectToMessageBroker(host, port)
	broker.Connection = connection
	return nil
}

// WriteToTopic method for publish message to topic
func (broker *Broker) WriteToTopic(topic string, message EventData) error {
	message.APIVersion = broker.APIVersion
	message.ServiceName = broker.ServiceName

	err := broker.Connection.Publish(topic, message)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

// ListenTopic get events in channel of topic
func (broker *Broker) ListenTopic(topic string, channel string) (<-chan *EventData, error) {

	inputChannel := make(chan *EventData)

	broker.Connection.Subscribe(topic, func(event *EventData) {
		if event.APIVersion == channel {
			inputChannel <- event
		}
	})

	return inputChannel, nil
}
