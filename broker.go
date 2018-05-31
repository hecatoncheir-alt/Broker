package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	nsq "github.com/bitly/go-nsq"
)

// EventData is a struct of event for send or receive from broker
type EventData struct {
	Message     string
	Data        string
	APIVersion  string
	ServiceName string
}

// New constructor for Broker
func New(apiVersion, serviceName string) *Broker {
	broker := Broker{}
	broker.APIVersion = apiVersion
	broker.ServiceName = serviceName
	broker.configuration = nsq.NewConfig()
	// broker.сonfiguration.MaxInFlight = 6
	// broker.сonfiguration.MsgTimeout = time.Duration(time.Second * 6)
	broker.Log = log.New(os.Stdout, "Broker: ", 3)
	return &broker
}

// Broker is a object of message stream
type Broker struct {
	IP            string
	APIVersion    string
	ServiceName   string
	Port          int
	configuration *nsq.Config
	Producer      *nsq.Producer
	Log           *log.Logger
}

// connectToMessageBroker method for connect to message broker
func (broker *Broker) connectToMessageBroker(host string, port int) (*nsq.Producer, error) {
	if host != "" && string(port) != "" {
		broker.IP = host
		broker.Port = port
	}

	hostAddr := fmt.Sprintf("%v:%v", broker.IP, strconv.Itoa(broker.Port))
	producer, err := nsq.NewProducer(hostAddr, broker.configuration)

	if err != nil {
		broker.Log.Print("Could not connect to message broker")
	}

	broker.Log.Printf("Connected to message broker")

	return producer, err

}

// Connect to message broker for publish events
func (broker *Broker) Connect(host string, port int) error {
	producer, err := broker.connectToMessageBroker(host, port)
	broker.Producer = producer
	return err
}

// WriteToTopic method for publish message to topic
func (broker *Broker) WriteToTopic(topic string, message EventData) error {
	message.APIVersion = broker.APIVersion
	message.ServiceName = broker.ServiceName
	event, err := json.Marshal(message)
	if err != nil {
		return err
	}

	go broker.Producer.Publish(topic, event)
	return nil
}

// ListenTopic get events in channel of topic
func (broker *Broker) ListenTopic(topic string, channel string) (<-chan []byte, error) {
	consumer, err := nsq.NewConsumer(topic, channel, broker.configuration)
	if err != nil {
		return nil, err
	}

	events := make(chan []byte, 6)

	handler := nsq.HandlerFunc(func(message *nsq.Message) error {
		events <- message.Body
		return nil
	})

	consumer.AddConcurrentHandlers(handler, 6)

	hostAddr := fmt.Sprintf("%v:%v", broker.IP, strconv.Itoa(broker.Port))
	go consumer.ConnectToNSQD(hostAddr)

	return events, nil
}
