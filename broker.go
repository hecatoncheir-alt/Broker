package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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
	logger := log.New(os.Stdout, "Broker: ", 3)
	broker := Broker{
		APIVersion:    apiVersion,
		ServiceName:   serviceName,
		Log:           logger,
		InputChannel:  make(chan EventData),
		OutputChannel: make(chan EventData)}

	go func() {
		for outputEvent := range broker.OutputChannel {
			err := broker.write(outputEvent)
			if err != nil {
				broker.Log.Printf("Error write event: %v to event bus", outputEvent)
			}
		}
	}()

	return &broker
}

// Broker is a object of message stream
type Broker struct {
	Port        int
	IP          string
	APIVersion  string
	ServiceName string

	InputChannel  chan EventData
	OutputChannel chan EventData

	Connection *net.TCPConn
	Log        *log.Logger
}

// Connect to message broker for publish events
func (broker *Broker) Connect(host string, port int) error {

	address := fmt.Sprintf("%v:%v", host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		broker.Log.Print("Could not connect to message broker")
		return err
	}

	connection, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		broker.Log.Print("Could not connect to message broker")
		return err
	}

	broker.Connection = connection

	go broker.SubscribeOnEvents(connection)

	return nil
}

func (broker *Broker) SubscribeOnEvents(connection io.ReadCloser) {
	request := make([]byte, 2560)

	defer connection.Close()

	for {
		lengthOfBytes, err := connection.Read(request)

		if err != nil || lengthOfBytes == 0 {
			broker.InputChannel <- EventData{Message: "Connection closed"}
			break // connection already closed by client
		}

		event := EventData{}
		err = json.Unmarshal(request[:lengthOfBytes], &event)
		if err != nil {
			broker.Log.Printf("Error by unmarshal event: %v. Error: %v", string(request[:lengthOfBytes]), err)
			continue
		}

		broker.InputChannel <- event
	}
}

func (broker *Broker) write(data EventData) error {
	encodedData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = broker.Connection.Write(encodedData)
	if err != nil {
		return err
	}

	return nil
}

// Write method for publish message to EventBus
func (broker *Broker) Write(data EventData) {

	data.ServiceName = broker.ServiceName
	data.APIVersion = broker.APIVersion

	broker.OutputChannel <- data
}
