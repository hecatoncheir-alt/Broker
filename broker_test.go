package broker

import (
	"log"
	"testing"

	"github.com/hecatoncheir/Configuration"
)

func TestBrokerCanSendMessage(test *testing.T) {
	bro := New("1.0.0", "Test service name")

	config := configuration.New()

	err := bro.Connect(config.Development.Broker.Host, config.Development.Broker.Port)

	if err != nil {
		log.Println(err)
	}

	defer bro.Connection.Close()

	// item := map[string]string{"Name": "test item"}

	item := EventData{Message: "Name", Data: "test item"}

	items, err := bro.ListenTopic(config.Development.InitialTopic, config.APIVersion)
	if err != nil {
		test.Error(err)
	}

	err = bro.WriteToTopic(config.Development.InitialTopic, item)
	if err != nil {
		test.Error(err)
	}

	for item := range items {
		if item.Message == "Name" && item.Data == "test item" {
			break
		} else {
			test.Errorf("Not right message structure")
			break
		}
	}
}
