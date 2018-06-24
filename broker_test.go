package broker

import (
	"testing"

	"github.com/hecatoncheir/Configuration"
)

func TestBrokerCanSendMessage(test *testing.T) {
	config := configuration.New()

	firstService := New(config.APIVersion, "First service")
	err := firstService.Connect(config.Development.EventBus.Host, config.Development.EventBus.Port)
	if err != nil {
		test.Error(err)
	}

	defer firstService.Connection.Close()

	secondService := New(config.APIVersion, "Second service")
	err = secondService.Connect(config.Development.EventBus.Host, config.Development.EventBus.Port)

	if err != nil {
		test.Error(err)
	}

	defer secondService.Connection.Close()

	// item := map[string]string{"Name": "test item"}

	item := EventData{Message: "Name", Data: "test item"}

	go secondService.Write(item)

	for item := range firstService.InputChannel {
		if item.Message == "Name" && item.Data == "test item" && item.ServiceName == "Second service" {
			break
		} else {
			test.Errorf("Not right message structure")
			break
		}
	}
}
