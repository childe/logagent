package main

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"testing"
)

const (
	topic = "logstash-iis-nxlog"
)

var kconf_data = []byte(`
{
    "broker_list": ["192.168.81.208:9092"],
    "topic_id": "logstash-iis-nxlog",
    "compression_codec": "gzip",
    "ack_timeout_ms": 100,
    "required_acks": "no_response",
    "flush_frequency_ms": 100
}
`)

func TestNewProducer(t *testing.T) {
	var kconf KafkaConfig
	err := json.Unmarshal(kconf_data, &kconf)

	t.Log(kconf, err)

	p := newProducer(&kconf)
	t.Log(p)

	entry := &iisLogEntry{
		Line: "hahahah",
	}

	// t.Error("...")
	p.Input() <- &sarama.ProducerMessage{
		Topic: kconf.TopicID,
		Key:   sarama.StringEncoder("key"),
		Value: entry,
	}
	p.Close()

	// t.Error("...")
}
