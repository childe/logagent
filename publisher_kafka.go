package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strings"
	"time"
)

type KafkaConfig struct {
	BrokerList       []string `json:"broker_list"`        // ["localhost:xxx", "remote:xxx"]
	TopicID          string   `json:"topic_id"`           //
	CompressionCodec string   `json:"compression_codec"`  // none, gzip or snappy
	AckTimeoutMS     int      `json:"ack_timeout_ms"`     // milliseconds
	RequiredAcks     string   `json:"required_acks"`      // no_response, wait_for_local, wait_for_all
	FlushFrequencyMS int      `json:"flush_frequency_ms"` // milliseconds
}

func newProducer(kconf *KafkaConfig) sarama.AsyncProducer {

	fmt.Printf("kafka config: %+v\n", kconf)

	config := sarama.NewConfig()

	cc := strings.ToLower(kconf.CompressionCodec)
	switch {
	case cc == "none":
		config.Producer.Compression = sarama.CompressionNone
	case cc == "gzip":
		config.Producer.Compression = sarama.CompressionGZIP // Compress messages
	case cc == "snappy":
		config.Producer.Compression = sarama.CompressionSnappy // Compress messages
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	ra := strings.ToLower(kconf.RequiredAcks)
	switch {
	case ra == "no_response":
		config.Producer.RequiredAcks = sarama.NoResponse
	case ra == "wait_for_local":
		config.Producer.RequiredAcks = sarama.WaitForLocal
	case ra == "wait_for_all":
		config.Producer.RequiredAcks = sarama.WaitForAll
	default:
		config.Producer.RequiredAcks = sarama.NoResponse
	}

	config.Producer.Timeout = time.Millisecond * time.Duration(kconf.AckTimeoutMS)
	config.Producer.Flush.Frequency = time.Millisecond * time.Duration(kconf.FlushFrequencyMS)

	producer, err := sarama.NewAsyncProducer(kconf.BrokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}

type iisLogEntry struct {
	Line string

	encoded []byte
	err     error
}

func (ile *iisLogEntry) encode() []byte {
	return []byte(ile.Line)
}

func (ile *iisLogEntry) ensureEncoded() {
	if ile.encoded == nil && ile.err == nil {
		ile.encoded = ile.encode()
	}
}

func (ile *iisLogEntry) Length() int {
	ile.ensureEncoded()
	return len(ile.encoded)
}

func (ile *iisLogEntry) Encode() ([]byte, error) {
	ile.ensureEncoded()
	return ile.encoded, ile.err
}

func PublishKafka(input chan []*FileEvent,
	registrar chan []*FileEvent,
	kconf *KafkaConfig) {

	producer := newProducer(kconf)
	defer producer.Close()

	for events := range input {
		for _, event := range events {
			splited := event.DelimiterRegexp.Split(*event.Text, -1)

			var msg string
			var jsonFields []string
			i := 0
			if len(splited) != event.FieldNamesLength {
				jsonFields = make([]string, len(*event.Fields)+2)
				jsonFields[0] = "\"message\":\"" + *event.Text + "\""
				i++
				//msg = "{\"message\":\"" + *event.Text + "\"}"
			} else {
				jsonFields = make([]string, event.FieldNamesLength+len(*event.Fields)+1)
				for idx, fieldname := range event.FieldNames {
					i++
					jsonFields[idx] = "\"" + fieldname + "\"" + ":" + event.FieldTypes[idx] + strings.Trim(splited[idx], event.QuoteChar) + event.FieldTypes[idx]
				}
			}

			// dump Fields into json string
			for k, v := range *event.Fields {
				jsonFields[i] = "\"" + k + "\":\"" + v + "\""
				i++
			}

			jsonFields[i] = "\"path\":\"" + *event.Source + "\""

			msg = "{" + strings.Join(jsonFields, ",") + "}"

			entry := &iisLogEntry{
				Line: msg,
			}
			producer.Input() <- &sarama.ProducerMessage{
				Topic: kconf.TopicID,
				Key:   sarama.StringEncoder(""),
				Value: entry,
			}
		}

		// Tell the registrar that we've successfully sent these events
		registrar <- events
	}
}
