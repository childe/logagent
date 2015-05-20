package main

import (
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
	WriteTimeout     string   `json:"write_timeout"`      // string, 100ms, 1s, default 1s
	DailTimeout      string   `json:"dail_timeout"`       // string, 100ms, 1s, default 5s
	KeepAlive        string   `json:"keepalive"`          // string, 100ms, 1s, 0 to disable it. default 30s
}

func MustParseInterval(interval string, dft time.Duration) time.Duration {
	if interval == "" {
		return dft
	}

	d, err := time.ParseDuration(interval)
	if d <= 0 || err != nil {
		return dft
	}
	return d
}

func newProducer(kconf *KafkaConfig) sarama.SyncProducer {
	config := sarama.NewConfig()

	config.Net.DialTimeout = MustParseInterval(kconf.DailTimeout, time.Second*5)
	config.Net.WriteTimeout = MustParseInterval(kconf.WriteTimeout, time.Second*1)
	config.Net.ReadTimeout = time.Second * 10
	config.Net.KeepAlive = MustParseInterval(kconf.KeepAlive, time.Second*30)

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

	log.Printf("kconf: %+v", config)

	producer, err := sarama.NewSyncProducer(kconf.BrokerList, config)
	if err != nil {
		log.Println("failed to start producer:", err, kconf.BrokerList)
		return nil
	}

	log.Println("created new producer: ", kconf.BrokerList)
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

var (
	producer sarama.SyncProducer
)

func get_producer(kconf *KafkaConfig) sarama.SyncProducer {
	if producer == nil {
		producer = newProducer(kconf)
	}
	return producer
}

func CloseProducer() {
	if producer != nil {
		producer.Close()
	}
}

func PublishKafka(input chan []*FileEvent,
	registrar chan []*FileEvent,
	kconf *KafkaConfig) {

	for events := range input {

		p := get_producer(kconf)
		if p == nil {
			log.Println("no producer, events cnt: ", len(events))
			// un-acked FileEvent will be consumed later again.
		} else {
			msg := ""
			for _, event := range events {
				splited := event.DelimiterRegexp.Split(*event.Text, -1)

				if len(splited) != event.FieldNamesLength {
					msg += "{\"message\":\"" + *event.Text + "\"}\n"
				} else {
					jsonFields := make([]string, event.FieldNamesLength)
					for idx, fieldname := range event.FieldNames {
						//fmt.Println(idx, fieldname)
						jsonFields[idx] = "\"" + fieldname + "\"" + ":" + event.FieldTypes[idx] + strings.Trim(splited[idx], event.QuoteChar) + event.FieldTypes[idx]
					}
					msg += "{" + strings.Join(jsonFields, ",") + "}\n"

					//// dump Fields into json string
					//for k, v := range *event.Fields {
					//jsonFields[idx] = "\"" + k + "\":\"" + v + "\""
					//i++
					//}

					//jsonFields[idx] = "\"path\":\"" + *event.Source + "\""
					//msg = "{" + strings.Join(jsonFields, ",") + "}"
				}
			}

			entry := &iisLogEntry{
				Line: msg,
			}

			_, _, err := p.SendMessage(&sarama.ProducerMessage{
				Topic: kconf.TopicID,
				Key:   sarama.StringEncoder(""),
				Value: entry,
			})

			//FIXME: data may lost if remote kafka cluster down a little while. coz unacked events
			// will not re-sent before the next ack. and ack only changes Offset of last success..
			if err == nil {
				// Tell the registrar that we've successfully sent these events
				registrar <- events
			} else {
				p = nil // if error happens, we nil producer, force it to reconnect
			}
		} // p == nil
	} // for events := range input
}
