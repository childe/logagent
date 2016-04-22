package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"text/template"
	"time"
)

const configFileSizeLimit = 10 << 20

var defaultConfig = &struct {
	netTimeout   int64
	fileDeadtime string
}{
	netTimeout:   15,
	fileDeadtime: "24h",
}

// Config is parsed from a json file, including files and kakfa config
type Config struct {
	Files []FileConfig `json:"files"`
	Kafka KafkaConfig  `json:"kafka"`
}

// FileConfig :
// Paths: list of paths
// Fields: a dict, add this dict to the whole event
// FieldNames: split the message to FieldsNames
// ExactMatch: if set it to false, "error errormsg abcd xyz" could be splited to
// logleve and logmessage. if set to true, could not splitted, because
//splited parts do not match.
// TODO
type FileConfig struct {
	Paths            []string          `json:"paths"`
	Fields           map[string]string `json:"fields"`
	FieldNames       []string          `json:"fieldnames"`
	ExactMatch       bool
	Delimiter        string
	DelimiterRegexp  *regexp.Regexp
	QuoteChar        string
	DeadTime         string
	FieldNamesLength int
	deadtime         time.Duration
	Hostname         string
	NoHostname       bool
	NoPath           bool
	NoTimestamp      bool

	HarvestFromBeginningOnNewFile bool
}

// DiscoverConfigs parse config from config file
func DiscoverConfigs(fileOrDirectory string) (files []string, err error) {
	fi, err := os.Stat(fileOrDirectory)
	if err != nil {
		return nil, err
	}
	files = make([]string, 0)
	if fi.IsDir() {
		entries, err := ioutil.ReadDir(fileOrDirectory)
		if err != nil {
			return nil, err
		}
		for _, filename := range entries {
			files = append(files, path.Join(fileOrDirectory, filename.Name()))
		}
	} else {
		files = append(files, fileOrDirectory)
	}
	return files, nil
}

// MergeConfig Append values to the 'to' config from the 'from' config, erroring
// if a value would be overwritten by the merge.
func MergeConfig(to *Config, from Config) (err error) {

	to.Kafka.AckTimeoutMS = from.Kafka.AckTimeoutMS
	to.Kafka.BrokerList = append(to.Kafka.BrokerList, from.Kafka.BrokerList...)
	to.Kafka.CompressionCodec = from.Kafka.CompressionCodec
	to.Kafka.FlushFrequencyMS = from.Kafka.FlushFrequencyMS
	to.Kafka.RequiredAcks = from.Kafka.RequiredAcks
	to.Kafka.TopicID = from.Kafka.TopicID
	to.Kafka.TopicIDTemplate = template.Must(template.New("topic").Parse(from.Kafka.TopicID))
	to.Kafka.KeepAlive = from.Kafka.KeepAlive
	to.Kafka.RefreshFrequency = from.Kafka.RefreshFrequency
	to.Kafka.Key = from.Kafka.Key
	if from.Kafka.Key != nil {
		to.Kafka.KeyTemplate = template.Must(template.New("key").Parse(*from.Kafka.Key))
	} else {
		to.Kafka.KeyTemplate = nil
	}

	to.Files = append(to.Files, from.Files...)

	return nil
}

// LoadConfig load config from config file
func LoadConfig(path string) (config Config, err error) {
	config.Kafka.RefreshFrequency = 600000
	config.Kafka.Key = nil

	configFile, err := os.Open(path)
	if err != nil {
		emit("Failed to open config file '%s': %s\n", path, err)
		return
	}

	fi, _ := configFile.Stat()
	if size := fi.Size(); size > (configFileSizeLimit) {
		emit("config file (%q) size exceeds reasonable limit (%d) - aborting", path, size)
		return // REVU: shouldn't this return an error, then?
	}

	if fi.Size() == 0 {
		emit("config file (%q) is empty, skipping", path)
		return
	}

	buffer := make([]byte, fi.Size())
	_, err = configFile.Read(buffer)
	emit("%s\n", buffer)

	buffer, err = StripComments(buffer)
	if err != nil {
		emit("Failed to strip comments from json: %s\n", err)
		return
	}

	err = json.Unmarshal(buffer, &config)
	if err != nil {
		emit("Failed unmarshalling json: %s\n", err)
		return
	}

	for k := range config.Files {
		if config.Files[k].DeadTime == "" {
			config.Files[k].DeadTime = defaultConfig.fileDeadtime
		}
		config.Files[k].deadtime, err = time.ParseDuration(config.Files[k].DeadTime)
		if err != nil {
			emit("Failed to parse dead time duration '%s'. Error was: %s\n", config.Files[k].DeadTime, err)
			return
		}
		hostname, err := os.Hostname()
		if err == nil {
			config.Files[k].Hostname = hostname
		} else {
			emit("Failed to get hostname")
		}
	}

	return
}

// FinalizeConfig set default config
func FinalizeConfig(config *Config) {
}

// StripComments remove comments from json config file
func StripComments(data []byte) ([]byte, error) {
	data = bytes.Replace(data, []byte("\r"), []byte(""), 0) // Windows
	lines := bytes.Split(data, []byte("\n"))
	var filtered [][]byte

	for _, line := range lines {
		match, err := regexp.Match(`^\s*#`, line)
		if err != nil {
			return nil, err
		}
		if !match {
			filtered = append(filtered, line)
		}
	}

	return bytes.Join(filtered, []byte("\n")), nil
}
