package main

import (
	"regexp"
)

func SplitConf(config *Config) (err error) {
	for idx, _ := range config.Files {

		config.Files[idx].DelimiterRegexp = regexp.MustCompile(config.Files[idx].Delimiter)

		config.Files[idx].FieldNamesLength = len(config.Files[idx].FieldNames)
	}

	return nil
}
