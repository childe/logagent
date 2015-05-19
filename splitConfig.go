package main

import (
	"fmt"
)

func SplitConf(config *Config) (err error) {
	for idx, c := range config.Files {
		if len(config.Files[idx].FieldNames) != len(config.Files[idx].FieldTypes) {
			return fmt.Errorf("FieldNames length does not match that of FieldTypes")
		}

		config.Files[idx].FieldNamesLength = len(config.Files[idx].FieldNames)

		for innerIdx, fieldtype := range c.FieldTypes {
			if fieldtype == "string" {
				config.Files[idx].FieldTypes[innerIdx] = "\""
			} else if fieldtype == "integer" {
				config.Files[idx].FieldTypes[innerIdx] = ""
			} else if fieldtype == "float" {
				config.Files[idx].FieldTypes[innerIdx] = ""
			}
		}
	}

	return nil
}
