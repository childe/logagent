package main

import (
	"os"
	"regexp"
)

type FileEvent struct {
	Source           *string `json:"source,omitempty"`
	Offset           int64   `json:"offset,omitempty"`
	Line             uint64  `json:"line,omitempty"`
	Text             *string `json:"text,omitempty"`
	Fields           *map[string]string
	FieldNames       []string `json:fieldnames`
	DelimiterRegexp  *regexp.Regexp
	ExactMatch       bool
	QuoteChar        string
	FieldNamesLength int
	Hostname         string

	ileinfo  *os.FileInfo
	fileinfo *os.FileInfo
}
