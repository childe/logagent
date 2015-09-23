package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// use encoding/json to format log
func JsonFormat1(event *FileEvent) string {
	logEvent := map[string]string{}

	if len(event.FieldNames) == 0 {
		logEvent["message"] = *event.Text
	} else {
		splited := event.DelimiterRegexp.Split(strings.TrimSpace(*event.Text), -1)
		if len(splited) != event.FieldNamesLength {
			logEvent["message"] = *event.Text
		} else {
			for idx, fieldname := range event.FieldNames {
				logEvent[fieldname] = strings.Trim(splited[idx], event.QuoteChar)
			}
		}
	}

	// dump Fields into json string
	for k, v := range *event.Fields {
		logEvent[k] = v
	}

	logEvent["path"] = *event.Source

	msg, _ := json.Marshal(logEvent)

	return string(msg)
}

//this is copied from https://github.com/golang/go/blob/f9ed2f75c43cb8745a1593ec3e4208c46419216a/src/encoding/json/encode.go
type encodeState struct {
	bytes.Buffer // accumulated output
}

var hex = "0123456789abcdef"

func (e *encodeState) string(s string) (int, error) {
	len0 := e.Len()
	e.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			if start < i {
				e.WriteString(s[start:i])
			}
			switch b {
			case '\\', '"':
				e.WriteByte('\\')
				e.WriteByte(b)
			case '\n':
				e.WriteByte('\\')
				e.WriteByte('n')
			case '\r':
				e.WriteByte('\\')
				e.WriteByte('r')
			case '\t':
				e.WriteByte('\\')
				e.WriteByte('t')
			default:
				// This encodes bytes < 0x20 except for \n and \r,
				// as well as <, > and &. The latter are escaped because they
				// can lead to security holes when user-controlled strings
				// are rendered into JSON and served to some browsers.
				e.WriteString(`\u00`)
				e.WriteByte(hex[b>>4])
				e.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				e.WriteString(s[start:i])
			}
			e.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				e.WriteString(s[start:i])
			}
			e.WriteString(`\u202`)
			e.WriteByte(hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		e.WriteString(s[start:])
	}
	e.WriteByte('"')
	return e.Len() - len0, nil
}

// use string func. we do not need to format complex struct, only map[string]string, so string func could meet our needs
func JsonFormat2(event *FileEvent) string {
	e := &encodeState{}

	e.WriteByte('{')

	if len(event.FieldNames) == 0 {
		e.WriteString("\"message\"")
		e.WriteByte(':')
		e.string(*event.Text)
	} else {
		splited := event.DelimiterRegexp.Split(strings.TrimSpace(*event.Text), -1)
		if len(splited) == event.FieldNamesLength {
			for idx, fieldname := range event.FieldNames {
				if idx != 0 {
					e.WriteByte(',')
				}
				e.WriteString("\"" + fieldname + "\"")
				e.WriteByte(':')
				e.string(strings.Trim(splited[idx], event.QuoteChar))
			}
		} else {
			e.WriteString("\"message\"")
			e.WriteByte(':')
			e.string(*event.Text)
			if event.ExactMatch == false && len(splited) > event.FieldNamesLength {
				for idx, fieldname := range event.FieldNames {
					e.WriteByte(',')
					e.WriteString("\"" + fieldname + "\"")
					e.WriteByte(':')
					e.string(strings.Trim(splited[idx], event.QuoteChar))
				}
			}
		}
	}

	// dump Fields into json string
	for k, v := range *event.Fields {
		e.WriteByte(',')
		e.WriteString("\"" + k + "\"")
		e.WriteByte(':')
		e.WriteString("\"" + v + "\"")
	}

	e.WriteByte('}')

	msg := string(e.Bytes())

	return msg
}

func JsonFormat(event *FileEvent) string {
	return JsonFormat2(event)
}
