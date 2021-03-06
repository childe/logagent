package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os" // for File and friends
	"strings"
	"time"
)

type Harvester struct {
	Path            string /* the file path to harvest */
	FileConfig      FileConfig
	Offset          int64
	FinishChan      chan int64
	mergedBytesread int

	file *os.File /* the file being watched */
}

func (h *Harvester) Harvest(output chan *FileEvent) {
	h.open()
	info, e := h.file.Stat()
	if e != nil {
		panic(fmt.Sprintf("Harvest: unexpected error: %s", e.Error()))
	}
	defer h.file.Close()

	// On completion, push offset so we can continue where we left off if we relaunch on the same file
	defer func() { h.FinishChan <- h.Offset }()

	var line uint64 = 0 // Ask registrar about the line number

	var multilineBuf []string
	var multilineBufIndex int
	if h.FileConfig.Multiline != nil {
		multilineBuf = make([]string, h.FileConfig.Multiline.MaxLine)
		multilineBufIndex = 0
	}

	// get current offset in file
	offset, _ := h.file.Seek(0, os.SEEK_CUR)

	if h.Offset > 0 {
		emit("harvest: %q position:%d (offset snapshot:%d)\n", h.Path, h.Offset, offset)
	} else if options.tailOnRotate {
		emit("harvest: (tailing) %q (offset snapshot:%d)\n", h.Path, offset)
	} else {
		emit("harvest: %q (offset snapshot:%d)\n", h.Path, offset)
	}

	h.Offset = offset

	reader := bufio.NewReaderSize(h.file, options.harvesterBufferSize) // 16kb buffer by default
	buffer := new(bytes.Buffer)

	var read_timeout = 10 * time.Second
	last_read_time := time.Now()
	var shouldReturn = false
	var shouldMultiline = false
	for {
		text, bytesread, err := h.readline(reader, buffer, read_timeout, h.FileConfig.MaxBytes)
		h.mergedBytesread += bytesread

		if err != nil {
			if err == io.EOF {
				// timed out waiting for data, got eof.
				// Check to see if the file was truncated
				info, _ := h.file.Stat()
				if info.Size() < h.Offset {
					emit("File truncated, seeking to beginning: %s\n", h.Path)
					h.file.Seek(0, os.SEEK_SET)
					h.Offset = 0
					shouldMultiline = true
				} else if age := time.Since(last_read_time); age > h.FileConfig.deadtime {
					// if last_read_time was more than dead time, this file is probably
					// dead. Stop watching it.
					emit("Stopping harvest of %s; last change was %v ago\n", h.Path, age)
					shouldReturn = true
				}
			} else {
				emit("Unexpected state reading from %s; error: %s\n", h.Path, err)
				shouldReturn = true
			}
		} else {
			last_read_time = time.Now()
		}

		if h.FileConfig.Multiline != nil && (shouldReturn || shouldMultiline) {
			h.sendEvent(multilineBuf, multilineBufIndex, output, &info, line)
		} else if err == nil {
			line++

			if h.FileConfig.Multiline != nil {
				match := h.FileConfig.Multiline.MatchRegexp.MatchString(*text)
				if match {
					if h.FileConfig.Multiline.Leader == true {
						if multilineBufIndex > 0 {
							h.sendEvent(multilineBuf, multilineBufIndex, output, &info, line)

							multilineBuf[0] = *text
							multilineBufIndex = 1
						} else { // new leader
							multilineBuf[0] = *text
							multilineBufIndex = 1
						}
					} else {
						multilineBuf[multilineBufIndex] = *text
						multilineBufIndex++
						if multilineBufIndex >= h.FileConfig.Multiline.MaxLine {
							h.sendEvent(multilineBuf, multilineBufIndex, output, &info, line)
							multilineBufIndex = 0
						}
					}
				} else { // not match
					if h.FileConfig.Multiline.Leader == true {
						multilineBuf[multilineBufIndex] = *text
						multilineBufIndex++
						if multilineBufIndex >= h.FileConfig.Multiline.MaxLine {
							h.sendEvent(multilineBuf, multilineBufIndex, output, &info, line)
							multilineBufIndex = 0
						}
					} else { // follower
						if multilineBufIndex > 0 {
							h.sendEvent(multilineBuf, multilineBufIndex, output, &info, line)

							multilineBuf[0] = *text
							multilineBufIndex = 1
						} else { // new leader
							multilineBuf[0] = *text
							multilineBufIndex = 1
						}
					}
				}
			} else { // no multiline config
				event := &FileEvent{
					NoHostname:       h.FileConfig.NoHostname,
					NoTimestamp:      h.FileConfig.NoTimestamp,
					NoPath:           h.FileConfig.NoPath,
					Hostname:         &h.FileConfig.Hostname,
					MaxBytes:         h.FileConfig.MaxBytes,
					Source:           &h.Path,
					Offset:           h.Offset,
					Line:             line,
					Text:             text,
					Fields:           &h.FileConfig.Fields,
					FieldNames:       h.FileConfig.FieldNames,
					DelimiterRegexp:  h.FileConfig.DelimiterRegexp,
					ExactMatch:       h.FileConfig.ExactMatch,
					QuoteChar:        h.FileConfig.QuoteChar,
					FieldNamesLength: h.FileConfig.FieldNamesLength,
					fileinfo:         &info,
				}
				h.Offset += int64(bytesread)

				output <- event // ship the new event downstream
			}
		}

		if shouldReturn {
			return
		}

	} /* forever */
}

func (h *Harvester) open() *os.File {
	// Special handling that "-" means to read from standard input
	if h.Path == "-" {
		h.file = os.Stdin
		return h.file
	}

	for {
		var err error
		h.file, err = openfile(h.Path, os.O_RDONLY, 0)

		if err != nil {
			// retry on failure.
			emit("Failed opening %s: %s\n", h.Path, err)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	// Check we are not following a rabbit hole (symlinks, etc.)
	mustBeRegularFile(h.file) // panics

	if h.Offset > 0 {
		h.file.Seek(h.Offset, os.SEEK_SET)
	} else if options.tailOnRotate {
		h.file.Seek(0, os.SEEK_END)
	} else {
		h.file.Seek(0, os.SEEK_SET)
	}

	return h.file
}

func (h *Harvester) readline(reader *bufio.Reader, buffer *bytes.Buffer, eof_timeout time.Duration, maxBytes int) (*string, int, error) {
	var is_partial bool = true
	var newline_length int = 1
	start_time := time.Now()

	for {
		segment, err := reader.ReadBytes('\n')

		if segment != nil && len(segment) > 0 {
			if segment[len(segment)-1] == '\n' {
				// Found a complete line
				is_partial = false

				// Check if also a CR present
				if len(segment) > 1 && segment[len(segment)-2] == '\r' {
					newline_length++
				}
			}

			// TODO(sissel): if buffer exceeds a certain length, maybe report an error condition? chop it?
			buffer.Write(segment)
		}

		if buffer.Len() > maxBytes {
			bufferSize := buffer.Len()
			str := new(string)
			*str = buffer.String()
			// Reset the buffer for the next line
			buffer.Reset()
			return str, bufferSize, nil
		}

		if err != nil {
			if err == io.EOF && is_partial {
				time.Sleep(1 * time.Second) // TODO(sissel): Implement backoff

				// Give up waiting for data after a certain amount of time.
				// If we time out, return the error (eof)
				if time.Since(start_time) > eof_timeout {
					return nil, 0, err
				}
				continue
			} else {
				emit("error: Harvester.readLine: %s", err.Error())
				return nil, 0, err // TODO(sissel): don't do this?
			}
		}

		// If we got a full line, return the whole line without the EOL chars (CRLF or LF)
		if !is_partial {
			// Get the str length with the EOL chars (LF or CRLF)
			bufferSize := buffer.Len()
			str := new(string)
			*str = buffer.String()[:bufferSize-newline_length]
			// Reset the buffer for the next line
			buffer.Reset()
			return str, bufferSize, nil
		}
	} /* forever read chunks */

	return nil, 0, nil
}

// panics
func mustBeRegularFile(f *os.File) {
	if f == nil {
		panic(fmt.Errorf("Harvester: BUG: f arg is nil"))
	}
	info, e := f.Stat()
	if e != nil {
		panic(fmt.Errorf("Harvester: FAULT: stat error: %s", e.Error()))
	}

	if !info.Mode().IsRegular() {
		panic(fmt.Errorf("Harvester: not a regular file:%q", info.Mode(), info.Name()))
	}
}

// sendEvent create a new event and send it ot output channel
func (h *Harvester) sendEvent(multilineBuf []string, multilineBufIndex int,
	output chan *FileEvent, info *os.FileInfo, line uint64) error {
	mergedText := strings.Join(multilineBuf[:multilineBufIndex], "\n")
	multilineBufIndex = 0

	event := &FileEvent{
		NoHostname:       h.FileConfig.NoHostname,
		NoTimestamp:      h.FileConfig.NoTimestamp,
		NoPath:           h.FileConfig.NoPath,
		MaxBytes:         h.FileConfig.MaxBytes,
		Hostname:         &h.FileConfig.Hostname,
		Source:           &h.Path,
		Offset:           h.Offset,
		Line:             line,
		Text:             &mergedText,
		Fields:           &h.FileConfig.Fields,
		FieldNames:       h.FileConfig.FieldNames,
		DelimiterRegexp:  h.FileConfig.DelimiterRegexp,
		ExactMatch:       h.FileConfig.ExactMatch,
		QuoteChar:        h.FileConfig.QuoteChar,
		FieldNamesLength: h.FileConfig.FieldNamesLength,
		fileinfo:         info,
	}
	h.Offset += int64(h.mergedBytesread)
	h.mergedBytesread = 0

	output <- event // ship the new event downstream
	return nil
}
