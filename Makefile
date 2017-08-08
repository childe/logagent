hash:=$(shell git rev-parse --short HEAD)

default: logagent

all:
	@echo $(hash)
	mkdir -p build/$(hash)

	GOOS=windows GOARCH=amd64 go build -o build/$(hash)/logagent-windows-x64-$(hash).exe
	GOOS=windows GOARCH=386 go build -o build/$(hash)/logagent-windows-386-$(hash).exe
	GOOS=linux GOARCH=amd64 go build -o build/$(hash)/logagent-linux-x64-$(hash)
	GOOS=linux GOARCH=386 go build -o build/$(hash)/logagent-linux-386-$(hash)
	GOOS=darwin GOARCH=amd64 go build -o build/$(hash)/logagent-darwin-x64-$(hash)

logagent:
	go build -o build/logagent
