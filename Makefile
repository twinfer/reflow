.PHONY: proto build test vet tidy

GOBIN := $(shell go env GOPATH)/bin

proto:
	PATH="$(GOBIN):$$PATH" buf generate

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...

tidy:
	go mod tidy
