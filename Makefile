
export GOPATH := $(GOPATH):$(PWD)

SRC=src/conf/*.go

.PHONY: all deps test

all: test

deps:

test:
	go test conf -test.v

