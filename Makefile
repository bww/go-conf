
SOURCES=*.go

all: $(SOURCES)
	go get -d && go test -test.v

