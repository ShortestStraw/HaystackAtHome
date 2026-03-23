include ../Makefile.inc

.PHONY = build
BIN_DIR ?= .

PROG=gw

clean:
	rm -rf $(BIN_DIR)/$(PROG)

build:
	go build -ldflags="-X '$(VERSION_VAR)=$(VERSION)'" -o $(BIN_DIR)/$(PROG) main.go