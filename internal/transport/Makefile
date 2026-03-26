.PHONY = build clean

clean: 
	rm -rf gw-ss.pb.go gw-ss_grpc.pb.go

build: clean
	@export PATH="$PATH:$(go env GOPATH)/bin"
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    gw-ss.proto