FROM golang:1.26 AS builder

# Install build dependencies: protoc + protobuf compilers
RUN apt update
RUN apt install golang protobuf-compiler make -y

# Install exact versions of protobuf generators (match your go.mod)
 RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1

# Set working directory
WORKDIR /build

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Ensure generated gRPC code is up-to-date
RUN make build