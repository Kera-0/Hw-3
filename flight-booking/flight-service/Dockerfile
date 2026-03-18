FROM golang:1.23.6 AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y protobuf-compiler ca-certificates && rm -rf /var/lib/apt/lists/*

RUN GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5 && \
    GOBIN=/usr/local/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN protoc --proto_path=proto --go_out=. --go-grpc_out=. proto/flight.proto

RUN CGO_ENABLED=0 GOOS=linux go build -o /flight-service ./flight-service/cmd

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /flight-service /flight-service
COPY --from=builder /app/flight-service/migrations /migrations

EXPOSE 50051

CMD ["/flight-service"]