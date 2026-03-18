FROM golang:1.23 AS builder
RUN apt-get update && apt-get install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.3
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
WORKDIR /app
COPY go.mod go.sum ./
COPY proto/ proto/
RUN mkdir -p gen/flight && \
    protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           proto/flight.proto && \
    mv proto/flight.pb.go gen/flight/ && \
    mv proto/flight_grpc.pb.go gen/flight/
COPY . .
RUN CGO_ENABLED=0 go build -o /booking-service ./booking-service/cmd/main.go
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /booking-service /booking-service
COPY --from=builder /app/booking-service/migrations /migrations
EXPOSE 8080
CMD ["/booking-service"]
