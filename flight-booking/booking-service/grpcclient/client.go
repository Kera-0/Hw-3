package grpcclient

import (
    "context"
    "log"
    "math"
    "os"
    "strconv"
    "sync"
    "time"

    pb "flight-booking/gen/flight"

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"
)

type CBState int

const (
    CBClosed CBState = iota
    CBOpen
    CBHalfOpen
)

func (s CBState) String() string {
    switch s {
    case CBClosed:
        return "CLOSED"
    case CBOpen:
        return "OPEN"
    case CBHalfOpen:
        return "HALF_OPEN"
    }
    return "UNKNOWN"
}

type CircuitBreaker struct {
    mu               sync.Mutex
    state            CBState
    failureCount     int
    failureThreshold int
    timeout          time.Duration
    lastFailure      time.Time
}

func NewCircuitBreaker() *CircuitBreaker {
    return &CircuitBreaker{
        state:            CBClosed,
        failureThreshold: envInt("CB_FAILURE_THRESHOLD", 5),
        timeout:          time.Duration(envInt("CB_TIMEOUT_SECONDS", 15)) * time.Second,
    }
}

func (cb *CircuitBreaker) Allow() error {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    switch cb.state {
    case CBOpen:
        if time.Since(cb.lastFailure) > cb.timeout {
            log.Println("CIRCUIT BREAKER: OPEN -> HALF_OPEN")
            cb.state = CBHalfOpen
            return nil
        }
        return status.Error(codes.Unavailable, "circuit breaker open")
    }
    return nil
}

func (cb *CircuitBreaker) RecordSuccess() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    if cb.state == CBHalfOpen {
        log.Println("CIRCUIT BREAKER: HALF_OPEN -> CLOSED")
    }
    cb.state = CBClosed
    cb.failureCount = 0
}

func (cb *CircuitBreaker) RecordFailure() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    cb.failureCount++
    cb.lastFailure = time.Now()
    if cb.state == CBHalfOpen {
        log.Println("CIRCUIT BREAKER: HALF_OPEN -> OPEN")
        cb.state = CBOpen
        return
    }
    if cb.failureCount >= cb.failureThreshold {
        log.Printf("CIRCUIT BREAKER: CLOSED -> OPEN (failures=%d)", cb.failureCount)
        cb.state = CBOpen
    }
}

type FlightClient struct {
    conn       *grpc.ClientConn
    client     pb.FlightServiceClient
    apiKey     string
    cb         *CircuitBreaker
    maxRetries int
    baseDelay  time.Duration
}

func NewFlightClient(addr, apiKey string) (*FlightClient, error) {
    var conn *grpc.ClientConn
    var err error
    for i := 0; i < 30; i++ {
        conn, err = grpc.Dial(addr,
            grpc.WithTransportCredentials(insecure.NewCredentials()),
            grpc.WithBlock(),
            grpc.WithTimeout(3*time.Second),
        )
        if err == nil {
            break
        }
        log.Printf("Connecting to Flight Service %s... %v", addr, err)
        time.Sleep(time.Second)
    }
    if err != nil {
        return nil, err
    }
    return &FlightClient{
        conn:       conn,
        client:     pb.NewFlightServiceClient(conn),
        apiKey:     apiKey,
        cb:         NewCircuitBreaker(),
        maxRetries: envInt("RETRY_MAX_ATTEMPTS", 3),
        baseDelay:  time.Duration(envInt("RETRY_BASE_DELAY_MS", 100)) * time.Millisecond,
    }, nil
}

func (fc *FlightClient) Close() error { return fc.conn.Close() }

func (fc *FlightClient) authCtx(ctx context.Context) context.Context {
    return metadata.AppendToOutgoingContext(ctx, "x-api-key", fc.apiKey)
}

func isRetryable(c codes.Code) bool {
    return c == codes.Unavailable || c == codes.DeadlineExceeded
}

func (fc *FlightClient) do(ctx context.Context, name string, fn func(context.Context) error) error {
    if err := fc.cb.Allow(); err != nil {
        log.Printf("CB BLOCKED: %s", name)
        return err
    }
    var lastErr error
    for i := 0; i < fc.maxRetries; i++ {
        if i > 0 {
            d := fc.baseDelay * time.Duration(math.Pow(2, float64(i-1)))
            log.Printf("RETRY %s attempt %d/%d delay=%v", name, i+1, fc.maxRetries, d)
            time.Sleep(d)
        }
        err := fn(fc.authCtx(ctx))
        if err == nil {
            fc.cb.RecordSuccess()
            return nil
        }
        lastErr = err
        if !isRetryable(status.Code(err)) {
            return err
        }
    }
    fc.cb.RecordFailure()
    return lastErr
}

func (fc *FlightClient) SearchFlights(ctx context.Context, origin, dest, date string) (*pb.SearchFlightsResponse, error) {
    var r *pb.SearchFlightsResponse
    err := fc.do(ctx, "SearchFlights", func(c context.Context) error {
        var e error
        r, e = fc.client.SearchFlights(c, &pb.SearchFlightsRequest{Origin: origin, Destination: dest, Date: date})
        return e
    })
    return r, err
}

func (fc *FlightClient) GetFlight(ctx context.Context, id string) (*pb.GetFlightResponse, error) {
    var r *pb.GetFlightResponse
    err := fc.do(ctx, "GetFlight", func(c context.Context) error {
        var e error
        r, e = fc.client.GetFlight(c, &pb.GetFlightRequest{Id: id})
        return e
    })
    return r, err
}

func (fc *FlightClient) ReserveSeats(ctx context.Context, flightID, bookingID string, seats int32) (*pb.ReserveSeatsResponse, error) {
    var r *pb.ReserveSeatsResponse
    err := fc.do(ctx, "ReserveSeats", func(c context.Context) error {
        var e error
        r, e = fc.client.ReserveSeats(c, &pb.ReserveSeatsRequest{
            FlightId: flightID, BookingId: bookingID, SeatCount: seats,
        })
        return e
    })
    return r, err
}

func (fc *FlightClient) ReleaseReservation(ctx context.Context, bookingID string) (*pb.ReleaseReservationResponse, error) {
    var r *pb.ReleaseReservationResponse
    err := fc.do(ctx, "ReleaseReservation", func(c context.Context) error {
        var e error
        r, e = fc.client.ReleaseReservation(c, &pb.ReleaseReservationRequest{BookingId: bookingID})
        return e
    })
    return r, err
}

func envInt(k string, d int) int {
    v := os.Getenv(k)
    if v == "" {
        return d
    }
    i, err := strconv.Atoi(v)
    if err != nil {
        return d
    }
    return i
}
