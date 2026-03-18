package service

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "strings"
    "time"

    "github.com/google/uuid"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/redis/go-redis/v9"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/timestamppb"

    pb "flight-booking/gen/flight"
)

type FlightServiceServer struct {
    pb.UnimplementedFlightServiceServer
    db       *pgxpool.Pool
    rdb      *redis.Client
    cacheTTL time.Duration
}

func NewFlightServiceServer(db *pgxpool.Pool, rdb *redis.Client, cacheTTL time.Duration) *FlightServiceServer {
    return &FlightServiceServer{db: db, rdb: rdb, cacheTTL: cacheTTL}
}

type flightRow struct {
    ID, FlightNumber, Airline, Origin, Destination, Status string
    DepartureTime, ArrivalTime                             time.Time
    TotalSeats, AvailableSeats                             int32
    Price                                                  float64
}

func (f *flightRow) ToProto() *pb.FlightInfo {
    return &pb.FlightInfo{
        Id: f.ID, FlightNumber: f.FlightNumber, Airline: f.Airline,
        Origin: f.Origin, Destination: f.Destination,
        DepartureTime: timestamppb.New(f.DepartureTime),
        ArrivalTime:   timestamppb.New(f.ArrivalTime),
        TotalSeats: f.TotalSeats, AvailableSeats: f.AvailableSeats,
        Price: f.Price, Status: statusToProto(f.Status),
    }
}

func statusToProto(s string) pb.FlightStatus {
    switch s {
    case "SCHEDULED":
        return pb.FlightStatus_FLIGHT_STATUS_SCHEDULED
    case "DEPARTED":
        return pb.FlightStatus_FLIGHT_STATUS_DEPARTED
    case "CANCELLED":
        return pb.FlightStatus_FLIGHT_STATUS_CANCELLED
    case "COMPLETED":
        return pb.FlightStatus_FLIGHT_STATUS_COMPLETED
    default:
        return pb.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
    }
}

func statusFromProto(s pb.FlightStatus) string {
    switch s {
    case pb.FlightStatus_FLIGHT_STATUS_SCHEDULED:
        return "SCHEDULED"
    case pb.FlightStatus_FLIGHT_STATUS_DEPARTED:
        return "DEPARTED"
    case pb.FlightStatus_FLIGHT_STATUS_CANCELLED:
        return "CANCELLED"
    case pb.FlightStatus_FLIGHT_STATUS_COMPLETED:
        return "COMPLETED"
    default:
        return ""
    }
}

func scanFlight(row pgx.Row) (*flightRow, error) {
    f := &flightRow{}
    err := row.Scan(&f.ID, &f.FlightNumber, &f.Airline, &f.Origin, &f.Destination,
        &f.DepartureTime, &f.ArrivalTime, &f.TotalSeats, &f.AvailableSeats, &f.Price, &f.Status)
    return f, err
}

const flightCols = "id,flight_number,airline,origin,destination,departure_time,arrival_time,total_seats,available_seats,price,status"

func (s *FlightServiceServer) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
    if req.Origin == "" || req.Destination == "" {
        return nil, status.Error(codes.InvalidArgument, "origin and destination required")
    }
    cacheKey := fmt.Sprintf("search:%s:%s:%s", req.Origin, req.Destination, req.Date)
    if cached, err := s.rdb.Get(ctx, cacheKey).Result(); err == nil {
        log.Printf("CACHE HIT: %s", cacheKey)
        var flights []*pb.FlightInfo
        if json.Unmarshal([]byte(cached), &flights) == nil {
            return &pb.SearchFlightsResponse{Flights: flights}, nil
        }
    }
    log.Printf("CACHE MISS: %s", cacheKey)
    q := "SELECT " + flightCols + " FROM flights WHERE origin=$1 AND destination=$2 AND status='SCHEDULED'"
    args := []interface{}{req.Origin, req.Destination}
    if req.Date != "" {
        q += " AND DATE(departure_time)=$3"
        args = append(args, req.Date)
    }
    q += " ORDER BY departure_time"
    rows, err := s.db.Query(ctx, q, args...)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "query: %v", err)
    }
    defer rows.Close()
    var flights []*pb.FlightInfo
    for rows.Next() {
        f := &flightRow{}
        if err := rows.Scan(&f.ID, &f.FlightNumber, &f.Airline, &f.Origin, &f.Destination,
            &f.DepartureTime, &f.ArrivalTime, &f.TotalSeats, &f.AvailableSeats, &f.Price, &f.Status); err != nil {
            return nil, status.Errorf(codes.Internal, "scan: %v", err)
        }
        flights = append(flights, f.ToProto())
    }
    if data, err := json.Marshal(flights); err == nil {
        s.rdb.Set(ctx, cacheKey, string(data), s.cacheTTL)
    }
    return &pb.SearchFlightsResponse{Flights: flights}, nil
}

func (s *FlightServiceServer) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.GetFlightResponse, error) {
    if req.Id == "" {
        return nil, status.Error(codes.InvalidArgument, "id required")
    }
    cacheKey := fmt.Sprintf("flight:%s", req.Id)
    if cached, err := s.rdb.Get(ctx, cacheKey).Result(); err == nil {
        log.Printf("CACHE HIT: %s", cacheKey)
        var f pb.FlightInfo
        if json.Unmarshal([]byte(cached), &f) == nil {
            return &pb.GetFlightResponse{Flight: &f}, nil
        }
    }
    log.Printf("CACHE MISS: %s", cacheKey)
    row := s.db.QueryRow(ctx, "SELECT "+flightCols+" FROM flights WHERE id=$1", req.Id)
    f, err := scanFlight(row)
    if err != nil {
        if err == pgx.ErrNoRows {
            return nil, status.Error(codes.NotFound, "flight not found")
        }
        return nil, status.Errorf(codes.Internal, "query: %v", err)
    }
    flight := f.ToProto()
    if data, err := json.Marshal(flight); err == nil {
        s.rdb.Set(ctx, cacheKey, string(data), s.cacheTTL)
    }
    return &pb.GetFlightResponse{Flight: flight}, nil
}

func (s *FlightServiceServer) ReserveSeats(ctx context.Context, req *pb.ReserveSeatsRequest) (*pb.ReserveSeatsResponse, error) {
    if req.FlightId == "" || req.BookingId == "" {
        return nil, status.Error(codes.InvalidArgument, "flight_id and booking_id required")
    }
    if req.SeatCount <= 0 {
        return nil, status.Error(codes.InvalidArgument, "seat_count must be positive")
    }
    var exID string
    var exSC int32
    var exSt string
    var exCA time.Time
    err := s.db.QueryRow(ctx,
        "SELECT id,seat_count,status,created_at FROM seat_reservations WHERE booking_id=$1",
        req.BookingId).Scan(&exID, &exSC, &exSt, &exCA)
    if err == nil {
        log.Printf("IDEMPOTENT: reservation exists for booking %s", req.BookingId)
        return &pb.ReserveSeatsResponse{Reservation: &pb.SeatReservationInfo{
            Id: exID, FlightId: req.FlightId, BookingId: req.BookingId,
            SeatCount: exSC, Status: pb.ReservationStatus_RESERVATION_STATUS_ACTIVE,
            CreatedAt: timestamppb.New(exCA),
        }}, nil
    }
    tx, err := s.db.Begin(ctx)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
    }
    defer tx.Rollback(ctx)
    var avail int32
    err = tx.QueryRow(ctx,
        "SELECT available_seats FROM flights WHERE id=$1 FOR UPDATE", req.FlightId).Scan(&avail)
    if err != nil {
        if err == pgx.ErrNoRows {
            return nil, status.Error(codes.NotFound, "flight not found")
        }
        return nil, status.Errorf(codes.Internal, "select: %v", err)
    }
    if avail < req.SeatCount {
        return nil, status.Errorf(codes.ResourceExhausted,
            "not enough seats: available=%d requested=%d", avail, req.SeatCount)
    }
    tx.Exec(ctx,
        "UPDATE flights SET available_seats=available_seats-$1, updated_at=NOW() WHERE id=$2",
        req.SeatCount, req.FlightId)
    resID := uuid.New().String()
    now := time.Now()
    tx.Exec(ctx,
        "INSERT INTO seat_reservations (id,flight_id,booking_id,seat_count,status,created_at,updated_at) VALUES ($1,$2,$3,$4,'ACTIVE',$5,$5)",
        resID, req.FlightId, req.BookingId, req.SeatCount, now)
    if err := tx.Commit(ctx); err != nil {
        return nil, status.Errorf(codes.Internal, "commit: %v", err)
    }
    s.invalidateCache(ctx, req.FlightId)
    log.Printf("Reserved %d seats flight=%s booking=%s", req.SeatCount, req.FlightId, req.BookingId)
    return &pb.ReserveSeatsResponse{Reservation: &pb.SeatReservationInfo{
        Id: resID, FlightId: req.FlightId, BookingId: req.BookingId,
        SeatCount: req.SeatCount, Status: pb.ReservationStatus_RESERVATION_STATUS_ACTIVE,
        CreatedAt: timestamppb.New(now),
    }}, nil
}

func (s *FlightServiceServer) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
    if req.BookingId == "" {
        return nil, status.Error(codes.InvalidArgument, "booking_id required")
    }
    tx, err := s.db.Begin(ctx)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
    }
    defer tx.Rollback(ctx)
    var resID, flightID, resStatus string
    var seatCount int32
    err = tx.QueryRow(ctx,
        "SELECT id,flight_id,seat_count,status FROM seat_reservations WHERE booking_id=$1 FOR UPDATE",
        req.BookingId).Scan(&resID, &flightID, &seatCount, &resStatus)
    if err != nil {
        if err == pgx.ErrNoRows {
            return nil, status.Error(codes.NotFound, "reservation not found")
        }
        return nil, status.Errorf(codes.Internal, "select: %v", err)
    }
    if resStatus != "ACTIVE" {
        return &pb.ReleaseReservationResponse{Reservation: &pb.SeatReservationInfo{
            Id: resID, FlightId: flightID, BookingId: req.BookingId,
            SeatCount: seatCount, Status: pb.ReservationStatus_RESERVATION_STATUS_RELEASED,
        }}, nil
    }
    tx.Exec(ctx, "UPDATE seat_reservations SET status='RELEASED', updated_at=NOW() WHERE id=$1", resID)
    tx.Exec(ctx, "UPDATE flights SET available_seats=available_seats+$1, updated_at=NOW() WHERE id=$2", seatCount, flightID)
    if err := tx.Commit(ctx); err != nil {
        return nil, status.Errorf(codes.Internal, "commit: %v", err)
    }
    s.invalidateCache(ctx, flightID)
    log.Printf("Released %d seats flight=%s booking=%s", seatCount, flightID, req.BookingId)
    return &pb.ReleaseReservationResponse{Reservation: &pb.SeatReservationInfo{
        Id: resID, FlightId: flightID, BookingId: req.BookingId,
        SeatCount: seatCount, Status: pb.ReservationStatus_RESERVATION_STATUS_RELEASED,
    }}, nil
}

func (s *FlightServiceServer) UpdateFlight(ctx context.Context, req *pb.UpdateFlightRequest) (*pb.UpdateFlightResponse, error) {
    if req.Id == "" {
        return nil, status.Error(codes.InvalidArgument, "id required")
    }
    sets := []string{}
    args := []interface{}{}
    idx := 1
    addSet := func(col string, val interface{}) {
        sets = append(sets, fmt.Sprintf("%s=$%d", col, idx))
        args = append(args, val)
        idx++
    }
    if req.FlightNumber != nil {
        addSet("flight_number", *req.FlightNumber)
    }
    if req.Airline != nil {
        addSet("airline", *req.Airline)
    }
    if req.Origin != nil {
        addSet("origin", *req.Origin)
    }
    if req.Destination != nil {
        addSet("destination", *req.Destination)
    }
    if req.Price != nil {
        addSet("price", *req.Price)
    }
    if req.Status != nil {
        if st := statusFromProto(*req.Status); st != "" {
            addSet("status", st)
        }
    }
    if len(sets) == 0 {
        return nil, status.Error(codes.InvalidArgument, "no fields to update")
    }
    addSet("updated_at", time.Now())
    args = append(args, req.Id)
    q := fmt.Sprintf("UPDATE flights SET %s WHERE id=$%d RETURNING %s",
        strings.Join(sets, ","), idx, flightCols)
    row := s.db.QueryRow(ctx, q, args...)
    f, err := scanFlight(row)
    if err != nil {
        if err == pgx.ErrNoRows {
            return nil, status.Error(codes.NotFound, "flight not found")
        }
        return nil, status.Errorf(codes.Internal, "update: %v", err)
    }
    s.invalidateCache(ctx, req.Id)
    return &pb.UpdateFlightResponse{Flight: f.ToProto()}, nil
}

func (s *FlightServiceServer) invalidateCache(ctx context.Context, flightID string) {
    s.rdb.Del(ctx, fmt.Sprintf("flight:%s", flightID))
    log.Printf("CACHE INVALIDATED: flight:%s", flightID)
    iter := s.rdb.Scan(ctx, 0, "search:*", 100).Iterator()
    for iter.Next(ctx) {
        s.rdb.Del(ctx, iter.Val())
        log.Printf("CACHE INVALIDATED: %s", iter.Val())
    }
}
