package handler

import (
    "log"
    "net/http"

    "github.com/gin-gonic/gin"
    "github.com/google/uuid"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    "flight-booking/booking-service/grpcclient"
)

type BookingHandler struct {
    db     *pgxpool.Pool
    flight *grpcclient.FlightClient
}

func NewBookingHandler(db *pgxpool.Pool, flight *grpcclient.FlightClient) *BookingHandler {
    return &BookingHandler{db: db, flight: flight}
}

type CreateReq struct {
    UserID         string `json:"user_id" binding:"required"`
    FlightID       string `json:"flight_id" binding:"required"`
    PassengerName  string `json:"passenger_name" binding:"required"`
    PassengerEmail string `json:"passenger_email" binding:"required"`
    SeatCount      int32  `json:"seat_count" binding:"required,min=1"`
}

type BookingResp struct {
    ID             string  `json:"id"`
    UserID         string  `json:"user_id"`
    FlightID       string  `json:"flight_id"`
    PassengerName  string  `json:"passenger_name"`
    PassengerEmail string  `json:"passenger_email"`
    SeatCount      int32   `json:"seat_count"`
    TotalPrice     float64 `json:"total_price"`
    Status         string  `json:"status"`
}

type FlightResp struct {
    ID             string  `json:"id"`
    FlightNumber   string  `json:"flight_number"`
    Airline        string  `json:"airline"`
    Origin         string  `json:"origin"`
    Destination    string  `json:"destination"`
    DepartureTime  string  `json:"departure_time"`
    ArrivalTime    string  `json:"arrival_time"`
    TotalSeats     int32   `json:"total_seats"`
    AvailableSeats int32   `json:"available_seats"`
    Price          float64 `json:"price"`
    Status         string  `json:"status"`
}

func (h *BookingHandler) SearchFlights(c *gin.Context) {
    origin := c.Query("origin")
    dest := c.Query("destination")
    if origin == "" || dest == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "origin and destination required"})
        return
    }
    resp, err := h.flight.SearchFlights(c.Request.Context(), origin, dest, c.Query("date"))
    if err != nil {
        grpcErr(c, err)
        return
    }
    out := make([]FlightResp, 0, len(resp.Flights))
    for _, f := range resp.Flights {
        out = append(out, FlightResp{
            ID: f.Id, FlightNumber: f.FlightNumber, Airline: f.Airline,
            Origin: f.Origin, Destination: f.Destination,
            DepartureTime: f.DepartureTime.AsTime().Format("2006-01-02T15:04:05Z07:00"),
            ArrivalTime:   f.ArrivalTime.AsTime().Format("2006-01-02T15:04:05Z07:00"),
            TotalSeats: f.TotalSeats, AvailableSeats: f.AvailableSeats,
            Price: f.Price, Status: f.Status.String(),
        })
    }
    c.JSON(http.StatusOK, out)
}

func (h *BookingHandler) GetFlight(c *gin.Context) {
    resp, err := h.flight.GetFlight(c.Request.Context(), c.Param("id"))
    if err != nil {
        grpcErr(c, err)
        return
    }
    f := resp.Flight
    c.JSON(http.StatusOK, FlightResp{
        ID: f.Id, FlightNumber: f.FlightNumber, Airline: f.Airline,
        Origin: f.Origin, Destination: f.Destination,
        DepartureTime: f.DepartureTime.AsTime().Format("2006-01-02T15:04:05Z07:00"),
        ArrivalTime:   f.ArrivalTime.AsTime().Format("2006-01-02T15:04:05Z07:00"),
        TotalSeats: f.TotalSeats, AvailableSeats: f.AvailableSeats,
        Price: f.Price, Status: f.Status.String(),
    })
}

func (h *BookingHandler) CreateBooking(c *gin.Context) {
    var req CreateReq
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }
    ctx := c.Request.Context()
    fr, err := h.flight.GetFlight(ctx, req.FlightID)
    if err != nil {
        grpcErr(c, err)
        return
    }
    totalPrice := float64(req.SeatCount) * fr.Flight.Price
    bookingID := uuid.New().String()
    if _, err = h.flight.ReserveSeats(ctx, req.FlightID, bookingID, req.SeatCount); err != nil {
        grpcErr(c, err)
        return
    }
    _, dbErr := h.db.Exec(ctx,
        "INSERT INTO bookings (id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price,status) VALUES ($1,$2,$3,$4,$5,$6,$7,'CONFIRMED')",
        bookingID, req.UserID, req.FlightID, req.PassengerName, req.PassengerEmail, req.SeatCount, totalPrice)
    if dbErr != nil {
        log.Printf("DB error, releasing seats: %v", dbErr)
        h.flight.ReleaseReservation(ctx, bookingID)
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create booking"})
        return
    }
    c.JSON(http.StatusCreated, BookingResp{
        ID: bookingID, UserID: req.UserID, FlightID: req.FlightID,
        PassengerName: req.PassengerName, PassengerEmail: req.PassengerEmail,
        SeatCount: req.SeatCount, TotalPrice: totalPrice, Status: "CONFIRMED",
    })
}

func (h *BookingHandler) GetBooking(c *gin.Context) {
    var b BookingResp
    var ca, ua interface{}
    err := h.db.QueryRow(c.Request.Context(),
        "SELECT id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price,status,created_at,updated_at FROM bookings WHERE id=$1",
        c.Param("id")).Scan(&b.ID, &b.UserID, &b.FlightID, &b.PassengerName, &b.PassengerEmail,
        &b.SeatCount, &b.TotalPrice, &b.Status, &ca, &ua)
    if err != nil {
        if err == pgx.ErrNoRows {
            c.JSON(http.StatusNotFound, gin.H{"error": "booking not found"})
            return
        }
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    c.JSON(http.StatusOK, b)
}

func (h *BookingHandler) CancelBooking(c *gin.Context) {
    id := c.Param("id")
    ctx := c.Request.Context()
    var st string
    err := h.db.QueryRow(ctx, "SELECT status FROM bookings WHERE id=$1", id).Scan(&st)
    if err != nil {
        if err == pgx.ErrNoRows {
            c.JSON(http.StatusNotFound, gin.H{"error": "booking not found"})
            return
        }
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    if st != "CONFIRMED" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "booking not CONFIRMED"})
        return
    }
    if _, err := h.flight.ReleaseReservation(ctx, id); err != nil {
        log.Printf("Release warning: %v", err)
    }
    h.db.Exec(ctx, "UPDATE bookings SET status='CANCELLED', updated_at=NOW() WHERE id=$1", id)
    c.JSON(http.StatusOK, gin.H{"message": "cancelled", "id": id})
}

func (h *BookingHandler) ListBookings(c *gin.Context) {
    uid := c.Query("user_id")
    if uid == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
        return
    }
    rows, err := h.db.Query(c.Request.Context(),
        "SELECT id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price,status FROM bookings WHERE user_id=$1 ORDER BY created_at DESC",
        uid)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    defer rows.Close()
    out := make([]BookingResp, 0)
    for rows.Next() {
        var b BookingResp
        rows.Scan(&b.ID, &b.UserID, &b.FlightID, &b.PassengerName,
            &b.PassengerEmail, &b.SeatCount, &b.TotalPrice, &b.Status)
        out = append(out, b)
    }
    c.JSON(http.StatusOK, out)
}

func grpcErr(c *gin.Context, err error) {
    st, ok := status.FromError(err)
    if !ok {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    switch st.Code() {
    case codes.NotFound:
        c.JSON(http.StatusNotFound, gin.H{"error": st.Message()})
    case codes.InvalidArgument:
        c.JSON(http.StatusBadRequest, gin.H{"error": st.Message()})
    case codes.ResourceExhausted:
        c.JSON(http.StatusConflict, gin.H{"error": st.Message()})
    case codes.Unauthenticated:
        c.JSON(http.StatusUnauthorized, gin.H{"error": st.Message()})
    case codes.Unavailable:
        c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service unavailable"})
    default:
        c.JSON(http.StatusInternalServerError, gin.H{"error": st.Message()})
    }
}
