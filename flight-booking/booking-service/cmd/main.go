package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "strings"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"

    "flight-booking/booking-service/grpcclient"
    "flight-booking/booking-service/handler"
)

func main() {
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatalf("DB error: %v", err)
    }
    defer pool.Close()
    for i := 0; i < 30; i++ {
        if pool.Ping(ctx) == nil {
            break
        }
        log.Println("Waiting for DB...")
        time.Sleep(time.Second)
    }
    runMigrations(ctx, pool)
    client, err := grpcclient.NewFlightClient(
        os.Getenv("FLIGHT_SERVICE_ADDR"),
        os.Getenv("SERVICE_API_KEY"),
    )
    if err != nil {
        log.Fatalf("gRPC client error: %v", err)
    }
    defer client.Close()
    h := handler.NewBookingHandler(pool, client)
    r := gin.Default()
    r.GET("/flights", h.SearchFlights)
    r.GET("/flights/:id", h.GetFlight)
    r.POST("/bookings", h.CreateBooking)
    r.GET("/bookings/:id", h.GetBooking)
    r.POST("/bookings/:id/cancel", h.CancelBooking)
    r.GET("/bookings", h.ListBookings)
    port := os.Getenv("HTTP_PORT")
    if port == "" {
        port = "8080"
    }
    log.Printf("Booking Service on :%s", port)
    r.Run(":" + port)
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool) {
    dir := "/migrations"
    if _, err := os.Stat(dir); os.IsNotExist(err) {
        dir = "booking-service/migrations"
    }
    entries, err := os.ReadDir(dir)
    if err != nil {
        return
    }
    pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (filename VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT NOW())")
    for _, e := range entries {
        if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
            continue
        }
        var c int
        pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE filename=$1", e.Name()).Scan(&c)
        if c > 0 {
            continue
        }
        content, _ := os.ReadFile(fmt.Sprintf("%s/%s", dir, e.Name()))
        if _, err := pool.Exec(ctx, string(content)); err != nil {
            log.Fatalf("Migration %s: %v", e.Name(), err)
        }
        pool.Exec(ctx, "INSERT INTO schema_migrations (filename) VALUES ($1)", e.Name())
        log.Printf("Migration: %s", e.Name())
    }
}
