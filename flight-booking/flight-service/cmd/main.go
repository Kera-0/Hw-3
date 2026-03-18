package main

import (
    "context"
    "fmt"
    "log"
    "net"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/redis/go-redis/v9"
    "google.golang.org/grpc"

    pb "flight-booking/gen/flight"
    svc "flight-booking/flight-service/service"
)

func main() {
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatalf("DB connect error: %v", err)
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
    rdb := createRedisClient()
    for i := 0; i < 30; i++ {
        if rdb.Ping(ctx).Err() == nil {
            break
        }
        log.Println("Waiting for Redis...")
        time.Sleep(time.Second)
    }
    cacheTTL := 5 * time.Minute
    if v := os.Getenv("CACHE_TTL_MINUTES"); v != "" {
        if m, err := strconv.Atoi(v); err == nil {
            cacheTTL = time.Duration(m) * time.Minute
        }
    }
    apiKey := os.Getenv("SERVICE_API_KEY")
    port := os.Getenv("GRPC_PORT")
    if port == "" {
        port = "50051"
    }
    lis, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("Listen error: %v", err)
    }
    authInt := svc.NewAuthInterceptor(apiKey)
    srv := grpc.NewServer(grpc.UnaryInterceptor(authInt.Unary()))
    pb.RegisterFlightServiceServer(srv, svc.NewFlightServiceServer(pool, rdb, cacheTTL))
    log.Printf("Flight Service on :%s", port)
    if err := srv.Serve(lis); err != nil {
        log.Fatalf("Serve error: %v", err)
    }
}

func createRedisClient() *redis.Client {
    sentinelAddrs := os.Getenv("REDIS_SENTINEL_ADDRS")
    sentinelMaster := os.Getenv("REDIS_SENTINEL_MASTER")
    redisPassword := os.Getenv("REDIS_PASSWORD")
    if sentinelAddrs != "" && sentinelMaster != "" {
        addrs := strings.Split(sentinelAddrs, ",")
        log.Printf("Redis Sentinel: master=%s sentinels=%v", sentinelMaster, addrs)
        return redis.NewFailoverClient(&redis.FailoverOptions{
            MasterName:       sentinelMaster,
            SentinelAddrs:    addrs,
            Password:         redisPassword,
            SentinelPassword: redisPassword,
            DB:               0,
        })
    }
    return redis.NewClient(&redis.Options{Addr: "redis-master:6379", Password: redisPassword, DB: 0})
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool) {
    dir := "/migrations"
    if _, err := os.Stat(dir); os.IsNotExist(err) {
        dir = "flight-service/migrations"
    }
    entries, err := os.ReadDir(dir)
    if err != nil {
        log.Printf("No migrations: %v", err)
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
        content, err := os.ReadFile(fmt.Sprintf("%s/%s", dir, e.Name()))
        if err != nil {
            log.Fatalf("Read migration %s: %v", e.Name(), err)
        }
        if _, err := pool.Exec(ctx, string(content)); err != nil {
            log.Fatalf("Apply migration %s: %v", e.Name(), err)
        }
        pool.Exec(ctx, "INSERT INTO schema_migrations (filename) VALUES ($1)", e.Name())
        log.Printf("Migration applied: %s", e.Name())
    }
}
