CREATE TABLE IF NOT EXISTS bookings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(100) NOT NULL,
    flight_id UUID NOT NULL,
    passenger_name VARCHAR(200) NOT NULL,
    passenger_email VARCHAR(200) NOT NULL,
    seat_count INT NOT NULL CHECK (seat_count > 0),
    total_price NUMERIC(12,2) NOT NULL CHECK (total_price > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'CONFIRMED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_booking_status CHECK (status IN ('CONFIRMED','CANCELLED'))
);
CREATE INDEX idx_bookings_user ON bookings(user_id);
CREATE INDEX idx_bookings_flight ON bookings(flight_id);
