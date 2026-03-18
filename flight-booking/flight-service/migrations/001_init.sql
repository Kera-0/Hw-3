CREATE TABLE IF NOT EXISTS flights (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_number VARCHAR(10) NOT NULL,
    airline VARCHAR(100) NOT NULL,
    origin VARCHAR(3) NOT NULL,
    destination VARCHAR(3) NOT NULL,
    departure_time TIMESTAMPTZ NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL,
    total_seats INT NOT NULL CHECK (total_seats > 0),
    available_seats INT NOT NULL CHECK (available_seats >= 0),
    price NUMERIC(10,2) NOT NULL CHECK (price > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_flight_number_date UNIQUE (flight_number, departure_time),
    CONSTRAINT chk_available_le_total CHECK (available_seats <= total_seats),
    CONSTRAINT chk_status CHECK (status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED'))
);

CREATE TABLE IF NOT EXISTS seat_reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_id UUID NOT NULL REFERENCES flights(id),
    booking_id UUID NOT NULL UNIQUE,
    seat_count INT NOT NULL CHECK (seat_count > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_reservation_status CHECK (status IN ('ACTIVE','RELEASED','EXPIRED'))
);

CREATE INDEX idx_flights_route_date ON flights(origin, destination, departure_time);
CREATE INDEX idx_flights_status ON flights(status);
CREATE INDEX idx_reservations_flight ON seat_reservations(flight_id);
CREATE INDEX idx_reservations_booking ON seat_reservations(booking_id);

INSERT INTO flights (flight_number, airline, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status) VALUES
('SU1234', 'Aeroflot', 'VKO', 'LED', '2026-04-01 10:00:00+03', '2026-04-01 11:30:00+03', 180, 180, 5500.00, 'SCHEDULED'),
('S72345', 'S7 Airlines', 'DME', 'LED', '2026-04-01 14:00:00+03', '2026-04-01 15:30:00+03', 160, 160, 4800.00, 'SCHEDULED'),
('SU5678', 'Aeroflot', 'LED', 'VKO', '2026-04-02 08:00:00+03', '2026-04-02 09:30:00+03', 180, 180, 5200.00, 'SCHEDULED'),
('DP4567', 'Pobeda', 'VKO', 'AER', '2026-04-01 06:00:00+03', '2026-04-01 08:30:00+03', 189, 189, 3200.00, 'SCHEDULED');
