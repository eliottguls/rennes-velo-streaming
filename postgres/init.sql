-- Tables de service alimentées par Flink (sink JDBC) et lues par Grafana.

CREATE TABLE IF NOT EXISTS stations (
    station_id  TEXT PRIMARY KEY,
    name        TEXT,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    capacity    INT,
    updated_at  TIMESTAMP
);

-- État courant de chaque station (upsert).
CREATE TABLE IF NOT EXISTS station_status_latest (
    station_id       TEXT PRIMARY KEY,
    bikes_available  INT,
    docks_available  INT,
    is_renting       BOOLEAN,
    is_returning     BOOLEAN,
    last_reported    TIMESTAMP,
    ingested_at      TIMESTAMP,
    latency_seconds  DOUBLE PRECISION
);

-- Disponibilité agrégée par station sur des fenêtres de 5 minutes.
CREATE TABLE IF NOT EXISTS station_availability_5m (
    station_id    TEXT,
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    avg_bikes     DOUBLE PRECISION,
    min_bikes     INT,
    max_bikes     INT,
    samples       BIGINT,
    PRIMARY KEY (station_id, window_start)
);

-- Activité du réseau : départs/arrivées déduits des variations de stock.
CREATE TABLE IF NOT EXISTS network_activity_5m (
    window_start  TIMESTAMP PRIMARY KEY,
    window_end    TIMESTAMP,
    departures    BIGINT,
    arrivals      BIGINT,
    active_stations BIGINT
);

-- Événements : une station devient vide ou pleine.
CREATE TABLE IF NOT EXISTS station_alerts (
    station_id   TEXT,
    alert_type   TEXT,
    event_time   TIMESTAMP,
    bikes_available INT,
    docks_available INT,
    PRIMARY KEY (station_id, event_time, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_avail_window ON station_availability_5m (window_start);
CREATE INDEX IF NOT EXISTS idx_alerts_time ON station_alerts (event_time);

-- Vue pratique pour la carte Grafana.
CREATE OR REPLACE VIEW station_map AS
SELECT s.station_id, s.name, s.lat, s.lon, s.capacity,
       l.bikes_available, l.docks_available, l.last_reported,
       CASE WHEN s.capacity > 0 THEN round(100.0 * l.bikes_available / s.capacity) END AS fill_pct
FROM stations s
JOIN station_status_latest l USING (station_id);

-- Utilisateur en lecture seule pour Grafana.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
    END IF;
END $$;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;

-- ================================================================ Bus / métro (GTFS-RT)

CREATE TABLE IF NOT EXISTS bus_routes (
    route_id    TEXT PRIMARY KEY,
    short_name  TEXT,
    long_name   TEXT,
    route_type  INT,
    color       TEXT
);

-- Dernière position connue de chaque véhicule (upsert).
CREATE TABLE IF NOT EXISTS bus_positions_latest (
    vehicle_id      TEXT PRIMARY KEY,
    route_id        TEXT,
    direction_id    INT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    bearing         DOUBLE PRECISION,
    speed_kmh       DOUBLE PRECISION,
    current_status  TEXT,
    position_time   TIMESTAMP,
    ingested_at     TIMESTAMP,
    processed_at    TIMESTAMP
);

-- Activité par ligne sur des fenêtres d'une minute.
CREATE TABLE IF NOT EXISTS route_activity_1m (
    route_id       TEXT,
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    vehicles       BIGINT,
    avg_speed_kmh  DOUBLE PRECISION,
    positions      BIGINT,
    PRIMARY KEY (route_id, window_start)
);

-- Âge des positions à l'ingestion, par minute (part de latence due à la source).
CREATE TABLE IF NOT EXISTS bus_latency_1m (
    window_start          TIMESTAMP PRIMARY KEY,
    window_end            TIMESTAMP,
    events                BIGINT,
    avg_source_delay_s    DOUBLE PRECISION,
    max_source_delay_s    DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_route_activity_window ON route_activity_1m (window_start);

CREATE OR REPLACE VIEW bus_map AS
SELECT p.vehicle_id, COALESCE(r.short_name, p.route_id) AS ligne, r.color,
       p.lat, p.lon, p.bearing, round(p.speed_kmh::numeric, 1) AS speed_kmh,
       p.current_status, p.position_time
FROM bus_positions_latest p
LEFT JOIN bus_routes r USING (route_id)
WHERE p.position_time > (now() AT TIME ZONE 'UTC') - INTERVAL '5 minutes';

GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
