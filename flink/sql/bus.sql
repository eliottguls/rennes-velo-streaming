-- Job Flink SQL bus/métro : GTFS-RT VehiclePosition STAR (Redpanda) -> positions, vitesses, activité par ligne -> Postgres

SET 'pipeline.name' = 'rennes-bus';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout' = '30s';

-- ---------------------------------------------------------------- Sources

CREATE TABLE bus_positions_raw (
    vehicle_id      STRING,
    vehicle_label   STRING,
    trip_id         STRING,
    route_id        STRING,
    direction_id    INT,
    lat             DOUBLE,
    lon             DOUBLE,
    bearing         DOUBLE,
    speed_ms        DOUBLE,
    stop_id         STRING,
    current_status  STRING,
    position_ts     BIGINT,
    feed_ts         BIGINT,
    ingested_at     BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(position_ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bus.vehicle_positions',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-bus-pipeline',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE bus_routes_raw (
    route_id    STRING,
    short_name  STRING,
    long_name   STRING,
    route_type  INT,
    color       STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bus.routes',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-bus-pipeline-routes',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ---------------------------------------------------------------- Sinks (Postgres)

CREATE TABLE pg_bus_routes (
    route_id STRING, short_name STRING, long_name STRING, route_type INT, color STRING,
    PRIMARY KEY (route_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'bus_routes',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE pg_bus_positions_latest (
    vehicle_id STRING, route_id STRING, direction_id INT,
    lat DOUBLE, lon DOUBLE, bearing DOUBLE, speed_kmh DOUBLE, current_status STRING,
    position_time TIMESTAMP(3), ingested_at TIMESTAMP(3), processed_at TIMESTAMP(3),
    PRIMARY KEY (vehicle_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'bus_positions_latest',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '500ms'
);

CREATE TABLE pg_route_activity_1m (
    route_id STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
    vehicles BIGINT, avg_speed_kmh DOUBLE, positions BIGINT,
    PRIMARY KEY (route_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'route_activity_1m',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE pg_bus_latency_1m (
    window_start TIMESTAMP(3), window_end TIMESTAMP(3), events BIGINT,
    avg_source_delay_s DOUBLE, max_source_delay_s DOUBLE,
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'bus_latency_1m',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '1s'
);

-- ---------------------------------------------------------------- Vues intermédiaires

-- Position précédente de chaque véhicule, pour déduire sa vitesse quand le flux ne la donne pas.
CREATE TEMPORARY VIEW bus_with_prev AS
SELECT
    *,
    LAG(lat) OVER w AS prev_lat,
    LAG(lon) OVER w AS prev_lon,
    LAG(position_ts) OVER w AS prev_ts
FROM bus_positions_raw
WINDOW w AS (PARTITION BY vehicle_id ORDER BY event_time);

CREATE TEMPORARY VIEW bus_moves AS
SELECT
    vehicle_id, route_id, direction_id, lat, lon, bearing, current_status,
    position_ts, ingested_at, event_time,
    COALESCE(
        speed_ms * 3.6,
        -- distance équirectangulaire (précise à l'échelle d'une ville) / durée
        CASE WHEN prev_ts IS NOT NULL AND position_ts > prev_ts THEN
            SQRT(POWER((lat - prev_lat) * 110540, 2)
               + POWER((lon - prev_lon) * 111320 * COS(RADIANS(lat)), 2))
            / ((position_ts - prev_ts) / 1000.0) * 3.6
        END
    ) AS speed_kmh
FROM bus_with_prev;

-- ---------------------------------------------------------------- Job

EXECUTE STATEMENT SET
BEGIN

INSERT INTO pg_bus_routes
SELECT route_id, short_name, long_name, route_type, color FROM bus_routes_raw;

INSERT INTO pg_bus_positions_latest
SELECT
    vehicle_id, route_id, direction_id, lat, lon, bearing,
    -- au-delà de 90 km/h c'est un saut GPS, pas une vitesse
    CASE WHEN speed_kmh <= 90 THEN speed_kmh END,
    current_status,
    CAST(event_time AS TIMESTAMP(3)),
    CAST(TO_TIMESTAMP_LTZ(ingested_at, 3) AS TIMESTAMP(3)),
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM bus_moves;

INSERT INTO pg_route_activity_1m
SELECT
    route_id, window_start, window_end,
    COUNT(DISTINCT vehicle_id),
    AVG(CASE WHEN speed_kmh <= 90 THEN speed_kmh END),
    COUNT(*)
FROM TABLE(TUMBLE(TABLE bus_moves, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE route_id IS NOT NULL
GROUP BY route_id, window_start, window_end;

-- Âge de la position quand le producteur la lit : la part de latence due à la source.
-- La latence du pipeline lui-même se lit dans bus_positions_latest (processed_at - ingested_at).
INSERT INTO pg_bus_latency_1m
SELECT
    window_start, window_end, COUNT(*),
    AVG((ingested_at - position_ts) / 1000.0),
    MAX((ingested_at - position_ts) / 1000.0)
FROM TABLE(TUMBLE(TABLE bus_positions_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

END;
