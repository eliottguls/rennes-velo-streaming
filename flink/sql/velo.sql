-- Job Flink SQL vélos : GBFS STAR (Redpanda) -> agrégats temps réel -> Postgres
-- Toutes les requêtes INSERT tournent dans un seul job (STATEMENT SET).

SET 'pipeline.name' = 'rennes-velo';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Une station sans changement n'émet rien : sans ce réglage le watermark resterait bloqué.
SET 'table.exec.source.idle-timeout' = '90s';

-- ---------------------------------------------------------------- Sources

CREATE TABLE station_status_raw (
    station_id        STRING,
    bikes_available   INT,
    ebikes_available  INT,
    docks_available   INT,
    is_installed      BOOLEAN,
    is_renting        BOOLEAN,
    is_returning      BOOLEAN,
    last_reported     BIGINT,
    feed_last_updated BIGINT,
    ingested_at       BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(last_reported, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '2' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'velo.station_status',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-velo-pipeline',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE station_information_raw (
    station_id  STRING,
    name        STRING,
    lat         DOUBLE,
    lon         DOUBLE,
    capacity    INT,
    ingested_at BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'velo.station_information',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-velo-pipeline-info',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ---------------------------------------------------------------- Sinks (Postgres, upsert par clé primaire)

CREATE TABLE pg_stations (
    station_id STRING, name STRING, lat DOUBLE, lon DOUBLE, capacity INT, updated_at TIMESTAMP(3),
    PRIMARY KEY (station_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'stations',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE pg_station_status_latest (
    station_id STRING, bikes_available INT, docks_available INT,
    is_renting BOOLEAN, is_returning BOOLEAN,
    last_reported TIMESTAMP(3), ingested_at TIMESTAMP(3), latency_seconds DOUBLE,
    PRIMARY KEY (station_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'station_status_latest',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE pg_station_availability_5m (
    station_id STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
    avg_bikes DOUBLE, min_bikes INT, max_bikes INT, samples BIGINT,
    PRIMARY KEY (station_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'station_availability_5m',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE pg_network_activity_5m (
    window_start TIMESTAMP(3), window_end TIMESTAMP(3),
    departures BIGINT, arrivals BIGINT, active_stations BIGINT,
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'network_activity_5m',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE pg_station_alerts (
    station_id STRING, alert_type STRING, event_time TIMESTAMP(3),
    bikes_available INT, docks_available INT,
    PRIMARY KEY (station_id, event_time, alert_type) NOT ENFORCED
) WITH (
    'connector' = 'jdbc', 'url' = '${POSTGRES_JDBC_URL}', 'table-name' = 'station_alerts',
    'username' = '${POSTGRES_USER}', 'password' = '${POSTGRES_PASSWORD}',
    'sink.buffer-flush.interval' = '2s'
);

-- ---------------------------------------------------------------- Vues intermédiaires

-- Variation du stock de chaque station par rapport à la mesure précédente.
CREATE TEMPORARY VIEW station_deltas AS
SELECT
    station_id,
    bikes_available,
    docks_available,
    event_time,
    bikes_available - LAG(bikes_available) OVER w AS delta_bikes,
    LAG(bikes_available) OVER w AS prev_bikes,
    LAG(docks_available) OVER w AS prev_docks
FROM station_status_raw
WHERE is_installed
WINDOW w AS (PARTITION BY station_id ORDER BY event_time);

-- ---------------------------------------------------------------- Job

EXECUTE STATEMENT SET
BEGIN

INSERT INTO pg_stations
SELECT station_id, name, lat, lon, capacity, CAST(TO_TIMESTAMP_LTZ(ingested_at, 3) AS TIMESTAMP(3))
FROM station_information_raw;

INSERT INTO pg_station_status_latest
SELECT
    station_id, bikes_available, docks_available, is_renting, is_returning,
    CAST(TO_TIMESTAMP_LTZ(last_reported, 3) AS TIMESTAMP(3)),
    CAST(TO_TIMESTAMP_LTZ(ingested_at, 3) AS TIMESTAMP(3)),
    (ingested_at - last_reported) / 1000.0
FROM station_status_raw;

INSERT INTO pg_station_availability_5m
SELECT
    station_id, window_start, window_end,
    AVG(CAST(bikes_available AS DOUBLE)), MIN(bikes_available), MAX(bikes_available), COUNT(*)
FROM TABLE(TUMBLE(TABLE station_status_raw, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
GROUP BY station_id, window_start, window_end;

INSERT INTO pg_network_activity_5m
SELECT
    window_start, window_end,
    SUM(CASE WHEN delta_bikes < 0 THEN -delta_bikes ELSE 0 END),
    SUM(CASE WHEN delta_bikes > 0 THEN delta_bikes ELSE 0 END),
    COUNT(DISTINCT station_id)
FROM TABLE(TUMBLE(TABLE station_deltas, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
WHERE delta_bikes IS NOT NULL
GROUP BY window_start, window_end;

INSERT INTO pg_station_alerts
SELECT
    station_id,
    CASE WHEN bikes_available = 0 THEN 'EMPTY' ELSE 'FULL' END,
    CAST(event_time AS TIMESTAMP(3)),
    bikes_available, docks_available
FROM station_deltas
WHERE (bikes_available = 0 AND prev_bikes > 0)
   OR (docks_available = 0 AND prev_docks > 0);

END;
