"""Interroge le flux GTFS-RT des bus/métro STAR et publie chaque nouvelle position dans Kafka."""

from __future__ import annotations

import json
import logging
import os
import signal
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from gtfsrt import parse_routes, parse_vehicle_positions

log = logging.getLogger("bus-producer")

VP_URL = os.getenv(
    "GTFSRT_VEHICLE_POSITIONS_URL",
    "https://proxy.transport.data.gouv.fr/resource/star-rennes-integration-gtfs-rt-vehicle-position",
)
GTFS_STATIC_URL = os.getenv(
    "GTFS_STATIC_URL", "https://eu.ftp.opendatasoft.com/star/gtfs/GTFS_STAR_BUS_METRO_EN_COURS.zip"
)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
POSITIONS_TOPIC = os.getenv("POSITIONS_TOPIC", "bus.vehicle_positions")
ROUTES_TOPIC = os.getenv("ROUTES_TOPIC", "bus.routes")
POLL_SECONDS = float(os.getenv("BUS_POLL_SECONDS", "5"))
ROUTES_REFRESH_SECONDS = float(os.getenv("ROUTES_REFRESH_SECONDS", str(24 * 3600)))

running = True


def stop(*_):
    global running
    running = False


def ensure_topics(admin: AdminClient) -> None:
    topics = [
        NewTopic(POSITIONS_TOPIC, num_partitions=3, replication_factor=1,
                 config={"retention.ms": str(3 * 24 * 3600 * 1000)}),
        NewTopic(ROUTES_TOPIC, num_partitions=1, replication_factor=1,
                 config={"cleanup.policy": "compact"}),
    ]
    for topic, fut in admin.create_topics(topics).items():
        try:
            fut.result()
            log.info("topic %s créé", topic)
        except Exception as exc:
            log.debug("topic %s : %s", topic, exc)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    ensure_topics(AdminClient({"bootstrap.servers": BOOTSTRAP}))
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20,
                         "compression.type": "gzip", "enable.idempotence": True})
    session = requests.Session()
    session.headers["User-Agent"] = "rennes-velo-streaming/1.0"

    last_vehicle_ts: dict[str, int] = {}
    last_feed_ts = 0
    last_routes = 0.0

    while running:
        started = time.monotonic()
        now_ms = int(time.time() * 1000)
        try:
            if not last_routes or started - last_routes > ROUTES_REFRESH_SECONDS:
                resp = session.get(GTFS_STATIC_URL, timeout=60)
                resp.raise_for_status()
                routes = parse_routes(resp.content, now_ms)
                for r in routes:
                    producer.produce(ROUTES_TOPIC, key=r["route_id"], value=json.dumps(r))
                producer.flush(10)
                last_routes = started
                log.info("%d lignes publiées sur %s", len(routes), ROUTES_TOPIC)

            resp = session.get(VP_URL, timeout=10)
            resp.raise_for_status()
            feed_ts, positions = parse_vehicle_positions(resp.content, now_ms)
            if feed_ts != last_feed_ts:
                # Cadence réelle de la source, utile pour qualifier le "temps réel"
                if last_feed_ts:
                    log.info("nouveau flux, %.1fs après le précédent", (feed_ts - last_feed_ts) / 1000)
                last_feed_ts = feed_ts
                fresh = [p for p in positions if last_vehicle_ts.get(p["vehicle_id"]) != p["position_ts"]]
                for p in fresh:
                    producer.produce(POSITIONS_TOPIC, key=p["vehicle_id"], value=json.dumps(p))
                producer.flush(10)
                last_vehicle_ts.update({p["vehicle_id"]: p["position_ts"] for p in fresh})
                log.info("%d/%d positions publiées", len(fresh), len(positions))
        except Exception:
            log.exception("échec de la collecte GTFS-RT")

        while running and time.monotonic() - started < POLL_SECONDS:
            time.sleep(0.2)

    producer.flush(10)


if __name__ == "__main__":
    main()
