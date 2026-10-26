"""Interroge le flux GBFS du STAR et publie chaque changement dans Kafka/Redpanda."""

from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from gbfs import GbfsClient

log = logging.getLogger("producer")

GBFS_URL = os.getenv("GBFS_URL", "https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
STATUS_TOPIC = os.getenv("STATUS_TOPIC", "velo.station_status")
INFO_TOPIC = os.getenv("INFO_TOPIC", "velo.station_information")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "30"))
INFO_REFRESH_SECONDS = float(os.getenv("INFO_REFRESH_SECONDS", "3600"))

running = True


def stop(*_):
    global running
    running = False


def ensure_topics(admin: AdminClient) -> None:
    topics = [
        NewTopic(STATUS_TOPIC, num_partitions=3, replication_factor=1,
                 config={"retention.ms": str(7 * 24 * 3600 * 1000)}),
        NewTopic(INFO_TOPIC, num_partitions=1, replication_factor=1,
                 config={"cleanup.policy": "compact"}),
    ]
    for topic, fut in admin.create_topics(topics).items():
        try:
            fut.result()
            log.info("topic %s créé", topic)
        except Exception as exc:  # déjà existant
            log.debug("topic %s : %s", topic, exc)


def publish(producer: Producer, topic: str, records: list[dict]) -> None:
    for rec in records:
        producer.produce(topic, key=rec["station_id"], value=json.dumps(rec))
    producer.flush(10)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    ensure_topics(AdminClient({"bootstrap.servers": BOOTSTRAP}))
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50,
                         "compression.type": "gzip", "enable.idempotence": True})
    client = GbfsClient(GBFS_URL)

    last_seen: dict[str, int] = {}  # station_id -> last_reported, pour ne publier que les changements
    last_info = 0.0
    backoff = POLL_SECONDS

    while running:
        started = time.monotonic()
        try:
            if started - last_info > INFO_REFRESH_SECONDS or not last_info:
                client.discover()
                info = client.station_information()
                publish(producer, INFO_TOPIC, info)
                last_info = started
                log.info("%d stations publiées sur %s", len(info), INFO_TOPIC)

            _, statuses = client.station_status()
            changed = [s for s in statuses if last_seen.get(s["station_id"]) != s["last_reported"]]
            publish(producer, STATUS_TOPIC, changed)
            last_seen.update({s["station_id"]: s["last_reported"] for s in changed})
            log.info("%d/%d statuts modifiés publiés", len(changed), len(statuses))
            backoff = POLL_SECONDS
        except Exception:
            log.exception("échec de la collecte, nouvel essai dans %.0fs", backoff)
            client.feeds = {}
            backoff = min(backoff * 2, 300)

        while running and time.monotonic() - started < backoff:
            time.sleep(0.5)

    producer.flush(10)


if __name__ == "__main__":
    main()
