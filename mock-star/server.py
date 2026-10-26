"""Faux flux STAR (GBFS vélos + GTFS-RT bus) pour développer sans réseau.

- GBFS 2.3 : 40 stations autour du centre de Rennes, stock en marche aléatoire.
- GTFS-RT : 30 bus qui avancent en continu sur 6 lignes, positions toutes les 5 s.
- GTFS statique : un zip minimal avec routes.txt.
"""

from __future__ import annotations

import io
import json
import math
import os
import random
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from google.transit import gtfs_realtime_pb2

BASE_URL = os.getenv("BASE_URL", "http://mock-star:8000")
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "20"))
BUS_TICK_SECONDS = float(os.getenv("BUS_TICK_SECONDS", "5"))
CENTER = (48.1113, -1.6800)

rng = random.Random(35)
stations = []
for i in range(40):
    angle, dist = rng.random() * 2 * math.pi, rng.random() * 0.025
    capacity = rng.choice([12, 16, 20, 24, 30])
    stations.append({
        "station_id": str(5501 + i),
        "name": f"Station simulée {i + 1:02d}",
        "lat": round(CENTER[0] + dist * math.sin(angle), 6),
        "lon": round(CENTER[1] + dist * 1.5 * math.cos(angle), 6),
        "capacity": capacity,
        "bikes": rng.randint(0, capacity),
        "last_reported": int(time.time()),
    })
lock = threading.Lock()

ROUTES = [("0001", "C1", "E6007E"), ("0002", "C2", "009EE0"), ("0003", "C3", "E2001A"),
          ("0004", "C4", "00A650"), ("0005", "C5", "F39200"), ("0006", "C6", "6F2282")]
buses = []
for i in range(30):
    route_id, _, _ = ROUTES[i % len(ROUTES)]
    buses.append({
        "id": f"bus-{100 + i}", "route_id": route_id, "direction": i % 2,
        "lat": CENTER[0] + rng.uniform(-0.02, 0.02), "lon": CENTER[1] + rng.uniform(-0.03, 0.03),
        "bearing": rng.uniform(0, 360), "speed": rng.uniform(3, 12), "ts": int(time.time()),
    })
bus_feed_ts = int(time.time())


def bus_tick() -> None:
    global bus_feed_ts
    while True:
        time.sleep(BUS_TICK_SECONDS)
        now = int(time.time())
        with lock:
            for b in buses:
                b["bearing"] = (b["bearing"] + rng.uniform(-25, 25)) % 360
                b["speed"] = max(0.0, min(14.0, b["speed"] + rng.uniform(-3, 3)))
                dist = b["speed"] * BUS_TICK_SECONDS
                b["lat"] += dist * math.cos(math.radians(b["bearing"])) / 110540
                b["lon"] += dist * math.sin(math.radians(b["bearing"])) / (111320 * math.cos(math.radians(b["lat"])))
                # reste autour de Rennes
                if abs(b["lat"] - CENTER[0]) > 0.03 or abs(b["lon"] - CENTER[1]) > 0.045:
                    b["bearing"] = (b["bearing"] + 180) % 360
                b["ts"] = now
            bus_feed_ts = now


def vehicle_positions() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    with lock:
        feed.header.timestamp = bus_feed_ts
        for b in buses:
            v = feed.entity.add(id=b["id"]).vehicle
            v.vehicle.id = b["id"]
            v.trip.trip_id = f"{b['route_id']}-{b['direction']}"
            v.trip.route_id = b["route_id"]
            v.trip.direction_id = b["direction"]
            v.position.latitude = b["lat"]
            v.position.longitude = b["lon"]
            v.position.bearing = b["bearing"]
            v.timestamp = b["ts"]
    return feed.SerializeToString()


def static_gtfs() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        rows = "".join(f"{rid},{short},Ligne {short} (simulée),3,{color}\n" for rid, short, color in ROUTES)
        zf.writestr("routes.txt", "route_id,route_short_name,route_long_name,route_type,route_color\n" + rows)
    return buf.getvalue()


def tick() -> None:
    while True:
        time.sleep(TICK_SECONDS)
        now = int(time.time())
        with lock:
            for s in stations:
                if rng.random() < 0.4:
                    s["bikes"] = max(0, min(s["capacity"], s["bikes"] + rng.choice([-2, -1, -1, 1, 1, 2])))
                    s["last_reported"] = now


def feeds() -> dict:
    now = int(time.time())
    with lock:
        return {
            "gbfs.json": {"data": {"fr": {"feeds": [
                {"name": "station_information", "url": f"{BASE_URL}/station_information.json"},
                {"name": "station_status", "url": f"{BASE_URL}/station_status.json"},
            ]}}},
            "station_information.json": {"data": {"stations": [
                {k: s[k] for k in ("station_id", "name", "lat", "lon", "capacity")} for s in stations
            ]}},
            "station_status.json": {"data": {"stations": [{
                "station_id": s["station_id"],
                "num_bikes_available": s["bikes"],
                "num_docks_available": s["capacity"] - s["bikes"],
                "is_installed": 1, "is_renting": 1, "is_returning": 1,
                "last_reported": s["last_reported"],
            } for s in stations]}},
        }, now


class Handler(BaseHTTPRequestHandler):
    def _send(self, body: bytes, content_type: str):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/gtfs-rt/vehicle-positions":
            return self._send(vehicle_positions(), "application/x-protobuf")
        if self.path == "/gtfs/static.zip":
            return self._send(static_gtfs(), "application/zip")
        docs, now = feeds()
        doc = docs.get(self.path.lstrip("/"))
        if doc is None:
            self.send_error(404)
            return
        body = json.dumps({"last_updated": now, "ttl": 30, "version": "2.3", **doc}).encode()
        self._send(body, "application/json")

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    threading.Thread(target=tick, daemon=True).start()
    threading.Thread(target=bus_tick, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
