"""Lecture du flux GTFS-RT VehiclePosition et du GTFS statique (routes.txt)."""

from __future__ import annotations

import csv
import io
import zipfile

from google.transit import gtfs_realtime_pb2

STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}


def parse_vehicle_positions(payload: bytes, ingested_at_ms: int) -> tuple[int, list[dict]]:
    """Retourne (timestamp du flux en ms, positions normalisées)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    feed_ts = feed.header.timestamp * 1000 if feed.header.HasField("timestamp") else ingested_at_ms

    positions = []
    for entity in feed.entity:
        if not entity.HasField("vehicle") or not entity.vehicle.HasField("position"):
            continue
        v = entity.vehicle
        vehicle_id = v.vehicle.id or v.vehicle.label or entity.id
        positions.append({
            "vehicle_id": vehicle_id,
            "vehicle_label": v.vehicle.label or None,
            "trip_id": v.trip.trip_id or None,
            "route_id": v.trip.route_id or None,
            "direction_id": v.trip.direction_id if v.trip.HasField("direction_id") else None,
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "bearing": v.position.bearing if v.position.HasField("bearing") else None,
            "speed_ms": v.position.speed if v.position.HasField("speed") else None,
            "stop_id": v.stop_id or None,
            "current_status": STATUS.get(v.current_status) if v.HasField("current_status") else None,
            "position_ts": v.timestamp * 1000 if v.HasField("timestamp") else feed_ts,
            "feed_ts": feed_ts,
            "ingested_at": ingested_at_ms,
        })
    return feed_ts, positions


def parse_routes(gtfs_zip: bytes, ingested_at_ms: int) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(gtfs_zip)) as zf:
        with zf.open("routes.txt") as fh:
            reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig"))
            return [{
                "route_id": r["route_id"],
                "short_name": r.get("route_short_name") or None,
                "long_name": r.get("route_long_name") or None,
                "route_type": int(r["route_type"]) if r.get("route_type") else None,
                "color": f"#{r['route_color']}" if r.get("route_color") else None,
                "ingested_at": ingested_at_ms,
            } for r in reader]
