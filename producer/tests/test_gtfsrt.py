import io
import zipfile

from google.transit import gtfs_realtime_pb2

from gtfsrt import parse_routes, parse_vehicle_positions


def _feed() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1793052000
    e = feed.entity.add(id="e1")
    e.vehicle.vehicle.id = "bus-42"
    e.vehicle.trip.route_id = "0001"
    e.vehicle.trip.direction_id = 1
    e.vehicle.position.latitude = 48.11
    e.vehicle.position.longitude = -1.68
    e.vehicle.position.speed = 8.5
    e.vehicle.timestamp = 1793051995
    e.vehicle.current_status = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
    feed.entity.add(id="sans-position").vehicle.vehicle.id = "x"
    return feed.SerializeToString()


def test_parse_vehicle_positions():
    feed_ts, positions = parse_vehicle_positions(_feed(), ingested_at_ms=1)
    assert feed_ts == 1793052000000
    assert len(positions) == 1
    p = positions[0]
    assert p["vehicle_id"] == "bus-42"
    assert p["route_id"] == "0001"
    assert p["direction_id"] == 1
    assert abs(p["speed_ms"] - 8.5) < 1e-6
    assert p["position_ts"] == 1793051995000
    assert p["current_status"] == "STOPPED_AT"
    assert p["bearing"] is None


def test_parse_routes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("routes.txt", "﻿route_id,route_short_name,route_long_name,route_type,route_color\n"
                                  "0001,C1,Chantepie <> Saint-Grégoire,3,E6007E\n")
    routes = parse_routes(buf.getvalue(), ingested_at_ms=1)
    assert routes == [{"route_id": "0001", "short_name": "C1", "long_name": "Chantepie <> Saint-Grégoire",
                       "route_type": 3, "color": "#E6007E", "ingested_at": 1}]
