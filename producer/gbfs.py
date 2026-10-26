"""Client GBFS minimal + normalisation des messages (GBFS 2.x et 3.x)."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import requests


def _to_epoch_ms(value: Any) -> int | None:
    """GBFS 2.x donne des timestamps POSIX (s), GBFS 3.x des dates RFC 3339."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value * 1000)
    if isinstance(value, str):
        if value.isdigit():
            return int(value) * 1000
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    return None


def _localized(value: Any) -> str | None:
    """GBFS 3.x : les noms sont des listes [{"text": ..., "language": ...}]."""
    if isinstance(value, list):
        for item in value:
            if item.get("language", "").startswith("fr"):
                return item.get("text")
        return value[0].get("text") if value else None
    return value


def _bool(value: Any) -> bool:
    return bool(int(value)) if isinstance(value, (int, str)) and str(value).isdigit() else bool(value)


def normalize_status(station: dict, feed_last_updated: Any, ingested_at_ms: int) -> dict:
    bikes = station.get("num_bikes_available")
    if bikes is None:  # GBFS 3.x
        bikes = station.get("num_vehicles_available", 0)
    ebikes = None
    for vt in station.get("num_bikes_available_types") or []:
        if isinstance(vt, dict) and "ebike" in vt:  # format 2.x non standard mais fréquent
            ebikes = vt["ebike"]
    return {
        "station_id": str(station["station_id"]),
        "bikes_available": int(bikes),
        "ebikes_available": ebikes,
        "docks_available": int(station.get("num_docks_available") or 0),
        "is_installed": _bool(station.get("is_installed", True)),
        "is_renting": _bool(station.get("is_renting", True)),
        "is_returning": _bool(station.get("is_returning", True)),
        "last_reported": _to_epoch_ms(station.get("last_reported")) or _to_epoch_ms(feed_last_updated),
        "feed_last_updated": _to_epoch_ms(feed_last_updated),
        "ingested_at": ingested_at_ms,
    }


def normalize_information(station: dict, ingested_at_ms: int) -> dict:
    return {
        "station_id": str(station["station_id"]),
        "name": _localized(station.get("name")),
        "lat": float(station["lat"]),
        "lon": float(station["lon"]),
        "capacity": int(station["capacity"]) if station.get("capacity") is not None else None,
        "ingested_at": ingested_at_ms,
    }


class GbfsClient:
    def __init__(self, discovery_url: str, language: str = "fr", timeout: float = 10.0):
        self.discovery_url = discovery_url
        self.language = language
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "rennes-velo-streaming/1.0"
        self.feeds: dict[str, str] = {}

    def _get(self, url: str) -> dict:
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def discover(self) -> dict[str, str]:
        doc = self._get(self.discovery_url)
        data = doc["data"]
        if "feeds" in data:  # GBFS 3.x
            feeds = data["feeds"]
        else:  # GBFS 2.x : feeds groupés par langue
            lang = self.language if self.language in data else next(iter(data))
            feeds = data[lang]["feeds"]
        self.feeds = {f["name"]: f["url"] for f in feeds}
        return self.feeds

    def fetch(self, feed: str) -> dict:
        if not self.feeds:
            self.discover()
        return self._get(self.feeds[feed])

    def station_status(self) -> tuple[Any, list[dict]]:
        doc = self.fetch("station_status")
        now = int(time.time() * 1000)
        updated = doc.get("last_updated")
        return updated, [normalize_status(s, updated, now) for s in doc["data"]["stations"]]

    def station_information(self) -> list[dict]:
        doc = self.fetch("station_information")
        now = int(time.time() * 1000)
        return [normalize_information(s, now) for s in doc["data"]["stations"]]
