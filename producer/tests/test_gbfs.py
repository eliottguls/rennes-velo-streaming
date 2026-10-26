from gbfs import normalize_information, normalize_status


def test_status_gbfs_v2():
    rec = normalize_status(
        {"station_id": 5501, "num_bikes_available": 4, "num_docks_available": 12,
         "is_installed": 1, "is_renting": 1, "is_returning": 0, "last_reported": 1761516000},
        feed_last_updated=1761516030, ingested_at_ms=1,
    )
    assert rec["station_id"] == "5501"
    assert rec["bikes_available"] == 4
    assert rec["docks_available"] == 12
    assert rec["is_returning"] is False
    assert rec["last_reported"] == 1761516000000


def test_status_gbfs_v3():
    rec = normalize_status(
        {"station_id": "a", "num_vehicles_available": 7, "num_docks_available": 3,
         "is_installed": True, "is_renting": True, "is_returning": True,
         "last_reported": "2026-10-26T22:00:00+00:00"},
        feed_last_updated="2026-10-26T22:00:30+00:00", ingested_at_ms=1,
    )
    assert rec["bikes_available"] == 7
    assert rec["last_reported"] == 1793052000000


def test_status_falls_back_to_feed_timestamp():
    rec = normalize_status({"station_id": "x", "num_bikes_available": 0},
                           feed_last_updated=100, ingested_at_ms=1)
    assert rec["last_reported"] == 100000


def test_information_localized_name():
    rec = normalize_information(
        {"station_id": "1", "name": [{"text": "Republique", "language": "fr"}],
         "lat": 48.11, "lon": -1.68, "capacity": 20},
        ingested_at_ms=1,
    )
    assert rec["name"] == "Republique"
    assert rec["capacity"] == 20
