"""Tests for backend.event_log.EventLog (debrief / CSV / stats)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from backend.event_log import CSV_COLUMNS, EventLog, parse_since
from backend.models import (
    EncryptionEvent,
    EventType,
    LRRPPositionEvent,
    QualityEvent,
    VoiceCallEvent,
)


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 10, 21, 0, seconds)


def _voice(seconds: int, src: int, tgt: int, slot: int = 1) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_ts(seconds), raw_line="voice", slot=slot, src=src, tgt=tgt,
    )


def test_append_writes_jsonl_and_buffers(tmp_path: Path) -> None:
    log_path = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=log_path, capacity=10)
    log.append(_voice(0, 101, 1))
    log.append(_voice(1, 102, 1))
    log.close()

    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["type"] == "voice_call" and first["src"] == 101


def test_ring_buffer_caps_at_capacity(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=None, capacity=3)
    for i in range(10):
        log.append(_voice(i, 100 + i, 1))
    recent = log.recent(limit=99)
    # Only the last 3 should remain in-memory.
    assert [e.src for e in recent] == [107, 108, 109]


def test_recent_filters_by_type_and_since(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=None, capacity=100)
    log.append(_voice(0, 101, 1))
    log.append(QualityEvent(timestamp=_ts(5), raw_line="x", error_type="CSBK_CRC"))
    log.append(LRRPPositionEvent(
        timestamp=_ts(10), raw_line="x", src=101, lat=32.0, lon=34.0,
    ))
    log.append(_voice(20, 102, 1))

    voices = log.recent(types=["voice_call"])
    assert [e.src for e in voices] == [101, 102]

    after_5 = log.recent(since=_ts(6))
    assert [e.type for e in after_5] == [EventType.LRRP_POSITION, EventType.VOICE_CALL]


def test_iter_csv_emits_header_and_rows() -> None:
    log = EventLog(jsonl_path=None, capacity=100)
    log.append(_voice(0, 101, 9, slot=2))
    log.append(LRRPPositionEvent(
        timestamp=_ts(5), raw_line="pos", src=101, lat=32.123, lon=34.456,
    ))
    log.append(EncryptionEvent(
        timestamp=_ts(10), raw_line="enc", slot=1, flco="0x04", fid="0x80",
    ))

    csv_text = "".join(log.iter_csv())
    lines = csv_text.strip().splitlines()
    assert lines[0] == ",".join(CSV_COLUMNS)
    # 3 events → 3 data rows.
    assert len(lines) == 4
    assert "voice_call" in lines[1]
    assert "32.123" in lines[2]
    assert "true" in lines[3]  # encrypted column


def test_iter_csv_respects_type_filter() -> None:
    log = EventLog(jsonl_path=None, capacity=100)
    log.append(_voice(0, 101, 1))
    log.append(QualityEvent(timestamp=_ts(1), raw_line="x", error_type="CSBK_CRC"))
    csv_text = "".join(log.iter_csv(types=["quality"]))
    lines = csv_text.strip().splitlines()
    assert len(lines) == 2  # header + 1 quality row
    assert "CSBK_CRC" in lines[1]


def test_stats_counts_distinct_calls_and_distributions() -> None:
    log = EventLog(jsonl_path=None, capacity=100)
    # Two distinct calls on slot 1, plus a repeated voice frame that should not double-count.
    log.append(_voice(0, 101, 9, slot=1))
    log.append(_voice(1, 101, 9, slot=1))  # same call — should not increment
    log.append(_voice(5, 102, 9, slot=1))
    log.append(_voice(7, 101, 9, slot=2))  # different slot — new call
    log.append(LRRPPositionEvent(
        timestamp=_ts(10), raw_line="p", src=101, lat=32.0, lon=34.0,
    ))
    log.append(EncryptionEvent(
        timestamp=_ts(12), raw_line="e", slot=1, flco="0x04", fid="0x80",
    ))
    log.append(QualityEvent(timestamp=_ts(15), raw_line="q", error_type="CSBK_CRC"))

    s = log.stats()
    assert s["window_size"] == 7
    assert s["events_by_type"]["voice_call"] == 4
    assert s["calls_by_src"][101] == 2  # one on slot 1, one on slot 2
    assert s["calls_by_src"][102] == 1
    assert s["calls_by_tg"][9] == 3
    assert s["positions_by_src"][101] == 1
    assert s["encrypted_calls"] == 1
    assert s["quality_by_kind"]["CSBK_CRC"] == 1


def test_voice_src_zero_is_excluded_from_call_stats() -> None:
    log = EventLog(jsonl_path=None, capacity=10)
    log.append(_voice(0, 0, 9))  # placeholder pre-LC SRC
    log.append(_voice(1, 101, 9))
    s = log.stats()
    assert 0 not in s["calls_by_src"]
    assert s["calls_by_src"][101] == 1


def test_stream_history_filters_by_time_type_and_radio(tmp_path: Path) -> None:
    log_path = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=log_path, capacity=100)
    log.append(_voice(0, 101, 9))
    log.append(_voice(5, 102, 9))
    log.append(LRRPPositionEvent(
        timestamp=_ts(10), raw_line="pos", src=101, lat=32.0, lon=34.0,
    ))
    log.append(QualityEvent(timestamp=_ts(15), raw_line="q", error_type="CSBK_CRC"))
    log.append(_voice(20, 101, 7))
    log.close()

    from backend.event_log import stream_history

    # No filter — yields every line.
    all_evs = list(stream_history(log_path))
    assert len(all_evs) == 5

    # Filter by type.
    voices = list(stream_history(log_path, types=["voice_call"]))
    assert [e["src"] for e in voices] == [101, 102, 101]

    # Filter by SRC radio.
    by_radio = list(stream_history(log_path, src=101))
    # voice(0,101,9), lrrp_position(src=101), voice(20,101,7)
    assert len(by_radio) == 3

    # Filter by target talkgroup.
    by_tg = list(stream_history(log_path, tgt=9))
    assert len(by_tg) == 2

    # Filter by time window — only events within [_ts(7), _ts(16)].
    in_window = list(stream_history(log_path, since=_ts(7), until=_ts(16)))
    assert [e["type"] for e in in_window] == ["lrrp_position", "quality"]


def test_stream_history_skips_malformed_lines(tmp_path: Path) -> None:
    log_path = tmp_path / "events.jsonl"
    # Hand-craft a file with garbage interleaved with valid lines.
    log_path.write_text(
        '{"timestamp":"2026-01-01T00:00:00","type":"quality","raw_line":"x","error_type":"CSBK_CRC"}\n'
        'this is not json at all\n'
        '\n'
        '{"timestamp":"bad","type":"quality","raw_line":"x","error_type":"X"}\n'
        '{"timestamp":"2026-01-01T00:00:01","type":"quality","raw_line":"x","error_type":"SLCO_CRC"}\n',
        encoding="utf-8",
    )
    from backend.event_log import stream_history
    out = list(stream_history(log_path))
    # Only the two well-formed events with parseable timestamps survive.
    assert [o["error_type"] for o in out] == ["CSBK_CRC", "SLCO_CRC"]


def test_iter_history_csv_emits_header_and_filtered_rows(tmp_path: Path) -> None:
    log_path = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=log_path, capacity=100)
    log.append(_voice(0, 101, 9))
    log.append(QualityEvent(timestamp=_ts(5), raw_line="x", error_type="CSBK_CRC"))
    log.close()

    from backend.event_log import iter_history_csv

    csv_text = "".join(iter_history_csv(log_path, types=["quality"]))
    lines = csv_text.strip().splitlines()
    assert len(lines) == 2  # header + 1 quality row
    assert lines[0].startswith("timestamp,type")
    assert "CSBK_CRC" in lines[1]


def test_parse_since_accepts_duration_and_iso() -> None:
    assert parse_since(None) is None
    assert parse_since("") is None
    assert parse_since("garbage") is None
    # Duration parses to approximately "now - delta".
    cutoff = parse_since("5m")
    assert cutoff is not None
    delta = datetime.now() - cutoff
    assert timedelta(minutes=4, seconds=55) < delta < timedelta(minutes=5, seconds=5)
    # ISO timestamp passes through.
    iso = parse_since("2026-05-10T21:00:00")
    assert iso == datetime(2026, 5, 10, 21, 0, 0)
