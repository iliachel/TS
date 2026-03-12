"""Ingestion logic for the Open Notify astronauts API.

Flow:
- fetch JSON from the public API with retries and exponential backoff
- insert the raw JSON into ClickHouse (raw_astros)
- optionally OPTIMIZE tables for deduplication
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from app.ch_client import get_client


@dataclass
class IngestResult:
    """Result returned by fetch_and_insert."""

    attempts: int
    inserted_rows: int
    raw_id: int
    inserted_at: str


def _get_env(name: str, default: str) -> str:
    """Read environment variable with fallback."""
    value = os.getenv(name, default)
    if value is None:
        return default
    return value


def _parse_retry_after(value: str | None) -> float | None:
    """Parse Retry-After header (seconds) into float."""
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _hash_id(raw_json: str) -> int:
    """Generate a stable 64-bit ID from raw JSON.

    This is used for deduplication in ReplacingMergeTree.
    """
    digest = hashlib.sha256(raw_json.encode('utf-8')).digest()
    return int.from_bytes(digest[:8], 'big', signed=False)


def fetch_astros(
    url: str, max_attempts: int = 5, base_delay: float = 1.0, timeout: float = 10.0
) -> tuple[dict[str, Any], int]:
    """Fetch astronauts JSON with retry and exponential backoff.

    Returns a tuple of (payload, attempts_used).
    Raises RuntimeError after max_attempts failures.
    """
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = httpx.get(url, timeout=timeout)
            if response.status_code == 200:
                return response.json(), attempt

            last_error = RuntimeError(f'HTTP {response.status_code}: {response.text[:200]}')

            retry_after = _parse_retry_after(response.headers.get('Retry-After'))
            delay = base_delay * (2 ** (attempt - 1))
            if retry_after is not None:
                delay = max(delay, retry_after)
        except httpx.RequestError as exc:
            last_error = exc
            delay = base_delay * (2 ** (attempt - 1))

        time.sleep(delay)

    raise RuntimeError(f'Failed to fetch after {max_attempts} attempts') from last_error


def insert_raw(payload: dict[str, Any], attempts: int) -> IngestResult:
    """Insert raw JSON into ClickHouse and return metadata."""
    client = get_client()
    raw_json = json.dumps(payload, ensure_ascii=False)
    raw_id = _hash_id(raw_json)
    inserted_at = datetime.utcnow()

    client.insert(
        'raw_astros',
        [(raw_id, raw_json, inserted_at)],
        column_names=['id', 'raw_json', '_inserted_at'],
    )

    return IngestResult(
        attempts=attempts,
        inserted_rows=1,
        raw_id=raw_id,
        inserted_at=inserted_at.strftime('%Y-%m-%d %H:%M:%S'),
    )


def optimize_tables() -> None:
    """Run OPTIMIZE FINAL for deduplication."""
    client = get_client()
    client.command('OPTIMIZE TABLE raw_astros FINAL')
    client.command('OPTIMIZE TABLE people FINAL')


def fetch_and_insert() -> IngestResult:
    """Fetch JSON, insert into ClickHouse, optionally optimize."""
    url = _get_env('ASTROS_URL', 'http://api.open-notify.org/astros.json')
    payload, attempts = fetch_astros(url)
    result = insert_raw(payload, attempts)

    if _get_env('RUN_OPTIMIZE', 'false').lower() in {'1', 'true', 'yes'}:
        optimize_tables()

    return result
