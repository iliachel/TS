from __future__ import annotations

import httpx

import app.ingest as ingest


def test_hash_id_is_deterministic() -> None:
    payload = '{"k":"v"}'
    assert ingest._hash_id(payload) == ingest._hash_id(payload)


def test_fetch_astros_success_first_try(monkeypatch) -> None:
    def fake_get(url: str, timeout: float):
        return httpx.Response(200, json={'ok': True})

    monkeypatch.setattr(ingest.httpx, 'get', fake_get)

    payload, attempts = ingest.fetch_astros('http://example.test')
    assert payload == {'ok': True}
    assert attempts == 1


def test_fetch_astros_retries_and_uses_retry_after(monkeypatch) -> None:
    calls = {'count': 0}
    sleeps: list[float] = []

    def fake_get(url: str, timeout: float):
        calls['count'] += 1
        if calls['count'] < 3:
            headers = {'Retry-After': '2'}
            return httpx.Response(429, headers=headers, text='Too Many Requests')
        return httpx.Response(200, json={'ok': True})

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(ingest.httpx, 'get', fake_get)
    monkeypatch.setattr(ingest.time, 'sleep', fake_sleep)

    payload, attempts = ingest.fetch_astros('http://example.test', base_delay=1.0)

    assert payload == {'ok': True}
    assert attempts == 3
    # Should respect Retry-After (>=2) and exponential backoff
    assert sleeps[0] >= 2.0
    assert sleeps[1] >= 2.0


def test_fetch_astros_fails_after_max_attempts(monkeypatch) -> None:
    def fake_get(url: str, timeout: float):
        return httpx.Response(500, text='Server Error')

    def fake_sleep(seconds: float) -> None:
        pass

    monkeypatch.setattr(ingest.httpx, 'get', fake_get)
    monkeypatch.setattr(ingest.time, 'sleep', fake_sleep)

    try:
        ingest.fetch_astros('http://example.test', max_attempts=3)
    except RuntimeError as exc:
        assert 'Failed to fetch' in str(exc)
    else:
        assert False, 'Expected RuntimeError'
