"""ClickHouse client helper.

This module centralizes connection settings for ClickHouse and returns a
preconfigured client instance. Values are taken from environment variables
so the same code works in local runs and Docker.
"""

from __future__ import annotations

import os

import clickhouse_connect


def get_client():
    """Create a ClickHouse client using environment variables.

    Env vars:
    - CLICKHOUSE_HOST
    - CLICKHOUSE_PORT
    - CLICKHOUSE_DATABASE
    - CLICKHOUSE_USER
    - CLICKHOUSE_PASSWORD
    """
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    database = os.getenv('CLICKHOUSE_DATABASE', 'default')
    username = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD', '')

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
