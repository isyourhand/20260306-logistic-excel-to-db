from __future__ import annotations

from psycopg import connect


def open_connection(dsn: str):
    return connect(dsn)

