"""PostgreSQL connection helper.

Reads credentials from environment variables (or a local .env file) so the
same code works for the Docker Compose service, CI, and a candidate's own
infrastructure without changes.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg
from dotenv import load_dotenv

load_dotenv()


def connection_string() -> str:
    return (
        f"host={os.environ.get('PGHOST', 'localhost')} "
        f"port={os.environ.get('PGPORT', '5432')} "
        f"user={os.environ.get('PGUSER', 'postgres')} "
        f"password={os.environ.get('PGPASSWORD', 'postgres')} "
        f"dbname={os.environ.get('PGDATABASE', 'postgres')}"
    )


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    with psycopg.connect(connection_string()) as conn:
        yield conn
