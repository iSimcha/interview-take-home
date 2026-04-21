"""Smoke tests — run these after `docker compose up -d` to confirm the
database is reachable and the source tables are populated.
"""

from __future__ import annotations

import pytest

from entity_linking.db import connect


EXPECTED_MIN_ROWS = {
    "sources.sec_companies": 150,
    "sources.state_registrations": 200,
    "sources.usaspending_recipients": 90,
}


@pytest.mark.parametrize("table,minimum", list(EXPECTED_MIN_ROWS.items()))
def test_source_tables_populated(table: str, minimum: int) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        (count,) = cur.fetchone()
    assert count >= minimum, f"{table} has {count} rows, expected at least {minimum}"


def test_resolved_tables_exist_and_empty() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM resolved.entities")
        (entities,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM resolved.entity_source_links")
        (links,) = cur.fetchone()
    assert entities == 0, "resolved.entities should start empty — candidate populates it"
    assert links == 0, "resolved.entity_source_links should start empty — candidate populates it"
