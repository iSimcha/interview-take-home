"""Entry point — loads source records, runs the linker, writes results."""
from __future__ import annotations

import psycopg

from entity_linking.db import connect
from entity_linking.linker import build_resolved_entities, make_source_record, SourceRecord


def load_source_records(cur: psycopg.Cursor) -> list[SourceRecord]:
    records: list[SourceRecord] = []

    cur.execute("""
        SELECT cik, company_name, street, city, state, zip_code
        FROM sources.sec_companies ORDER BY cik
    """)
    for cik, name, street, city, state, zip_code in cur.fetchall():
        records.append(make_source_record(
            source_system="sec",
            source_record_id=str(cik),
            name=name or "",
            street=street, city=city, state=state, zip_code=zip_code,
        ))

    cur.execute("""
        SELECT registration_id, entity_name, agent_street, agent_city, agent_state, agent_zip
        FROM sources.state_registrations ORDER BY registration_id
    """)
    for reg_id, name, street, city, state, zip_code in cur.fetchall():
        records.append(make_source_record(
            source_system="state",
            source_record_id=reg_id,
            name=name or "",
            street=street, city=city, state=state, zip_code=zip_code,
        ))

    cur.execute("""
        SELECT uei, recipient_name, parent_name, street, city, state, zip_code
        FROM sources.usaspending_recipients ORDER BY uei
    """)
    for uei, name, parent_name, street, city, state, zip_code in cur.fetchall():
        records.append(make_source_record(
            source_system="usaspending",
            source_record_id=uei,
            name=name or "",
            street=street, city=city, state=state, zip_code=zip_code,
            parent_name=parent_name,
        ))

    return records


def main() -> None:
    with connect() as conn, conn.cursor() as cur:
        print("Loading records...")
        records = load_source_records(cur)
        sec_n   = sum(1 for r in records if r.source_system == "sec")
        state_n = sum(1 for r in records if r.source_system == "state")
        usa_n   = sum(1 for r in records if r.source_system == "usaspending")

        print("Matching and clustering...")
        entities, links = build_resolved_entities(records)

        print("Writing results...")
        cur.execute("TRUNCATE TABLE resolved.entity_source_links, resolved.entities")
        cur.executemany(
            "INSERT INTO resolved.entities (entity_id, canonical_name, notes) VALUES (%s, %s, %s)",
            [(e.entity_id, e.canonical_name, e.notes) for e in entities],
        )
        cur.executemany(
            """INSERT INTO resolved.entity_source_links
               (link_id, entity_id, source_system, source_record_id, match_score, match_method)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            [(l.link_id, l.entity_id, l.source_system,
              l.source_record_id, l.match_score, l.match_method)
             for l in links],
        )
        conn.commit()

    multi = sum(1 for e in entities if e.notes and e.notes.startswith("linked"))
    print(f"\nsources.sec_companies:           {sec_n} rows")
    print(f"sources.state_registrations:     {state_n} rows")
    print(f"sources.usaspending_recipients:  {usa_n} rows")
    print(f"resolved.entities:               {len(entities)} rows")
    print(f"resolved.entity_source_links:    {len(links)} rows")
    print(f"multi-source entities:           {multi}")


if __name__ == "__main__":
    main()