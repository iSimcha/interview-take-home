"""Entry point for the entity linking pipeline."""

from __future__ import annotations

from entity_linking.db import connect
from entity_linking.linker import build_resolved_entities, make_source_record


def load_source_records(cur) -> list:
    records = []

    cur.execute(
        """
        SELECT cik, company_name, street, city, state, zip_code
        FROM sources.sec_companies
        ORDER BY cik
        """
    )
    for cik, company_name, street, city, state, zip_code in cur.fetchall():
        records.append(
            make_source_record(
                source_system="sec",
                source_record_id=str(cik),
                name=company_name,
                street=street,
                city=city,
                state=state,
                zip_code=zip_code,
            )
        )

    cur.execute(
        """
        SELECT registration_id, entity_name, agent_street, agent_city, agent_state, agent_zip
        FROM sources.state_registrations
        ORDER BY registration_id
        """
    )
    for registration_id, entity_name, agent_street, agent_city, agent_state, agent_zip in cur.fetchall():
        records.append(
            make_source_record(
                source_system="state",
                source_record_id=registration_id,
                name=entity_name,
                street=agent_street,
                city=agent_city,
                state=agent_state,
                zip_code=agent_zip,
            )
        )

    cur.execute(
        """
        SELECT uei, recipient_name, street, city, state, zip_code
        FROM sources.usaspending_recipients
        ORDER BY uei
        """
    )
    for uei, recipient_name, street, city, state, zip_code in cur.fetchall():
        records.append(
            make_source_record(
                source_system="usaspending",
                source_record_id=uei,
                name=recipient_name,
                street=street,
                city=city,
                state=state,
                zip_code=zip_code,
            )
        )

    return records


def main() -> None:
    with connect() as conn, conn.cursor() as cur:
        records = load_source_records(cur)
        entities, links = build_resolved_entities(records)

        cur.execute("TRUNCATE TABLE resolved.entity_source_links, resolved.entities")

        cur.executemany(
            """
            INSERT INTO resolved.entities (entity_id, canonical_name, notes)
            VALUES (%s, %s, %s)
            """,
            [(entity.entity_id, entity.canonical_name, entity.notes) for entity in entities],
        )

        cur.executemany(
            """
            INSERT INTO resolved.entity_source_links (
                link_id,
                entity_id,
                source_system,
                source_record_id,
                match_score,
                match_method
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    link.link_id,
                    link.entity_id,
                    link.source_system,
                    link.source_record_id,
                    link.match_score,
                    link.match_method,
                )
                for link in links
            ],
        )
        conn.commit()

    sec_count = sum(1 for record in records if record.source_system == "sec")
    state_count = sum(1 for record in records if record.source_system == "state")
    usasp_count = sum(1 for record in records if record.source_system == "usaspending")

    print(f"sources.sec_companies:           {sec_count} rows")
    print(f"sources.state_registrations:     {state_count} rows")
    print(f"sources.usaspending_recipients:  {usasp_count} rows")
    print()
    print(f"resolved.entities:               {len(entities)} rows")
    print(f"resolved.entity_source_links:    {len(links)} rows")


if __name__ == "__main__":
    main()
