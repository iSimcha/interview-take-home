"""Entry point for the candidate's entity linking pipeline.

The expected end state is that running this module populates
`resolved.entities` and `resolved.entity_source_links` such that every record
in the three source tables is linked to exactly one entity, with duplicates
across sources collapsed together.

This skeleton deliberately leaves the linking logic unimplemented.
"""

from __future__ import annotations

from entity_linking.db import connect


def main() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sources.sec_companies")
        (sec_count,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM sources.state_registrations")
        (state_count,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM sources.usaspending_recipients")
        (usasp_count,) = cur.fetchone()

    print(f"sources.sec_companies:           {sec_count} rows")
    print(f"sources.state_registrations:     {state_count} rows")
    print(f"sources.usaspending_recipients:  {usasp_count} rows")
    print()
    print("Next step: implement your linking pipeline and populate resolved.entities + resolved.entity_source_links.")


if __name__ == "__main__":
    main()
