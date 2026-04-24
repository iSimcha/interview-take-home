"""Entry point for the candidate's entity linking pipeline.

The expected end state is that running this module populates
`resolved.entities` and `resolved.entity_source_links` such that every record
in the three source tables is linked to exactly one entity, with duplicates
across sources collapsed together.

This skeleton deliberately leaves the linking logic unimplemented.
"""

from __future__ import annotations

from entity_linking.db import connect
from entity_linking.pipeline import run_pipeline
import pandas as pd
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

def main() -> None:    
    with connect() as conn:
        sec_df = pd.read_sql("""SELECT cik, company_name, street, city, state, zip_code 
                            FROM sources.sec_companies
                            """, conn)

        state_df = pd.read_sql("""SELECT registration_id, entity_name, agent_street, agent_city, agent_state, agent_zip
                               FROM sources.state_registrations
                               """, conn)

        usa_df = pd.read_sql("""SELECT uei, recipient_name, parent_name, street, city, state, zip_code
                            FROM sources.usaspending_recipients
                            """, conn)

        run_pipeline(conn, sec_df, state_df, usa_df)
    print("Entity linking pipeline completed.")

if __name__ == "__main__":
    main()
