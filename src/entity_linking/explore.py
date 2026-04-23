"""Quick data exploration - run this to understand the data before coding."""
from entity_linking.db import connect

def explore():
    with connect() as conn, conn.cursor() as cur:
        # SEC companies
        cur.execute("SELECT cik, company_name, street, city, state, zip_code FROM sources.sec_companies ORDER BY company_name LIMIT 10")
        print("=== SEC COMPANIES (sample) ===")
        for row in cur.fetchall():
            print(row)

        # State registrations
        print("\n=== STATE REGISTRATIONS (sample) ===")
        cur.execute("SELECT registration_id, jurisdiction, entity_name, agent_street, agent_city, agent_state, agent_zip FROM sources.state_registrations ORDER BY entity_name LIMIT 10")
        for row in cur.fetchall():
            print(row)

        # USAspending
        print("\n=== USASPENDING RECIPIENTS (sample) ===")
        cur.execute("SELECT uei, recipient_name, parent_name, street, city, state, zip_code FROM sources.usaspending_recipients ORDER BY recipient_name LIMIT 10")
        for row in cur.fetchall():
            print(row)

        # Check a known match across sources
        print("\n=== LOOKING FOR 'ACME' ACROSS ALL SOURCES ===")
        cur.execute("SELECT 'sec' as src, cik::text as id, company_name as name, city, state FROM sources.sec_companies WHERE company_name ILIKE '%acme%'")
        for row in cur.fetchall():
            print(row)
        cur.execute("SELECT 'state' as src, registration_id as id, entity_name as name, agent_city, agent_state FROM sources.state_registrations WHERE entity_name ILIKE '%acme%'")
        for row in cur.fetchall():
            print(row)
        cur.execute("SELECT 'usasp' as src, uei as id, recipient_name as name, city, state FROM sources.usaspending_recipients WHERE recipient_name ILIKE '%acme%'")
        for row in cur.fetchall():
            print(row)

if __name__ == "__main__":
    explore()