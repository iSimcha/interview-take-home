"""Entry point for the entity linking pipeline."""
from __future__ import annotations
import re
import unicodedata
import pandas as pd
from rapidfuzz import fuzz
from entity_linking.db import connect


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

LEGAL_SUFFIXES = {
    r"\binc\.?$": "inc",
    r"\bincorporated$": "inc",
    r"\bllc\.?$": "llc",
    r"\bl\.l\.c\.?$": "llc",
    r"\bcorp\.?$": "corp",
    r"\bcorporation$": "corp",
    r"\bltd\.?$": "ltd",
    r"\blimited$": "ltd",
    r"\bco\.?$": "co",
    r"\bcompany$": "co",
}

STREET_ABBREVS = {
    r"\bstreet\b": "st",
    r"\bavenue\b": "ave",
    r"\bboulevard\b": "blvd",
    r"\bdrive\b": "dr",
    r"\bcourt\b": "ct",
    r"\bplace\b": "pl",
    r"\broad\b": "rd",
    r"\blane\b": "ln",
    r"\bparkway\b": "pkwy",
    r"\bsuite\b": "ste",
}


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    name = re.sub(r"[&]", "and", name)
    name = re.sub(r"[^\w\s]", " ", name)
    for pattern, replacement in LEGAL_SUFFIXES.items():
        name = re.sub(pattern, replacement, name)
    return re.sub(r"\s+", " ", name).strip()


def normalize_zip(zip_code: str) -> str:
    return str(zip_code).strip()[:5] if zip_code else ""


def normalize_street(street: str) -> str:
    if not street:
        return ""
    street = street.lower().strip()
    street = re.sub(r"[^\w\s]", " ", street)
    for pattern, replacement in STREET_ABBREVS.items():
        street = re.sub(pattern, replacement, street)
    return re.sub(r"\s+", " ", street).strip()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_sources(conn) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cik::text AS source_id, company_name AS name,
                   street, city, state, zip_code AS zip
            FROM sources.sec_companies
        """)
        sec = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        sec["source"] = "sec"

        cur.execute("""
            SELECT registration_id AS source_id, entity_name AS name,
                   agent_street AS street, agent_city AS city,
                   agent_state AS state, agent_zip AS zip
            FROM sources.state_registrations
        """)
        state = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        state["source"] = "state"

        cur.execute("""
            SELECT uei AS source_id, recipient_name AS name,
                   street, city, state, zip_code AS zip
            FROM sources.usaspending_recipients
        """)
        usa = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        usa["source"] = "usa"

    for df in [sec, state, usa]:
        df["norm_name"] = df["name"].fillna("").apply(normalize_name)
        df["norm_zip"] = df["zip"].fillna("").apply(normalize_zip)
        df["norm_street"] = df["street"].fillna("").apply(normalize_street)
        df["state"] = df["state"].fillna("").str.upper().str.strip()

    return sec, state, usa


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_pair(row_a: pd.Series, row_b: pd.Series) -> tuple[float, str]:
    name_score = fuzz.token_sort_ratio(row_a["norm_name"], row_b["norm_name"]) / 100.0
    street_score = fuzz.token_sort_ratio(row_a["norm_street"], row_b["norm_street"]) / 100.0
    zip_score = 1.0 if row_a["norm_zip"] and row_a["norm_zip"] == row_b["norm_zip"] else 0.0

    combined = (name_score * 0.6) + (street_score * 0.25) + (zip_score * 0.15)

    if combined >= 0.85:
        method = "name_address_high"
    elif combined >= 0.60:
        method = "name_address_fuzzy"
    else:
        method = "no_match"

    return round(combined, 4), method


# ---------------------------------------------------------------------------
# Blocking + matching
# ---------------------------------------------------------------------------

def find_matches(all_records: pd.DataFrame) -> list[dict]:
    """Block by state, then score all pairs within each block."""
    matches = []
    for state, group in all_records.groupby("state"):
        records = group.reset_index(drop=True)
        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                a = records.iloc[i]
                b = records.iloc[j]
                score, method = score_pair(a, b)
                if score >= 0.60:
                    matches.append({
                        "source_a": a["source"],
                        "id_a": a["source_id"],
                        "source_b": b["source"],
                        "id_b": b["source_id"],
                        "score": score,
                        "method": method,
                    })
    return matches


# ---------------------------------------------------------------------------
# Write results to DB
# ---------------------------------------------------------------------------

def write_results(conn, all_records: pd.DataFrame, matches: list[dict]) -> None:
    # Build clusters using union-find
    parent = {(r["source"], r["source_id"]): (r["source"], r["source_id"])
              for _, r in all_records.iterrows()}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    for m in matches:
        if m["method"] != "no_match":
            union((m["source_a"], m["id_a"]), (m["source_b"], m["id_b"]))

    # Group records by cluster
    clusters: dict = {}
    for _, row in all_records.iterrows():
        key = (row["source"], row["source_id"])
        root = find(key)
        clusters.setdefault(root, []).append(row)

    with conn.cursor() as cur:
        cur.execute("TRUNCATE resolved.entity_source_links, resolved.entities RESTART IDENTITY CASCADE")

        for root, rows in clusters.items():
            best = max(rows, key=lambda r: len(r["norm_name"]))
            cur.execute("""
                INSERT INTO resolved.entities (canonical_name)
                VALUES (%s)
                RETURNING entity_id
            """, (best["name"],))
            entity_id = cur.fetchone()[0]

            for row in rows:
                score = 1.0 if len(rows) == 1 else 0.75
                method = "singleton" if len(rows) == 1 else "clustered"
                source_system = "usaspending" if row["source"] == "usa" else row["source"]
                cur.execute("""
                    INSERT INTO resolved.entity_source_links
                        (entity_id, source_system, source_record_id, match_score, match_method)
                    VALUES (%s, %s, %s, %s, %s)
                """, (entity_id, source_system, row["source_id"], score, method))

    conn.commit()
    print(f"Wrote {len(clusters)} entities and {len(all_records)} source links")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    with connect() as conn:
        sec, state, usa = load_sources(conn)
        all_records = pd.concat([sec, state, usa], ignore_index=True)
        print(f"Loaded {len(sec)} SEC, {len(state)} state, {len(usa)} USASpending records")

        print("Finding matches...")
        matches = find_matches(all_records)
        print(f"Found {len(matches)} candidate pairs")

        print("Writing results...")
        write_results(conn, all_records, matches)


if __name__ == "__main__":
    main()