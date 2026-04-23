"""Entry point for the candidate's entity linking pipeline."""
from __future__ import annotations
import re
import unicodedata
import pandas as pd
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
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_zip(zip_code: str) -> str:
    if not zip_code:
        return ""
    return str(zip_code).strip()[:5]


def normalize_street(street: str) -> str:
    if not street:
        return ""
    street = street.lower().strip()
    street = re.sub(r"[^\w\s]", " ", street)
    for pattern, replacement in STREET_ABBREVS.items():
        street = re.sub(pattern, replacement, street)
    street = re.sub(r"\s+", " ", street).strip()
    return street


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_sources(conn) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sec = pd.read_sql("""
        SELECT cik AS source_id, company_name AS name,
               street, city, state, zip_code AS zip
        FROM sources.sec_companies
    """, conn)
    sec["source"] = "sec"

    state = pd.read_sql("""
        SELECT registration_id AS source_id, entity_name AS name,
               agent_street AS street, agent_city AS city,
               agent_state AS state, agent_zip AS zip
        FROM sources.state_registrations
    """, conn)
    state["source"] = "state"

    usa = pd.read_sql("""
        SELECT uei AS source_id, recipient_name AS name,
               street, city, state, zip_code AS zip
        FROM sources.usaspending_recipients
    """, conn)
    usa["source"] = "usa"

    for df in [sec, state, usa]:
        df["norm_name"] = df["name"].fillna("").apply(normalize_name)
        df["norm_zip"] = df["zip"].fillna("").apply(normalize_zip)
        df["norm_street"] = df["street"].fillna("").apply(normalize_street)
        df["state"] = df["state"].fillna("").str.upper().str.strip()

    return sec, state, usa


def main() -> None:
    with connect() as conn:
        sec, state, usa = load_sources(conn)

    print(f"Loaded {len(sec)} SEC, {len(state)} state, {len(usa)} USASpending records")
    print("Sample normalized SEC names:")
    print(sec[["name", "norm_name"]].head(5).to_string())


if __name__ == "__main__":
    main()