"""Entity linking pipeline.
Populates resolved.entities and resolved.entity_source_links
by matching records across three source tables.
"""
from __future__ import annotations
import re
import uuid
from entity_linking.db import connect
from rapidfuzz import fuzz


# --- Name and address normalization ---

LEGAL_SUFFIXES = [
    r'\bd/?b/?a\b.*$',
    r'\(formerly[^)]*\)',
    r',?\s*(incorporated|corporation|company|cooperative)',
    r',?\s*\b(l\.?l\.?c\.?|l\.?p\.?|l\.?l\.?p\.?|p\.?c\.?|inc\.?|corp\.?|ltd\.?|llc\.?)\s*$',
    r',?\s+co\.?\s*$',
    r',?\s*a cooperative$',
]

# Roman numeral pattern for detecting numbered entities like "Realty II" vs "Realty III"
ROMAN_NUMERAL = re.compile(r'\b(ii|iii|iv|v|vi|vii|viii|ix|x|\d+)\s*$')


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower().strip()
    n = n.replace("&", "and").replace(".", "").replace(",", "")
    for pattern in LEGAL_SUFFIXES:
        n = re.sub(pattern, "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def extract_name_number(norm_name: str) -> tuple[str, str]:
    """Split a normalized name into base name and trailing number/roman numeral."""
    m = ROMAN_NUMERAL.search(norm_name)
    if m:
        base = norm_name[:m.start()].strip()
        num = m.group(1)
        return base, num
    return norm_name, ""


def normalize_zip(z: str) -> str:
    if not z:
        return ""
    return z.strip().split("-")[0][:5]


def normalize_street(street: str) -> str:
    if not street:
        return ""
    s = street.lower().strip()
    s = re.sub(r',?\s*(suite|ste|floor|fl|bldg|building)\s*[#]?\s*\w+', '', s, flags=re.IGNORECASE)
    for full, abbr in {
        " boulevard": " blvd", " drive": " dr", " street": " st",
        " avenue": " ave", " road": " rd", " lane": " ln",
        " place": " pl", " court": " ct", " parkway": " pkwy",
    }.items():
        s = s.replace(full, abbr)
    s = s.replace(".", "")
    return re.sub(r"\s+", " ", s).strip()


# --- Load records from all three sources ---

def load_all_records(conn) -> list[dict]:
    records = []
    with conn.cursor() as cur:
        cur.execute("SELECT cik, company_name, street, city, state, zip_code FROM sources.sec_companies")
        for row in cur.fetchall():
            records.append({
                "source": "sec", "source_id": str(row[0]),
                "name": row[1] or "", "street": row[2] or "",
                "city": row[3] or "", "state": row[4] or "", "zip": row[5] or "",
                "parent_name": "",
            })

        cur.execute("SELECT registration_id, entity_name, agent_street, agent_city, agent_state, agent_zip FROM sources.state_registrations")
        for row in cur.fetchall():
            records.append({
                "source": "state", "source_id": row[0],
                "name": row[1] or "", "street": row[2] or "",
                "city": row[3] or "", "state": row[4] or "", "zip": row[5] or "",
                "parent_name": "",
            })

        cur.execute("SELECT uei, recipient_name, parent_name, street, city, state, zip_code FROM sources.usaspending_recipients")
        for row in cur.fetchall():
            records.append({
                "source": "usaspending", "source_id": row[0],
                "name": row[1] or "", "street": row[3] or "",
                "city": row[4] or "", "state": row[5] or "", "zip": row[6] or "",
                "parent_name": row[2] or "",
            })

    for r in records:
        r["norm_name"] = normalize_name(r["name"])
        r["norm_street"] = normalize_street(r["street"])
        r["norm_zip"] = normalize_zip(r["zip"])
        r["norm_parent"] = normalize_name(r["parent_name"]) if r["parent_name"] else ""
        base, num = extract_name_number(r["norm_name"])
        r["name_base"] = base
        r["name_number"] = num

    return records


# --- Union-Find for clustering matched records ---

class UnionFind:
    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


# --- Matching logic ---

def compute_match(r1: dict, r2: dict) -> tuple[float, str]:
    """Compare two records and return (score, method). Returns (0.0, '') if no match."""
    name_score = fuzz.ratio(r1["norm_name"], r2["norm_name"]) / 100.0
    token_score = fuzz.token_sort_ratio(r1["norm_name"], r2["norm_name"]) / 100.0
    best_name = max(name_score, token_score)

    # If both have trailing numbers/roman numerals and they differ, never match
    if r1["name_number"] and r2["name_number"] and r1["name_number"] != r2["name_number"]:
        return (0.0, "")

    # Address and state checks
    street_score = 0.0
    if r1["norm_street"] and r2["norm_street"]:
        street_score = fuzz.ratio(r1["norm_street"], r2["norm_street"]) / 100.0
    same_state = (not r1["state"] or not r2["state"] or r1["state"] == r2["state"])
    address_confirms = street_score > 0.7

    # Exact normalized name
    if r1["norm_name"] == r2["norm_name"] and r1["norm_name"] != "":
        if same_state:
            return (1.0, "exact_name")
        if address_confirms:
            return (0.95, "exact_name_confirmed_address")
        return (0.0, "")

    # Check word-level differences
    words1 = set(r1["norm_name"].split())
    words2 = set(r2["norm_name"].split())
    missing_words = words1.symmetric_difference(words2)
    filler_words = {"and", "of", "the", "for", "a", "an"}
    meaningful_diff = missing_words - filler_words

    # High fuzzy match (>=85%)
    if best_name >= 0.85:
        if meaningful_diff and not address_confirms:
            return (0.0, "")

        method = "fuzzy_name"
        score = best_name

        if address_confirms:
            score = min(score + 0.1, 1.0)
            method = "fuzzy_name_confirmed_address"
        elif not same_state:
            return (0.0, "")

        if score >= 0.70:
            return (round(score, 3), method)

    # Medium fuzzy name match (>=65%) with strong address in same state
    if best_name >= 0.65:
        if street_score > 0.8 and same_state:
            return (round(best_name + 0.1, 3), "medium_name_strong_address")

    return (0.0, "")


def match_all_records(records: list[dict]) -> tuple[UnionFind, dict]:
    n = len(records)
    uf = UnionFind(n)
    match_info = {}

    # Standard pairwise matching
    for i in range(n):
        for j in range(i + 1, n):
            score, method = compute_match(records[i], records[j])
            if score >= 0.70:
                uf.union(i, j)
                key = (min(i, j), max(i, j))
                if key not in match_info or score > match_info[key][0]:
                    match_info[key] = (score, method)

    # Parent name linking: match USAspending parent_name to other records
    for i in range(n):
        if records[i]["norm_parent"] and records[i]["norm_parent"] != records[i]["norm_name"]:
            for j in range(n):
                if i == j:
                    continue
                parent_score = fuzz.ratio(records[i]["norm_parent"], records[j]["norm_name"]) / 100.0
                if parent_score >= 0.85:
                    uf.union(i, j)
                    key = (min(i, j), max(i, j))
                    s = round(parent_score, 3)
                    if key not in match_info or s > match_info[key][0]:
                        match_info[key] = (s, "parent_name_link")

    return uf, match_info


# --- Build entities and write to DB ---

def pick_canonical_name(cluster_records: list[dict]) -> str:
    priority = {"sec": 0, "state": 1, "usaspending": 2}
    sorted_recs = sorted(cluster_records, key=lambda r: priority.get(r["source"], 3))
    best_source = sorted_recs[0]["source"]
    same_source = [r for r in sorted_recs if r["source"] == best_source]
    return max(same_source, key=lambda r: len(r["name"]))["name"]


def make_entity_id(members: list[tuple[int, dict]]) -> str:
    """Generate a deterministic UUID based on sorted source keys."""
    parts = sorted(f"{rec['source']}:{rec['source_id']}" for _, rec in members)
    name = "|".join(parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def write_results(conn, records: list[dict], uf: UnionFind, match_info: dict):
    clusters = {}
    for i, rec in enumerate(records):
        root = uf.find(i)
        clusters.setdefault(root, []).append((i, rec))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM resolved.entity_source_links")
        cur.execute("DELETE FROM resolved.entities")

        for root, members in clusters.items():
            cluster_records = [rec for _, rec in members]
            canonical = pick_canonical_name(cluster_records)
            entity_id = make_entity_id(members)
            sources_present = set(r["source"] for r in cluster_records)
            notes = f"Sources: {', '.join(sorted(sources_present))}. {len(members)} record(s)."

            cur.execute(
                "INSERT INTO resolved.entities (entity_id, canonical_name, notes) VALUES (%s, %s, %s)",
                (entity_id, canonical, notes),
            )

            for idx, rec in members:
                best_score = 1.0 if len(members) == 1 else 0.0
                best_method = "singleton" if len(members) == 1 else ""

                if len(members) > 1:
                    for other_idx, _ in members:
                        if other_idx == idx:
                            continue
                        key = (min(idx, other_idx), max(idx, other_idx))
                        if key in match_info:
                            s, m = match_info[key]
                            if s > best_score:
                                best_score = s
                                best_method = m

                    if best_score == 0.0:
                        best_score = 0.70
                        best_method = "transitive_link"

                cur.execute(
                    """INSERT INTO resolved.entity_source_links
                       (entity_id, source_system, source_record_id, match_score, match_method)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (entity_id, rec["source"], rec["source_id"], best_score, best_method),
                )

        conn.commit()


def main() -> None:
    print("Loading records...")
    with connect() as conn:
        records = load_all_records(conn)
        print(f"Loaded {len(records)} records")

        print("Matching records...")
        uf, match_info = match_all_records(records)

        print("Writing results...")
        write_results(conn, records, uf, match_info)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM resolved.entities")
            (entity_count,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM resolved.entity_source_links")
            (link_count,) = cur.fetchone()
            cur.execute("""
                SELECT COUNT(*) FROM resolved.entities e
                WHERE (SELECT COUNT(*) FROM resolved.entity_source_links l WHERE l.entity_id = e.entity_id) > 1
            """)
            (multi_count,) = cur.fetchone()

        print(f"\nDone!")
        print(f"  Entities created: {entity_count}")
        print(f"  Source links: {link_count}")
        print(f"  Entities with multiple sources: {multi_count}")


if __name__ == "__main__":
    main()