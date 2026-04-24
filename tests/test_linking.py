"""Tests for the entity linking pipeline."""
from entity_linking.db import connect
from entity_linking.main import (
    normalize_name, normalize_street, normalize_zip,
    compute_match, extract_name_number, main,
)


# --- Normalization tests ---

def test_normalize_strips_legal_suffixes():
    assert normalize_name("ACME SEMICONDUCTOR CORP") == "acme semiconductor"
    assert normalize_name("Acme Semiconductor Corporation") == "acme semiconductor"
    assert normalize_name("FULTON PHARMACEUTICALS, INC.") == "fulton pharmaceuticals"


def test_normalize_handles_ampersand_and_punctuation():
    assert normalize_name("Baker & Holt Industries, Inc.") == "baker and holt industries"
    assert normalize_name("BAKER AND HOLT INDUSTRIES INC") == "baker and holt industries"


def test_normalize_zip_strips_plus4():
    assert normalize_zip("28401-0088") == "28401"
    assert normalize_zip("95054") == "95054"


def test_normalize_street_abbreviations():
    assert "blvd" in normalize_street("200 Harbor Boulevard")
    assert "dr" in normalize_street("1200 Innovation Drive")


def test_extract_name_number():
    base, num = extract_name_number("westpoint industrial realty iii")
    assert base == "westpoint industrial realty"
    assert num == "iii"
    base, num = extract_name_number("acme semiconductor")
    assert base == "acme semiconductor"
    assert num == ""


# --- Matching tests ---

def test_exact_match_same_state():
    r1 = {"norm_name": "acme semiconductor", "norm_street": "1200 innovation dr", "state": "CA", "norm_zip": "95054", "name_base": "acme semiconductor", "name_number": ""}
    r2 = {"norm_name": "acme semiconductor", "norm_street": "1200 innovation dr", "state": "CA", "norm_zip": "95054", "name_base": "acme semiconductor", "name_number": ""}
    score, method = compute_match(r1, r2)
    assert score == 1.0
    assert method == "exact_name"


def test_exact_name_different_state_no_address_rejects():
    r1 = {"norm_name": "summit consulting group", "norm_street": "700 k st", "state": "DC", "norm_zip": "20006", "name_base": "summit consulting group", "name_number": ""}
    r2 = {"norm_name": "summit consulting group", "norm_street": "450 mountain rd", "state": "CO", "norm_zip": "80202", "name_base": "summit consulting group", "name_number": ""}
    score, method = compute_match(r1, r2)
    assert score == 0.0


def test_roman_numeral_mismatch_rejects():
    r1 = {"norm_name": "westpoint industrial realty ii", "norm_street": "400 madison ave", "state": "DE", "norm_zip": "10017", "name_base": "westpoint industrial realty", "name_number": "ii"}
    r2 = {"norm_name": "westpoint industrial realty iii", "norm_street": "400 madison ave", "state": "DE", "norm_zip": "10017", "name_base": "westpoint industrial realty", "name_number": "iii"}
    score, method = compute_match(r1, r2)
    assert score == 0.0


def test_fuzzy_match_catches_filler_word_difference():
    r1 = {"norm_name": "baker and holt industries", "norm_street": "45 commerce st", "state": "OH", "norm_zip": "44114", "name_base": "baker and holt industries", "name_number": ""}
    r2 = {"norm_name": "baker holt industries", "norm_street": "45 commerce st", "state": "OH", "norm_zip": "44114", "name_base": "baker holt industries", "name_number": ""}
    score, method = compute_match(r1, r2)
    assert score >= 0.7


def test_atlantic_near_miss_does_not_match():
    """Atlantic Coast Freight and Atlantic Coastal Freight are different companies."""
    r1 = {"norm_name": "atlantic coast freight", "norm_street": "88 shore rd", "state": "NC", "norm_zip": "28401", "name_base": "atlantic coast freight", "name_number": ""}
    r2 = {"norm_name": "atlantic coastal freight", "norm_street": "200 harbor blvd", "state": "NJ", "norm_zip": "07302", "name_base": "atlantic coastal freight", "name_number": ""}
    score, method = compute_match(r1, r2)
    assert score == 0.0


def test_no_match_for_unrelated_companies():
    r1 = {"norm_name": "acme semiconductor", "norm_street": "1200 innovation dr", "state": "CA", "norm_zip": "95054", "name_base": "acme semiconductor", "name_number": ""}
    r2 = {"norm_name": "baker and holt industries", "norm_street": "45 commerce st", "state": "OH", "norm_zip": "44114", "name_base": "baker and holt industries", "name_number": ""}
    score, method = compute_match(r1, r2)
    assert score == 0.0


def test_pipeline_links_all_500_records():
    """Run the full pipeline and verify all 500 source records are linked."""
    main()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM resolved.entity_source_links")
        (count,) = cur.fetchone()
    assert count == 500, f"Expected 500 links, got {count}"