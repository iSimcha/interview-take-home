"""Tests for the entity linking pipeline."""
from __future__ import annotations

from entity_linking.normalize import (
    extract_roman_suffix,
    normalize_name,
    normalize_zip,
)
from entity_linking.linker import (
    build_resolved_entities,
    compare_records,
    make_source_record,
)
from entity_linking.db import connect
from entity_linking.main import load_source_records


def rec(source_system, source_record_id, name, street="", city="", state="", zip_code="", parent_name=None):
    return make_source_record(
        source_system=source_system,
        source_record_id=source_record_id,
        name=name,
        street=street or None,
        city=city or None,
        state=state or None,
        zip_code=zip_code or None,
        parent_name=parent_name,
    )


# normalization

def test_normalize_name_strips_legal_suffixes():
    assert normalize_name("ACME SEMICONDUCTOR CORP")      == "ACME SEMICONDUCTOR"
    assert normalize_name("Fulton Pharmaceuticals, Inc.") == "FULTON PHARMACEUTICALS"
    assert normalize_name("GREEN VALLEY LLC")             == "GREEN VALLEY"


def test_normalize_name_ampersand_and_and_are_equivalent():
    assert normalize_name("Baker & Holt Industries Inc") == \
           normalize_name("Baker and Holt Industries Inc")


def test_normalize_zip_strips_plus4():
    assert normalize_zip("28401-0088") == "28401"
    assert normalize_zip("95054")      == "95054"
    assert normalize_zip("")           == ""


def test_extract_roman_suffix():
    assert extract_roman_suffix("WESTPOINT INDUSTRIAL REALTY III") == "III"
    assert extract_roman_suffix("WESTPOINT INDUSTRIAL REALTY II")  == "II"
    assert extract_roman_suffix("ACME SEMICONDUCTOR")              == ""


# matching

def test_same_source_never_matches():
    r1 = rec("sec", "1001", "ACME CORP", street="100 Main St", city="Austin", state="TX", zip_code="78701")
    r2 = rec("sec", "1002", "ACME CORP", street="100 Main St", city="Austin", state="TX", zip_code="78701")
    d  = compare_records(r1, r2)
    assert not d.matched
    assert d.method == "same_source"


def test_roman_numeral_mismatch_blocked():
    r1 = rec("sec",         "1001",   "WESTPOINT INDUSTRIAL REALTY II LP",
             street="1 Park Ave", city="New York", state="NY", zip_code="10017")
    r2 = rec("usaspending", "UEI001", "Westpoint Industrial Realty III",
             street="1 Park Ave", city="New York", state="NY", zip_code="10017")
    d  = compare_records(r1, r2)
    assert not d.matched
    assert d.method == "roman_numeral_mismatch"


def test_exact_name_same_state_high_score():
    r1 = rec("sec",         "1001",   "ACME SEMICONDUCTOR CORP",
             street="1200 Innovation Dr",    city="Sacramento", state="CA", zip_code="95054")
    r2 = rec("usaspending", "UEI001", "Acme Semiconductor Corp.",
             street="1200 Innovation Drive", city="Sacramento", state="CA", zip_code="95054")
    d  = compare_records(r1, r2)
    assert d.matched
    assert d.method == "exact_name_state"
    assert d.score  >= 0.985


def test_near_miss_different_state_no_match():
    r1 = rec("sec",         "2001",   "ATLANTIC COAST FREIGHT LLC",
             street="88 Shore Rd",     city="Raleigh",   state="NC", zip_code="28401")
    r2 = rec("usaspending", "UEI002", "Atlantic Coastal Freight",
             street="200 Harbor Blvd", city="Princeton", state="NJ", zip_code="07302")
    d  = compare_records(r1, r2)
    assert not d.matched


def test_same_name_different_state_no_address_no_match():
    r1 = rec("sec",   "3001",  "SUMMIT CONSULTING GROUP INC",
             street="700 K St",        city="Washington", state="DC", zip_code="20006")
    r2 = rec("state", "CO-01", "Summit Consulting Group LLC",
             street="450 Mountain Rd", city="Denver",     state="CO", zip_code="80202")
    d  = compare_records(r1, r2)
    assert not d.matched


# parent name linking

def test_parent_name_links_subsidiary_to_parent():
    parent = rec(
        "sec", "3001", "RIVERSTONE HOLDINGS CORP",
        street="100 Main St", city="Austin", state="TX", zip_code="78701",
    )
    subsidiary = rec(
        "usaspending", "UEI003", "RIVERSTONE FEDERAL PROGRAMS LLC",
        street="200 Oak Ave", city="Dallas", state="TX", zip_code="75201",
        parent_name="Riverstone Holdings Corp",
    )
    unrelated = rec(
        "state", "TX-999", "UNRELATED COMPANY INC",
        street="500 Pine St", city="Houston", state="TX", zip_code="77001",
    )
    entities, links = build_resolved_entities([parent, subsidiary, unrelated])
    assert len(entities) == 2
    assert len(links)    == 3
    link_map = {f"{l.source_system}:{l.source_record_id}": l.entity_id for l in links}
    assert link_map["sec:3001"] == link_map["usaspending:UEI003"]
    assert link_map["state:TX-999"] != link_map["sec:3001"]


# integration

def test_pipeline_links_all_500_records():
    with connect() as conn, conn.cursor() as cur:
        records = load_source_records(cur)
    assert len(records) == 500
    _, links = build_resolved_entities(records)
    assert len(links) == 500
    seen = {(l.source_system, l.source_record_id) for l in links}
    assert len(seen) == 500