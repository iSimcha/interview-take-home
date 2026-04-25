import pytest

from entity_linking.db import connect

from entity_linking.pipeline import find_best_match


def make_entity(name, street="", city="", state="", zip=""):
    return {
        "entity_id": "1",
        "name": name,
        "street": street,
        "city": city,
        "state": state,
        "zip": zip,
    }


#  exact matching entities
def test_exact_full_match():
    entities = [
        make_entity("ACME SEMICONDUCTOR", "123 MAIN ST", "NYC", "NY", "10001")
    ]

    match, score, method = find_best_match(
        "ACME SEMICONDUCTOR",
        "123 MAIN ST",
        "NYC",
        "NY",
        "10001",
        None,
        entities,
    )

    assert method == "exact_match"
    assert score == 1.0


# Fuzzy name match, with same location
def test_fuzzy_name_but_rest_exact():
    entities = [
        make_entity("SUMMIT CONSULTING GROUP", "10 BROADWAY", "NYC", "NY", "10001")
    ]

    match, score, method = find_best_match(
        "SUMMIT CONSULTING GRP",
        "10 BROADWAY",
        "NYC",
        "NY",
        "10001",
        None,
        entities,
    )

    assert method == "name_fuzzy_match_same_location"
    assert score >= 0.75


# Fuzzy name nad fuzzy street with same location
def test_fuzzy_name_and_street_same_location():
    entities = [
        make_entity("CASCADE BIOTECH", "55 SCIENCE PARK", "SEATTLE", "WA", "98101")
    ]

    match, score, method = find_best_match(
        "CASCADE BIOTECH INC",
        "55 SCIENCE PK",
        "SEATTLE",
        "WA",
        "98101",
        None,
        entities,
    )

    assert method in ["fuzzy_match"]
    assert score >= 0.75



# Parent matching for usaspending_recipients
def test_parent_match():
    entities = [
        make_entity("IRONWOOD DEFENSE SYSTEMS")
    ]

    match, score, method = find_best_match(
        "IRONWOOD EAST DIVISION",
        "12 SEC RD",
        "DALLAS",
        "TX",
        "75001",
        "IRONWOOD DEFENSE SYSTEMS",
        entities,
    )

    assert method in ["parent_match", "parent_fuzzy_match"]
    assert score >= 0.95
    

# Same name with different location, so different entity
def test_same_name_different_location_no_match():
    entities = [
        make_entity("NOVA TECH", "1 A ST", "BOSTON", "MA", "02101")
    ]

    match, score, method = find_best_match(
        "NOVA TECH",
        "999 Z ST",
        "MIAMI",
        "FL",
        "33101",
        None,
        entities,
    )

    assert method == "no_match" or score < 0.75


# Differnt entities at same location
def test_different_name_same_location():
    entities = [
        make_entity("ABC CONSTRUCTION GROUP", "10 BROADWAY", "NYC", "NY", "10001")
    ]

    match, score, method = find_best_match(
        "SUMMIT CONSULTING LLC",  # different name
        "10 BROADWAY",
        "NYC",
        "NY",
        "10001",
        None,
        entities,
    )

    assert method == "no_match" or score < 0.75


# Nothing matching
def test_completely_different_no_match():
    entities = [
        make_entity("GLACIER SYSTEMS", "10 TECH PARK", "AUSTIN", "TX", "73301")
    ]

    match, score, method = find_best_match(
        "ORION DEFENSE",
        "500 UNKNOWN RD",
        "MIAMI",
        "FL",
        "99999",
        None,
        entities,
    )

    assert method == "no_match" or score < 0.75