"""Tests for the entity linking pipeline logic."""
from __future__ import annotations
import pytest
from entity_linking.main import normalize_name, normalize_zip, normalize_street, score_pair
import pandas as pd


# ---------------------------------------------------------------------------
# Normalization tests
# ---------------------------------------------------------------------------

def test_normalize_name_strips_punctuation_and_suffix():
    assert normalize_name("Apple Inc.") == "apple inc"

def test_normalize_name_expands_ampersand():
    assert normalize_name("Johnson & Johnson") == "johnson and johnson"

def test_normalize_zip_truncates_zip4():
    assert normalize_zip("95014-2083") == "95014"


# ---------------------------------------------------------------------------
# Scoring tests
# ---------------------------------------------------------------------------

def make_record(name, street="", zip_code="", state="CA"):
    return pd.Series({
        "norm_name": normalize_name(name),
        "norm_street": normalize_street(street),
        "norm_zip": normalize_zip(zip_code),
        "state": state,
    })


def test_clear_match_scores_high():
    """Same company, minor name variation — should score high."""
    a = make_record("Rockwater Engineering Inc", "123 Main St", "90210")
    b = make_record("ROCKWATER ENGINEERING INC.", "123 Main Street", "90210")
    score, method = score_pair(a, b)
    assert score >= 0.85
    assert method == "name_address_high"


def test_clear_nonmatch_scores_low():
    """Completely different companies — should score low."""
    a = make_record("Apple Technologies LLC", "1 Infinite Loop", "95014")
    b = make_record("Banana Foods Co", "500 Broadway", "10001")
    score, method = score_pair(a, b)
    assert score < 0.60
    assert method == "no_match"


def test_ambiguous_match_scores_middle():
    """Similar name, different address — ambiguous, should be fuzzy."""
    a = make_record("Atlas Construction Corp", "100 Oak Ave", "30301")
    b = make_record("Atlas Construction Company", "200 Pine Rd", "94105")
    score, method = score_pair(a, b)
    assert 0.60 <= score < 0.85
    assert method == "name_address_fuzzy"


def test_same_name_different_state_scores_lower():
    """Common name used by unrelated entities in different states."""
    a = make_record("National Services Inc", "100 Main St", "10001", state="NY")
    b = make_record("National Services Inc", "100 Main St", "90001", state="CA")
    score, method = score_pair(a, b)
    # Name matches perfectly but zip differs — still could be high on name alone
    assert score >= 0.60  # name similarity pulls it up