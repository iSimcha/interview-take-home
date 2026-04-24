"""Normalization helpers for company names, addresses, and ZIP codes."""
from __future__ import annotations

import re

LEGAL_SUFFIXES: frozenset[str] = frozenset({
    "CO", "COMPANY", "CORP", "CORPORATION",
    "INC", "INCORPORATED",
    "LLC", "LLP", "LP", "LTD", "LIMITED",
    "PARTNERSHIP", "COOPERATIVE",
    "PC", "PLLC",
})

TOKEN_NORMALIZATIONS: dict[str, str] = {
    "AND":    "",
    "ASSOC":  "ASSOCIATES",
    "BROS":   "BROTHERS",
    "COMM":   "COMMUNICATIONS",
    "DEPT":   "DEPARTMENT",
    "FED":    "FEDERAL",
    "FEDN":   "FEDERATION",
    "FINCL":  "FINANCIAL",
    "INSTR":  "INSTRUMENTS",
    "INTL":   "INTERNATIONAL",
    "MGMT":   "MANAGEMENT",
    "MFG":    "MANUFACTURING",
    "NATL":   "NATIONAL",
    "PHARMA": "PHARMACEUTICALS",
    "SVC":    "SERVICE",
    "SVCS":   "SERVICES",
    "TECH":   "TECHNOLOGY",
    "UNIV":   "UNIVERSITY",
}

STREET_NORMALIZATIONS: dict[str, str] = {
    "AVENUE":    "AVE",
    "BOULEVARD": "BLVD",
    "COURT":     "CT",
    "DRIVE":     "DR",
    "LANE":      "LN",
    "PARKWAY":   "PKWY",
    "PLACE":     "PL",
    "ROAD":      "RD",
    "STREET":    "ST",
}

ROMAN_NUMERALS: frozenset[str] = frozenset({
    "I", "II", "III", "IV", "V",
    "VI", "VII", "VIII", "IX", "X",
})

FILLER_WORDS: frozenset[str] = frozenset({"OF", "THE", "FOR", "A", "AN"})


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = value.upper().replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    tokens = normalize_text(value).split()
    result: list[str] = []
    for token in tokens:
        mapped = TOKEN_NORMALIZATIONS.get(token, token)
        if mapped and mapped not in LEGAL_SUFFIXES:
            result.append(mapped)
    return " ".join(result)


def normalize_street(value: str | None) -> str:
    if not value:
        return ""
    text = normalize_text(value)
    text = re.sub(
        r"\b(SUITE|STE|APT|UNIT|FLOOR|FL|BLDG|BUILDING)\s*[#]?\s*\w*\b",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return " ".join(STREET_NORMALIZATIONS.get(t, t) for t in text.split()).strip()


def normalize_zip(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^0-9]", "", value)[:5]


def extract_roman_suffix(norm_name: str) -> str:
    tokens = norm_name.split()
    if tokens and tokens[-1] in ROMAN_NUMERALS:
        return tokens[-1]
    return ""
