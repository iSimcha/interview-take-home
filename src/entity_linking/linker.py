"""Entity linking: record comparison, clustering, and output building."""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from rapidfuzz import fuzz

from entity_linking.normalize import (
    FILLER_WORDS,
    extract_roman_suffix,
    normalize_name,
    normalize_street,
    normalize_text,
    normalize_zip,
)

# Fixed UUID namespaces — keeping these stable ensures entity IDs are
# reproducible across pipeline runs.
ENTITY_NAMESPACE = uuid.UUID("8b7b4e2f-cd47-4a14-b8b6-6cb0d087f432")
LINK_NAMESPACE   = uuid.UUID("4fd19c29-d27f-40a4-9a75-baaef24df43b")

# Scoring weights — name carries the most signal; geography breaks ties
NAME_WEIGHT   = 0.65
STREET_WEIGHT = 0.10
CITY_WEIGHT   = 0.10
STATE_WEIGHT  = 0.10
ZIP_WEIGHT    = 0.05

# SEC names are the most formally registered, so prefer them as canonical
SOURCE_PRIORITY: dict[str, int] = {"sec": 0, "state": 1, "usaspending": 2}


@dataclass(frozen=True)
class SourceRecord:
    source_system:    str
    source_record_id: str
    name:             str
    street:           str | None
    city:             str | None
    state:            str | None
    zip_code:         str | None
    parent_name:      str | None
    key:              str
    name_key:         str
    street_key:       str
    city_key:         str
    state_key:        str
    zip_key:          str
    parent_name_key:  str
    roman_suffix:     str


@dataclass(frozen=True)
class MatchDecision:
    matched: bool
    score:   float
    method:  str


@dataclass(frozen=True)
class EntityRow:
    entity_id:      uuid.UUID
    canonical_name: str
    notes:          str | None


@dataclass(frozen=True)
class LinkRow:
    link_id:          uuid.UUID
    entity_id:        uuid.UUID
    source_system:    str
    source_record_id: str
    match_score:      float
    match_method:     str


def make_source_record(
    *,
    source_system:    str,
    source_record_id: str,
    name:             str,
    street:           str | None = None,
    city:             str | None = None,
    state:            str | None = None,
    zip_code:         str | None = None,
    parent_name:      str | None = None,
) -> SourceRecord:
    name_key = normalize_name(name)
    return SourceRecord(
        source_system=source_system,
        source_record_id=source_record_id,
        name=name,
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
        parent_name=parent_name,
        key=f"{source_system}:{source_record_id}",
        name_key=name_key,
        street_key=normalize_street(street),
        city_key=normalize_text(city),
        state_key=(state or "").upper().strip(),
        zip_key=normalize_zip(zip_code),
        parent_name_key=normalize_name(parent_name) if parent_name else "",
        roman_suffix=extract_roman_suffix(name_key),
    )


def _sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.ratio(a, b) / 100.0


def compare_records(left: SourceRecord, right: SourceRecord) -> MatchDecision:
    """Compare two records and return a match decision.

    Hard blocks:
    - same_source: records from the same system are never compared
    - roman_numeral_mismatch: 'Realty II' and 'Realty III' never merge

    Match tiers (most to least strict):
    1. exact_name_state  — identical normalized name + same state + geo confirmed
    2. exact_name_zip    — identical normalized name + same ZIP + geo confirmed
    3. fuzzy_name_geo    — high similarity (>=0.92) + geo confirmed + word-diff check
    4. medium_name_addr  — moderate similarity (>=0.75) + same state + strong address
    """
    if left.source_system == right.source_system:
        return MatchDecision(False, 0.0, "same_source")

    if left.roman_suffix and right.roman_suffix and left.roman_suffix != right.roman_suffix:
        return MatchDecision(False, 0.0, "roman_numeral_mismatch")

    name_sim   = _sim(left.name_key, right.name_key)
    street_sim = _sim(left.street_key, right.street_key)
    city_sim   = _sim(left.city_key, right.city_key)
    same_state = bool(left.state_key and left.state_key == right.state_key)
    same_zip   = bool(left.zip_key and left.zip_key == right.zip_key)

    score = round(
        min(
            NAME_WEIGHT   * name_sim
            + STREET_WEIGHT * street_sim
            + CITY_WEIGHT   * city_sim
            + (STATE_WEIGHT if same_state else 0.0)
            + (ZIP_WEIGHT   if same_zip   else 0.0),
            0.999,
        ),
        3,
    )

    strong_street = street_sim >= 0.85 and bool(left.street_key and right.street_key)
    city_confirms = city_sim  >= 0.85 and bool(left.city_key   and right.city_key)
    geo_confirms  = same_state and (same_zip or strong_street or city_confirms)
    exact_name    = bool(left.name_key and left.name_key == right.name_key)

    if exact_name and same_state and (same_zip or strong_street or city_confirms):
        return MatchDecision(True, max(score, 0.985), "exact_name_state")

    if exact_name and same_zip and (strong_street or city_confirms):
        return MatchDecision(True, max(score, 0.975), "exact_name_zip")

    if name_sim >= 0.92 and geo_confirms:
        # Reject if names differ by a real word (not just filler) and there's
        # no strong address to back it up — catches "Atlantic Coast" vs "Atlantic Coastal"
        words_l = set(left.name_key.split())  - FILLER_WORDS
        words_r = set(right.name_key.split()) - FILLER_WORDS
        if words_l.symmetric_difference(words_r) and not (strong_street or city_confirms):
            return MatchDecision(False, score, "singleton")
        return MatchDecision(True, max(score, 0.90), "fuzzy_name_geo")

    if name_sim >= 0.75 and same_state and strong_street and city_confirms:
        return MatchDecision(True, max(score, 0.80), "medium_name_address")

    return MatchDecision(False, score, "singleton")


def find_matches(
    records: list[SourceRecord],
) -> tuple[dict[str, set[str]], dict[str, MatchDecision]]:
    ordered = sorted(records, key=lambda r: (r.source_system, r.source_record_id))

    neighbors:      dict[str, set[str]]       = {r.key: set() for r in ordered}
    best_decisions: dict[str, MatchDecision]  = {
        r.key: MatchDecision(False, 0.0, "singleton") for r in ordered
    }

    for i, left in enumerate(ordered):
        for right in ordered[i + 1:]:
            decision = compare_records(left, right)
            if not decision.matched:
                continue
            neighbors[left.key].add(right.key)
            neighbors[right.key].add(left.key)
            if decision.score > best_decisions[left.key].score:
                best_decisions[left.key] = decision
            if decision.score > best_decisions[right.key].score:
                best_decisions[right.key] = decision

    # USAspending records include a parent_name field — match it against
    # SEC/state names to link subsidiaries back to their parent entity.
    for rec in ordered:
        if rec.source_system != "usaspending":
            continue
        if not rec.parent_name_key or rec.parent_name_key == rec.name_key:
            continue
        for other in ordered:
            if other.source_system == "usaspending" or other.key == rec.key:
                continue
            parent_sim = _sim(rec.parent_name_key, other.name_key)
            if parent_sim < 0.85:
                continue
            neighbors[rec.key].add(other.key)
            neighbors[other.key].add(rec.key)
            d = MatchDecision(True, round(parent_sim, 3), "parent_name_link")
            if d.score > best_decisions[rec.key].score:
                best_decisions[rec.key] = d
            if d.score > best_decisions[other.key].score:
                best_decisions[other.key] = d

    return neighbors, best_decisions


def build_groups(
    records:   list[SourceRecord],
    neighbors: dict[str, set[str]],
) -> list[list[SourceRecord]]:
    """DFS to find connected components — each component becomes one entity."""
    by_key:  dict[str, SourceRecord] = {r.key: r for r in records}
    visited: set[str]                = set()
    groups:  list[list[SourceRecord]] = []

    for record in sorted(records, key=lambda r: (r.source_system, r.source_record_id)):
        if record.key in visited:
            continue
        stack:     list[str]          = [record.key]
        component: list[SourceRecord] = []
        visited.add(record.key)
        while stack:
            key = stack.pop()
            component.append(by_key[key])
            for nbr in sorted(neighbors[key]):
                if nbr not in visited:
                    visited.add(nbr)
                    stack.append(nbr)
        groups.append(
            sorted(component, key=lambda r: (r.source_system, r.source_record_id))
        )

    return groups


def choose_canonical_name(group: list[SourceRecord]) -> str:
    ranked = sorted(
        group,
        key=lambda r: (
            SOURCE_PRIORITY.get(r.source_system, 99),
            -len(normalize_text(r.name)),
            r.source_record_id,
        ),
    )
    return ranked[0].name.strip()


def _make_entity_id(group: list[SourceRecord]) -> uuid.UUID:
    parts = sorted(f"{r.source_system}:{r.source_record_id}" for r in group)
    return uuid.uuid5(ENTITY_NAMESPACE, "|".join(parts))


def _make_link_id(entity_id: uuid.UUID, source_system: str, source_record_id: str) -> uuid.UUID:
    return uuid.uuid5(LINK_NAMESPACE, f"{entity_id}|{source_system}|{source_record_id}")


def build_entity_rows(groups: list[list[SourceRecord]]) -> list[EntityRow]:
    rows: list[EntityRow] = []
    for group in groups:
        entity_id = _make_entity_id(group)
        notes = None if len(group) == 1 else f"linked_{len(group)}_records"
        rows.append(EntityRow(
            entity_id=entity_id,
            canonical_name=choose_canonical_name(group),
            notes=notes,
        ))
    return rows


def build_link_rows(
    groups:         list[list[SourceRecord]],
    best_decisions: dict[str, MatchDecision],
) -> list[LinkRow]:
    rows: list[LinkRow] = []
    for group in groups:
        entity_id = _make_entity_id(group)
        for rec in group:
            decision = best_decisions[rec.key]
            rows.append(LinkRow(
                link_id=_make_link_id(entity_id, rec.source_system, rec.source_record_id),
                entity_id=entity_id,
                source_system=rec.source_system,
                source_record_id=rec.source_record_id,
                match_score=round(decision.score, 3),
                match_method=decision.method,
            ))
    return rows


def build_resolved_entities(
    records: list[SourceRecord],
) -> tuple[list[EntityRow], list[LinkRow]]:
    neighbors, best_decisions = find_matches(records)
    groups = build_groups(records, neighbors)
    return build_entity_rows(groups), build_link_rows(groups, best_decisions)