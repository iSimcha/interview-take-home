from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from difflib import SequenceMatcher


# Fixed namespaces keep uuid5-generated entity/link IDs deterministic across reruns.
ENTITY_NAMESPACE = uuid.UUID("8b7b4e2f-cd47-4a14-b8b6-6cb0d087f432")
LINK_NAMESPACE = uuid.UUID("4fd19c29-d27f-40a4-9a75-baaef24df43b")

LEGAL_SUFFIXES = {
    "CO",
    "COMPANY",
    "CORP",
    "CORPORATION",
    "INC",
    "INCORPORATED",
    "LLC",
    "LTD",
    "LP",
    "L P",
    "LIMITED",
    "PARTNERSHIP",
}

TOKEN_NORMALIZATIONS = {
    "AND": "",
    "&": "",
    "BROS": "BROTHERS",
    "CO.": "COMPANY",
    "COMM": "COMMUNICATIONS",
    "DEPT": "DEPARTMENT",
    "FINCL": "FINANCIAL",
    "INSTR": "INSTRUMENTS",
    "PHARMA": "PHARMACEUTICALS",
    "PKWY": "PARKWAY",
    "RD": "ROAD",
    "ST": "SAINT",
    "TECH": "TECHNOLOGY",
    "UNIV": "UNIVERSITY",
}

STREET_NORMALIZATIONS = {
    "AVE": "AVENUE",
    "BLVD": "BOULEVARD",
    "BUILDING": "BLDG",
    "CT": "COURT",
    "DR": "DRIVE",
    "HWY": "HIGHWAY",
    "LANE": "LN",
    "LN": "LANE",
    "PARKWAY": "PKWY",
    "PKWY": "PARKWAY",
    "PLACE": "PL",
    "PL": "PLACE",
    "RD": "ROAD",
    "ROAD": "RD",
    "SQ": "SQUARE",
    "STE": "SUITE",
    "STREET": "ST",
    "ST": "STREET",
    "SUITE": "STE",
    "WAY": "WAY",
}

SOURCE_PRIORITY = {"sec": 0, "usaspending": 1, "state": 2}


@dataclass(frozen=True)
class SourceRecord:
    source_system: str
    source_record_id: str
    name: str
    street: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    key: str
    name_key: str
    street_key: str
    city_key: str
    state_key: str
    zip_key: str


@dataclass(frozen=True)
class MatchDecision:
    matched: bool
    score: float
    method: str


@dataclass(frozen=True)
class EntityRow:
    entity_id: uuid.UUID
    canonical_name: str
    notes: str | None


@dataclass(frozen=True)
class LinkRow:
    link_id: uuid.UUID
    entity_id: uuid.UUID
    source_system: str
    source_record_id: str
    match_score: float
    match_method: str


def make_source_record(
    *,
    source_system: str,
    source_record_id: str,
    name: str,
    street: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
) -> SourceRecord:
    return SourceRecord(
        source_system=source_system,
        source_record_id=source_record_id,
        name=name,
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
        key=f"{source_system}:{source_record_id}",
        name_key=normalize_name(name),
        street_key=normalize_street(street),
        city_key=normalize_text(city),
        state_key=normalize_text(state),
        zip_key=normalize_zip(zip_code),
    )


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.upper().replace("&", " AND ")
    cleaned = re.sub(r"[^A-Z0-9]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_zip(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^0-9]", "", value)[:5]


def normalize_name(value: str) -> str:
    normalized_tokens: list[str] = []
    for token in normalize_text(value).split():
        mapped = TOKEN_NORMALIZATIONS.get(token, token)
        if mapped and mapped not in LEGAL_SUFFIXES:
            normalized_tokens.append(mapped)
    return " ".join(normalized_tokens)


def normalize_street(value: str | None) -> str:
    normalized_tokens: list[str] = []
    for token in normalize_text(value).split():
        normalized_tokens.append(STREET_NORMALIZATIONS.get(token, token))
    return " ".join(normalized_tokens)


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return SequenceMatcher(a=left, b=right).ratio()


def compare_records(left: SourceRecord, right: SourceRecord) -> MatchDecision:
    if left.source_system == right.source_system:
        return MatchDecision(False, 0.0, "cross_source_only")

    name_similarity = similarity(left.name_key, right.name_key)
    street_similarity = similarity(left.street_key, right.street_key)
    city_similarity = similarity(left.city_key, right.city_key)
    same_state = bool(left.state_key and left.state_key == right.state_key)
    same_zip = bool(left.zip_key and left.zip_key == right.zip_key)
    exact_name = bool(left.name_key and left.name_key == right.name_key)
    strong_street_match = bool(left.street_key and right.street_key and street_similarity >= 0.9)

    score = round(
        min(
            (0.65 * name_similarity)
            + (0.1 * street_similarity)
            + (0.1 * city_similarity)
            + (0.1 if same_state else 0.0)
            + (0.05 if same_zip else 0.0),
            0.999,
        ),
        3,
    )

    if exact_name and same_state and (same_zip or strong_street_match or city_similarity >= 0.9):
        return MatchDecision(True, max(score, 0.985), "exact_name_state")
    if exact_name and same_zip and (strong_street_match or city_similarity >= 0.9):
        return MatchDecision(True, max(score, 0.975), "exact_name_zip_city")
    if name_similarity >= 0.94 and same_state and same_zip and (strong_street_match or city_similarity >= 0.85):
        return MatchDecision(True, max(score, 0.955), "fuzzy_name_state_zip")
    if name_similarity >= 0.97 and same_state and (strong_street_match or city_similarity >= 0.85):
        return MatchDecision(True, max(score, 0.95), "fuzzy_name_city_state")

    return MatchDecision(False, score, "singleton")


def find_matches(records: list[SourceRecord]) -> tuple[dict[str, set[str]], dict[str, MatchDecision]]:
    ordered_records = sorted(records, key=lambda record: (record.source_system, record.source_record_id))
    neighbors = {record.key: set() for record in ordered_records}
    best_decisions = {record.key: MatchDecision(False, 0.0, "singleton") for record in ordered_records}

    for index, left in enumerate(ordered_records):
        for right in ordered_records[index + 1 :]:
            decision = compare_records(left, right)
            if not decision.matched:
                continue

            neighbors[left.key].add(right.key)
            neighbors[right.key].add(left.key)

            if decision.score > best_decisions[left.key].score:
                best_decisions[left.key] = decision
            if decision.score > best_decisions[right.key].score:
                best_decisions[right.key] = decision

    return neighbors, best_decisions


def build_groups(records: list[SourceRecord], neighbors: dict[str, set[str]]) -> list[list[SourceRecord]]:
    records_by_key = {record.key: record for record in records}
    groups: list[list[SourceRecord]] = []
    visited: set[str] = set()

    for record in sorted(records, key=lambda item: (item.source_system, item.source_record_id)):
        if record.key in visited:
            continue

        stack = [record.key]
        component: list[SourceRecord] = []
        visited.add(record.key)

        while stack:
            current_key = stack.pop()
            component.append(records_by_key[current_key])
            for neighbor_key in sorted(neighbors[current_key]):
                if neighbor_key not in visited:
                    visited.add(neighbor_key)
                    stack.append(neighbor_key)

        groups.append(sorted(component, key=lambda item: (item.source_system, item.source_record_id)))

    return groups


def choose_canonical_name(records: list[SourceRecord]) -> str:
    ranked = sorted(
        records,
        key=lambda record: (
            SOURCE_PRIORITY.get(record.source_system, 99),
            -len(normalize_text(record.name)),
            normalize_text(record.name),
            record.source_record_id,
        ),
    )
    return ranked[0].name.strip()


def build_entity_rows(groups: list[list[SourceRecord]]) -> list[EntityRow]:
    entities: list[EntityRow] = []
    for group in groups:
        cluster_key = "|".join(f"{record.source_system}:{record.source_record_id}" for record in group)
        entity_id = uuid.uuid5(ENTITY_NAMESPACE, cluster_key)
        notes = None if len(group) == 1 else f"linked_{len(group)}_records"
        entities.append(
            EntityRow(
                entity_id=entity_id,
                canonical_name=choose_canonical_name(group),
                notes=notes,
            )
        )
    return entities


def build_link_rows(
    groups: list[list[SourceRecord]],
    best_decisions: dict[str, MatchDecision],
) -> list[LinkRow]:
    links: list[LinkRow] = []
    for group in groups:
        cluster_key = "|".join(f"{record.source_system}:{record.source_record_id}" for record in group)
        entity_id = uuid.uuid5(ENTITY_NAMESPACE, cluster_key)
        for record in group:
            decision = best_decisions[record.key]
            links.append(
                LinkRow(
                    link_id=uuid.uuid5(
                        LINK_NAMESPACE,
                        f"{entity_id}|{record.source_system}|{record.source_record_id}",
                    ),
                    entity_id=entity_id,
                    source_system=record.source_system,
                    source_record_id=record.source_record_id,
                    match_score=round(decision.score, 3),
                    match_method=decision.method,
                )
            )
    return links


def build_resolved_entities(records: list[SourceRecord]) -> tuple[list[EntityRow], list[LinkRow]]:
    neighbors, best_decisions = find_matches(records)
    groups = build_groups(records, neighbors)
    return build_entity_rows(groups), build_link_rows(groups, best_decisions)
