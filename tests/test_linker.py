from __future__ import annotations

from entity_linking.linker import build_resolved_entities, compare_records, make_source_record


def record(
    *,
    source_system: str,
    source_record_id: str,
    name: str,
    street: str,
    city: str,
    state: str,
    zip_code: str,
):
    return make_source_record(
        source_system=source_system,
        source_record_id=source_record_id,
        name=name,
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
    )


def test_clear_match_links_same_entity() -> None:
    sec = record(
        source_system="sec",
        source_record_id="1000001",
        name="ACME SEMICONDUCTOR CORP",
        street="1200 Innovation Dr, Suite 400",
        city="Sacramento",
        state="CA",
        zip_code="95054",
    )
    usasp = record(
        source_system="usaspending",
        source_record_id="UEI000100001",
        name="Acme Semiconductor Corp.",
        street="1200 Innovation Drive",
        city="Sacramento",
        state="CA",
        zip_code="95054",
    )

    decision = compare_records(sec, usasp)

    assert decision.matched is True
    assert decision.method == "exact_name_state"
    assert decision.score >= 0.985


def test_suffix_and_abbreviation_match() -> None:
    sec = record(
        source_system="sec",
        source_record_id="1000045",
        name="BAKER AND HOLT INDUSTRIES INC",
        street="45 Commerce Street",
        city="Cincinnati",
        state="OH",
        zip_code="44114-2210",
    )
    usasp = record(
        source_system="usaspending",
        source_record_id="UEI000100023",
        name="Baker Holt Industries",
        street="45 Commerce Street",
        city="Cincinnati",
        state="OH",
        zip_code="44114",
    )

    decision = compare_records(sec, usasp)

    assert decision.matched is True
    assert decision.method == "exact_name_state"
    assert decision.score >= 0.985


def test_near_miss_non_match_stays_separate() -> None:
    left = record(
        source_system="sec",
        source_record_id="2000001",
        name="ATLANTIC COAST FREIGHT LLC",
        street="88 Shore Road",
        city="Raleigh",
        state="NC",
        zip_code="28401",
    )
    right = record(
        source_system="usaspending",
        source_record_id="UEI2000001",
        name="ATLANTIC COASTAL FREIGHT",
        street="200 Harbor Boulevard",
        city="Princeton",
        state="NJ",
        zip_code="07302",
    )

    decision = compare_records(left, right)

    assert decision.matched is False
    assert decision.method == "singleton"
    assert decision.score < 0.95


def test_ambiguous_roman_numeral_case_does_not_merge() -> None:
    left = record(
        source_system="sec",
        source_record_id="3000002",
        name="WESTPOINT INDUSTRIAL REALTY II LP",
        street="1 Park Avenue",
        city="New York",
        state="NY",
        zip_code="10017",
    )
    right = record(
        source_system="usaspending",
        source_record_id="UEI3000003",
        name="Westpoint Industrial Realty III",
        street="1 Park Avenue",
        city="Albany",
        state="NY",
        zip_code="10017",
    )

    decision = compare_records(left, right)

    assert decision.matched is False
    assert decision.method == "roman_numeral_mismatch"


def test_build_resolved_entities_creates_one_cluster_for_true_match() -> None:
    sec = record(
        source_system="sec",
        source_record_id="1000125",
        name="DELTA LOGISTICS LLC",
        street="9090 Industrial Blvd, Ste 200",
        city="Houston",
        state="TX",
        zip_code="75019",
    )
    usasp = record(
        source_system="usaspending",
        source_record_id="UEI000100045",
        name="DELTA LOGISTICS LLC",
        street="9090 Industrial Boulevard, Suite 200",
        city="Houston",
        state="TX",
        zip_code="75019",
    )
    unrelated = record(
        source_system="state",
        source_record_id="TX-999999",
        name="DELTA TRANSPORT SERVICES LLC",
        street="100 Main Street",
        city="Dallas",
        state="TX",
        zip_code="75201",
    )

    entities, links = build_resolved_entities([sec, usasp, unrelated])

    assert len(entities) == 2
    assert len(links) == 3

    entity_ids_by_source = {f"{link.source_system}:{link.source_record_id}": link.entity_id for link in links}
    assert entity_ids_by_source["sec:1000125"] == entity_ids_by_source["usaspending:UEI000100045"]
    assert entity_ids_by_source["state:TX-999999"] != entity_ids_by_source["sec:1000125"]
