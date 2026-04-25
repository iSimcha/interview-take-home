from uuid import uuid4
from rapidfuzz import fuzz

from entity_linking.normalizer import normalize_name, normalize_address, normalize_zip


def find_best_match(name, street, city, state, zip_code, parent_name, entities):

    best_match = None
    best_score = 0
    best_method = "no_match"

    for ent in entities:

        ent_name = ent["name"]
        ent_street = ent.get("street", "")
        ent_city = ent.get("city", "")
        ent_state = ent.get("state", "")
        ent_zip = ent.get("zip", "")

        # If the entity name, street, city, state, zip code matches exactly => so same entity
        if (name == ent_name and
            street == ent_street and
            city == ent_city and
            state == ent_state and
            zip_code == ent_zip
        ):
            return ent, 1.0, "exact_match"

        if parent_name and ent_name:
            parent_score = fuzz.token_set_ratio(parent_name, ent_name) / 100

            if parent_name == ent_name:
                return ent, 1.0, "parent_match"

            if parent_score >= 0.95:
                return ent, 0.95, "parent_fuzzy_match"

        name_score = fuzz.token_set_ratio(name, ent_name) / 100 if name and ent_name else 0

        # If the street, city, state and zip matches exactly, but name is fuzzy matches => highly probability same entity
        if (name_score >= 0.85 and
            street and ent_street and street == ent_street and 
            city and ent_city and city == ent_city and
            state and ent_state and state == ent_state and
            zip_code and ent_zip and zip_code == ent_zip
        ):
            return ent, 0.90, "name_fuzzy_match_same_location"

        street_score = fuzz.token_set_ratio(street, ent_street) / 100 if street and ent_street else 0

        city_match = 1.0 if city and ent_city and city == ent_city else 0.0
        state_match = 1.0 if state and ent_state and state == ent_state else 0.0
        zip_match = 1.0 if zip_code and ent_zip and zip_code == ent_zip else 0.0

        # If the name and street are fuzzy matches, but rest of the address is same => generalized matching with weighted scoring
        score = (
            0.5 * name_score +
            0.2 * street_score +
            0.10 * city_match +
            0.10 * state_match +
            0.10 * zip_match
        )

        if score > best_score:
            best_score = score
            best_match = ent
            best_method = "fuzzy_match"

    return best_match, best_score, best_method



def run_pipeline(conn, sec_df, state_df, usa_df):

    entities = []
    links = []

    with conn.cursor() as cur:
        cur.execute("DELETE FROM resolved.entity_source_links;")
        cur.execute("DELETE FROM resolved.entities;")
        conn.commit()

    for _, row in state_df.iterrows():
        entity_id = str(uuid4())
        entities.append({
            "entity_id": entity_id,
            "name": normalize_name(row["entity_name"]),
            "street": normalize_address(row.get("agent_street")),
            "city": row.get("agent_city"),
            "state": row.get("agent_state"),
            "zip": normalize_zip(row.get("agent_zip"))
        })

        links.append({
            "entity_id": entity_id,
            "source_system": "state",
            "source_id": row["registration_id"],
            "match_score": 1.0,
            "match_method": "seed"
        })

    for _, row in sec_df.iterrows():
        name = normalize_name(row["company_name"])
        street = normalize_address(row.get("street"))
        city = row.get("city")
        state = row.get("state")
        zip_code = normalize_zip(row.get("zip_code"))
        parent_name = None

        match, score, method = find_best_match(
            name, street, city, state, zip_code, parent_name,
            entities=entities
        )

        if method == "no_match" or score < 0.75:
            entity_id = str(uuid4())
            method = "no_match"

            entities.append({
                "entity_id": entity_id,
                "name": name,
                "street": street,
                "city": city,
                "state": state,
                "zip": zip_code
            })
        else:
            entity_id = match["entity_id"]

        links.append({
            "entity_id": entity_id,
            "source_system": "sec",
            "source_id": str(row["cik"]),
            "match_score": float(score),
            "match_method": method
        })

    for _, row in usa_df.iterrows():
        name = normalize_name(row["recipient_name"])
        street = normalize_address(row.get("street"))
        city = row.get("city")
        state = row.get("state")
        zip_code = normalize_zip(row.get("zip_code"))
        parent_name = normalize_name(row.get("parent_name"))

        match, score, method = find_best_match(
            name, street, city, state, zip_code, parent_name,
            entities=entities
        )

        if method == "no_match" or score < 0.75:
            entity_id = str(uuid4())
            method = "no_match"

            entities.append({
                "entity_id": entity_id,
                "name": name,
                "street": street,
                "city": city,
                "state": state,
                "zip": zip_code
            })
        else:
            entity_id = match["entity_id"]

        links.append({
            "entity_id": entity_id,
            "source_system": "usaspending",
            "source_id": row["uei"],
            "match_score": float(score),
            "match_method": method
        })


    with conn.cursor() as cur:
        for ent in entities:
            cur.execute(
                """
                INSERT INTO resolved.entities (entity_id, canonical_name, notes)
                VALUES (%s, %s, %s)
                """,
                (ent["entity_id"], ent["name"], None)
            )

        for link in links:
            cur.execute(
                """
                INSERT INTO resolved.entity_source_links
                (entity_id, source_system, source_record_id, match_score, match_method)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    link["entity_id"],
                    link["source_system"],
                    link["source_id"],
                    link["match_score"],
                    link["match_method"]
                )
            )

        conn.commit()