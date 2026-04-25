# Name: Harsh Manoj More

# My approach:
I used a greedy incremental entity resolution approach. From initial SQL exploration, I observed that the `state_registrations` table had 220 rows with no duplicate entities, making it a reliable anchor dataset. The pipeline begins by seeding entities from `state_registrations` then incrementally incorporates records from `sec_companies`, followed by `usaspending_recipients`. Each new record is matched against the existing entity set. If a strong match is found, it links to an existing entity, otherwise a new entity is created. 

## Normalization:
Before matching, all inputs are standardized:
- Entity names: punctuation removal, uppercase conversion, suffix/abbreviation normalization
- Addresses: standardized formatting
- ZIP codes: reduced to first 5 digits



## The logic for finiding bect matching entities: 
The pipeline applies hierarchical matching rules:

1. Exact Match (match_score = 1.0)
    If all fields match exactly (name + address + zip), entities are merged with `exact_match`.

2. Parent Match (`parent_match` / `parent_fuzzy_match`)
    If parent_name matches entity name:
    - Exact match -> score = 1.0
    - Fuzzy match ≥ 0.95 -> `parent_fuzzy_match`
        This acts as a high-confidence shortcut for hierarchical relationships.

3. Fuzzy Name + Same Location (`name_fuzzy_match_same_location`)
    If name similarity ≥ 0.85 and address matches exactly, score = 0.9.
    
4. General Fuzzy Match (`fuzzy_match`)
    If:
    - name fuzzy match score
    - street fuzzy match score  
    - city/state/zip match exactly
    
    Weighted scoring:
    - name: 0.5
    - street: 0.2
    - other address fields: 0.3
    
    Final score must exceed 0.75.

5. No Match (no_match)
    Applied when parameters are insufficient or conflicting (e.g., same name different location or same location different name or totally different entities).
        
### !Note: All the cases described above here, are covered in the `test_pipeline.py` file.   
### !Note: Because I am on windows machine, I changed POSTGRES_UID and POSTGRES_GID to 0.
### !Note: the `postgres-data` directory was corrupted for me, due to which PostgreSQL is crashing on startup, so I had to recreate it empty so Docker could reinitialize. 



# Trade-offs:

## precision vs. recall.
The pipeline is tuned slightly toward precision over recall:
- High-confidence rules (exact match, parent match, strict location match) prevent incorrect merges.
- A relatively high threshold for fuzzy matching (≥ 0.85 in strong cases, ≥ 0.75 for acceptance) avoids over-clustering unrelated entities.
- This might result in some duplicate entities, but reduces harmful false merges.

## simplicity vs. coverage:
I intentionally avoided heavier frameworks (e.g., probabilistic record linkage libraries or complex data structures) in favor of:
- transparent rule-based logic
- interpretable scoring
- deterministic behavior
- code understandability and readability
This makes the pipeline easier to debug and review.



# Three example records:

## Ambiguous case1: Same name, different entities
The name `SUMMIT CONSULTING GROUP` appeared across all three sources (sec_companies, state_registrations, and usaspending_recipients), but corresponded to two distinct entities. The pipeline correctly produced two separate entities:
One entity linked records from DC-based sources with perfect match scores (1.0). Another entity linked records from Colorado with slightly lower fuzzy match scores (0.9).

Despite identical names and high similarity scores, the entities were not merged because their underlying records originated from different seeds and had differing locations. This demonstrates a deliberate design choice to prioritize precision over recall, avoiding false positives that could incorrectly merge unrelated organizations.

## Ambiguous case2: Parent-child relationships
Several records in the usaspending_recipients table contained a parent_name field that differed significantly from the entity's own name. For example, a record linked via parent_match to `RIVERSTONE HOLDINGS` despite having a different recipient name.
The pipeline explicitly incorporates parent-based matching, using both exact (parent_match) and fuzzy (parent_fuzzy_match) strategies. This allowed it to correctly associate subsidiaries or divisions with their parent organizations even when direct name matching would fail. This approach improves recall for hierarchical entities while maintaining precision by separating exact and fuzzy parent matches.

## Ambiguous case 3: Same address, different entities (co-location ambiguity)
Some records in the dataset share identical or very similar street addresses but represent different legal entities. For example, entities located at `400 Madison Ave, New York` can belong to unrelated organizations operating within the same building. A concrete case in the data is `WESTPOINT INDUSTRIAL REALTY II (UEI000100818)`, which is correctly linked to its own entity despite sharing a common commercial address pattern with other potential organizations in the dataset.
The pipeline treats address as a weak signal and does not resolve entities based on location alone. It requires additional agreement from name similarity and source-level matching to avoid incorrectly merging co-located but independent organizations.

# What I Would Do Next
With more time, I would: 
- First I would again manually go throught the data and try to find some more hidden patterns, which could help me to better match similar entitites together. 
- Enhance normalization methods.
- Then I would try to learn weights automatically (with regression on similarity features) instead of fixed thresholds.

Some thing which I really want to try out is, creating a NLP + K-Mean pipeline to cluster similar entities together. For this I would first clean and normalize the addresses fields, then combine them together to form a single string. After that I would try either `TF-IDF` or `word2vec` to get word embeddings, then I would write a script to give me a single vector for entire address. I would mostly prefer `word2vec` as it would make it easier to find similarities between two entities with 300 as vector size. And then I would be having 300 dimentional vctors for each entity, so applying K-means clustering would eventually club all the similar entities togheter, with this appraoch I could link entities with ease. And over here we can also consider iterative clustering approach to find better clusters. 

====================================================================================================




====================================================================================================

# Approach in detail:

* Iterate over available entities list (this is updated first with rows from `state_registrations` table, then `sec_companies` and then `usaspending_recipients`) first pipeline would check if all field are matching, if yes then we know for sure that those entities are same. So pipeline would give a `match_score` od 1.0 and `mnatch_method` as `exact_match`. 
_
* Then pipeline would check for parent name column which is present in `usaspending_recipients` table, it checks if the entity name matches any of the parent entity name, if yes, then there thise are considered as same entities and based on macth score either `parent_match` or `parent_fuzzy_match` is given for `match_method`. If parent name is eactly matching entity name then a score of 1.0 is given, and if the match score for parent name and entity name is greater than 0.95 then `parent_fuzzy_match` is given. This I deliberately kept high, to it forces to have maximum similarity. As this is  a high confident rule, which directly short circuits the scoring process.
_
* After that the pipeline considers if the name is fuzzy macth but rest of the address details match exactly, so here pipeline would return `name_fuzzy_match_same_location`. Over here if the names are matching with a score greater than or equal to 0.85 and given that other address parameters are matching exactly, indicates that the entities are same only. So, I gave a higher pre-defined match score of 0.9 and methid as `name_fuzzy_match_same_location`.
_
* The other case could be both entity name and street address is a fuzzy match and city, state, and zip code are exact match, then if the match score in this case is greater that 0.75 pipeline gives `fuzzy_match` as `match_method`. Since, rest of the address details are matching exactly I gave them total weight of 0.3, While name gets the highest weight of 0.5 and 0.2 for street address. This weighted scoring helps to find macthing entites only if the total score exceed 0.75, which ensures that both name and street address should have similarities other wise there would be no match. 
_
* In other cases the entities might not be related to each other. These cases could be 
    1. having same name with different location
    2. differnt entities, but same location
    3. or else nothing is matching
for these cases the pipeline would give `no_match` as `match_method`.
Hence, the weighted scoring function should balance these signals, while high-confidence rules (exact match and parent match) short-circuit the scoring process.