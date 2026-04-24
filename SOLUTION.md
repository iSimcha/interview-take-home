# Solution: Entity Linking Across Public Data Sources

## Approach

The pipeline works in four stages:

1. **Load and normalize**: Pull all 500 records from the three source tables
   into a common format. Normalize company names by lowercasing, stripping legal
   suffixes (Inc, Corp, LLC, etc.) entirely, replacing "&" with "and", and
   removing punctuation. Normalize addresses by abbreviating street types
   (Boulevard -> Blvd, etc.) and stripping suite/floor info. Normalize ZIP codes
   to 5 digits. Extract trailing Roman numerals or numbers from names to prevent
   false merges (e.g., "Realty II" vs "Realty III").

2. **Pairwise matching**: Compare every pair of records using fuzzy string
   matching (rapidfuzz). The matching uses a tiered scoring system:
   - Exact normalized name, same state → score 1.0
   - Exact name, different state, confirmed by matching address → score 0.95
   - Exact name, different state, no address match → no match
   - High fuzzy match (>=85%) with only filler word differences → match if
     same state or address confirms
   - High fuzzy match with meaningful word differences → only match with
     address confirmation
   - Medium fuzzy match (>=65%) with strong address match in same state →
     match with moderate confidence
   - Different trailing numbers/roman numerals → never match

3. **Parent name linking**: USAspending records include a `parent_name` field.
   After standard matching, the pipeline links USAspending subsidiaries to their
   parent entities by fuzzy matching `parent_name` against all other normalized
   names (threshold 0.85).

4. **Clustering and output**: Union-Find groups matched records into clusters.
   For each cluster, a canonical name is chosen (preferring SEC names as the most
   formal source), a deterministic entity ID is generated via uuid5 on sorted
   source keys, and results are written to the resolved tables.

## Trade-offs

**Precision over recall**: Same-name matches across states are rejected when
addresses don't confirm. This may miss real matches where a company moved, but
avoids merging unrelated companies sharing a common name.

**Word-level diff checking**: Instead of relying purely on fuzzy scores, the
pipeline checks whether names differ by meaningful words or just filler words
("and", "of", "the"). "Baker and Holt" vs "Baker Holt" differs only by a
filler word and matches. "Atlantic Coastal Freight" vs "Atlantic Coast Freight"
differs by a meaningful word and requires address confirmation.

**O(n²) pairwise comparison**: With 500 records this runs in under a second.
For larger datasets blocking would be needed.

**Agent address for state_registrations**: This table stores the registered
agent's address, not the company's operating address. This makes address
confirmation less reliable for state records, so name similarity carries
more weight in those matches.

## Hard Cases

1. **Summit Consulting Group**: Two different entities share the same base name
   in different states (DC and CO). After suffix removal both normalize to
   "summit consulting group". Solved by rejecting exact-name matches across states
   without address confirmation.

2. **Atlantic Coast vs Atlantic Coastal Freight**: Two genuinely different
   companies with ~96% fuzzy name similarity. "Atlantic Coast Freight" (NC) and
   "Atlantic Coastal Freight" (NJ) are separated because "coastal" vs "coast" is
   a meaningful word difference requiring address confirmation, which fails.

3. **Westpoint Industrial Realty II vs III**: Same base name, different Roman
   numeral suffix. The pipeline extracts trailing numerals and blocks matches when
   they differ, regardless of name similarity.

4. **Riverstone Federal Programs / Riverstone Holdings**: The USAspending
   record for "Riverstone Federal Programs LLC" carries a `parent_name` of
   "Riverstone Holdings Corp." The parent name linking step catches this and
   correctly links the subsidiary to the parent entity.

## What I Would Do Next

- **Blocking**: Only compare records sharing the first few characters of the
  normalized name or the same ZIP, reducing comparisons from O(n²).
- **Address parsing**: Use `usaddress` to parse addresses into components
  (street number, street name) for more precise comparison.
- **Confidence review**: Generate a report of low-confidence matches (0.70–0.85)
  for human review before committing to the resolved tables.
- **Incremental updates**: Support adding new records without reprocessing
  the full dataset.
