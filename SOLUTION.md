# Solution: Entity Linking Across Public Data Sources

## Approach

The pipeline works in four stages:

1. **Load and normalize**: Pull all 500 records from the three source tables into a common format. Normalize company names by lowercasing, stripping legal suffixes (Inc, Corp, LLC, etc.), replacing "&" with "and", and removing punctuation. Normalize addresses by abbreviating street types (Boulevard -> Blvd, etc.) and stripping suite/floor info. Normalize ZIP codes to 5 digits. Extract trailing Roman numerals or numbers from names to prevent false merges (e.g., "Realty II" vs "Realty III").

2. **Pairwise matching**: Compare every pair of records using fuzzy string matching (rapidfuzz). The matching uses a tiered scoring system:
   - **Exact normalized name, same state** -> score 1.0
   - **Exact name, different state, confirmed by matching address** -> score 0.95
   - **Exact name, different state, no address match** -> no match (prevents false merges of companies with common names in different states)
   - **High fuzzy match (>=85%) with only filler word differences** -> match if same state or address confirms
   - **High fuzzy match with meaningful word differences** -> only match with address confirmation
   - **Medium fuzzy match (>=65%) with strong address match in same state** -> match with moderate confidence
   - **Different trailing numbers/roman numerals** -> never match regardless of name similarity

3. **Parent name linking**: USAspending records include a `parent_name` field. After standard matching, the pipeline links USAspending subsidiaries to their parent entities by fuzzy matching `parent_name` against all other normalized names.

4. **Clustering and output**: Use Union-Find to group matched records into clusters. For each cluster, pick a canonical name (preferring SEC names as the most formal source), generate a deterministic entity ID using uuid5, and write to the resolved tables.

## Trade-offs

- **Precision over recall**: I reject same-name matches across states when addresses don't confirm. This might miss real matches where a company moved, but it avoids merging unrelated companies that share a common name (like Summit Consulting Group in DC vs CO).

- **Word-level diff checking**: Instead of relying purely on fuzzy scores, I check whether names differ by meaningful words or just filler words ("and", "of", "the"). "Baker and Holt" vs "Baker Holt" differs only by a filler word, so it matches. "Atlantic Coastal Freight" vs "Atlantic Coast Freight" differs by a meaningful word, so it requires address confirmation.

- **O(n^2) pairwise comparison**: With 500 records this runs in seconds. For larger datasets I'd add blocking to reduce comparisons.

## Hard Cases

1. **Summit Consulting Group**: Two different entities share the same base name. "Summit Consulting Group Inc" (DC) and "Summit Consulting Group LLC" (CO) are separate companies. After suffix removal both normalize to "summit consulting group". Solved by rejecting exact-name matches across states without address confirmation. The USAspending records link correctly because they share addresses with their respective SEC records.

2. **Atlantic Coast vs Atlantic Coastal Freight**: Two genuinely different companies with very similar names (~96% fuzzy match). "Atlantic Coast Freight" operates in NC, "Atlantic Coastal Freight" operates in NJ. Solved by checking word-level differences: "coastal" vs "coast" is a meaningful word change, so address confirmation is required, and their addresses don't match.

3. **Westpoint Industrial Realty II vs III**: Same base name, different Roman numeral suffix. The pipeline extracts trailing numerals and blocks matches when they differ, regardless of how similar the rest of the name is.

4. **Riverstone Federal Programs / Riverstone Holdings**: The USAspending record for "Riverstone Federal Programs LLC" has a `parent_name` of "Riverstone Holdings Corp." The parent name linking step catches this and links the subsidiary to the parent entity.

## What I'd Do Next

- **Blocking**: Only compare records sharing the first few characters of the normalized name or the same state, reducing comparisons from O(n^2).

- **Address parsing**: Use `usaddress` to parse addresses into components (street number, street name, city) for more precise comparison.

- **Confidence review**: Generate a report of low-confidence matches (0.70-0.85) for human review.

- **Incremental updates**: Support adding new records without reprocessing the full dataset.

## Final Thoughts

This was a fun problem to work on. It's the kind of messy, real-world data challenge that doesn't have a clean textbook answer, and I enjoyed digging into the data and figuring out where the edge cases were. The Summit Consulting Group situation was a good reminder that sometimes the simplest matching rule (same name = same company) can be wrong, and that context like address and state matters a lot. I'd be excited to work on problems like this at iSimcha, especially in the clinical trial space where getting entity resolution right has a direct impact on whether patients find the right trials.