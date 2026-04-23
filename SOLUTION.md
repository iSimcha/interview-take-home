# Solution: Entity Linking Across Public Data Sources

## Approach

The pipeline works in four stages:

1. **Load and normalize**: Pull all 500 records from the three source tables into a common format. Normalize company names by lowercasing, stripping legal suffixes (Inc, Corp, LLC, etc.), replacing "&" with "and", and removing punctuation. Normalize addresses by abbreviating street types (Boulevard -> Blvd, etc.) and stripping suite/floor info. Normalize ZIP codes to 5 digits.

2. **Pairwise matching**: Compare every pair of records using fuzzy string matching (rapidfuzz). The matching logic uses a tiered scoring system:
   - **Exact normalized name, same state** -> score 1.0
   - **Exact name, different state, confirmed by matching address** -> score 0.95
   - **Exact name, different state, no address match** -> no match (prevents false merges of distinct companies with similar names)
   - **High fuzzy name match (>=85%)** -> score based on similarity, boosted if address confirms, penalized if states differ without address confirmation
   - **Medium fuzzy match (>=65%) with strong address match** -> match with moderate confidence

3. **Clustering**: Use Union-Find to group matched records into clusters. Each cluster represents one real-world entity.

4. **Write results**: For each cluster, pick a canonical name (preferring SEC names as the most formal), create an entity row, and link all source records to it with their match scores and methods.

## Trade-offs

- **Precision over recall**: I chose to reject same-name matches when states differ and addresses don't confirm. This means we might miss a few real matches (e.g., a company that moved), but we avoid incorrectly merging two unrelated companies that happen to share a common name. For a system that needs to be trustworthy, false merges are worse than missed links.

- **O(n^2) pairwise comparison**: With 500 records, comparing all pairs (~125k comparisons) runs in a few seconds. This wouldn't scale to millions of records, but for this dataset it's simple and correct. With more time, I'd add blocking (only compare records that share the same state or first few characters of the name) to reduce comparisons.

- **Name normalization vs. matching**: I strip legal suffixes during normalization rather than during matching. This means "ACME SEMICONDUCTOR CORP" and "Acme Semiconductor Corporation" normalize to the same string and get an exact match. The downside is we lose the signal that one says "Corp" and the other says "Corporation", but in practice that distinction doesn't indicate different entities.

## Hard Cases

1. **Summit Consulting Group**: Appears as both "Summit Consulting Group Inc" (DC, 700 K Street) and "Summit Consulting Group LLC" (CO, 450 Mountain Rd). After normalization, both become "summit consulting group". My initial pass merged them into one entity. I fixed this by rejecting exact-name matches where states differ and addresses don't confirm. Now they're correctly split into two entities.

2. **Fulton Pharmaceuticals / Fulton Pharma**: The USAspending record uses "FULTON PHARMA", a significant abbreviation of "FULTON PHARMACEUTICALS, INC." The fuzzy name score alone (~68%) isn't high enough to match confidently, but the address match ("22 Technology Way", Newark, NJ) confirms the link. This is handled by the medium-name-strong-address tier, scoring 0.84.

3. **Atlantic Coast Freight vs. Atlantic Coastal Freight**: These are actually two different SEC records (CIK 1001316 and 1001275) with slightly different names and different addresses (Raleigh, NC vs. Princeton, NJ). The normalized names are similar but not identical ("atlantic coast freight" vs "atlantic coastal freight"), and the different addresses/states prevent a false merge.

## What I'd Do Next

- **Blocking**: Add a blocking step to only compare records that share the first 3-4 characters of the normalized name or the same state. This would reduce the number of comparisons from O(n^2) to something more manageable for larger datasets.

- **Address parsing**: Use the `usaddress` library to properly parse addresses into components (street number, street name, city, state, zip) for more precise address comparison instead of fuzzy matching the full string.

- **Confidence review**: Build a simple report of low-confidence matches (score 0.7-0.85) for human review. In production, you'd want a human-in-the-loop step for ambiguous cases.

- **Incremental updates**: Currently the pipeline does a full rebuild. For production use, I'd add incremental matching so new records can be linked without reprocessing everything.

## Final Thoughts

This was a fun problem to work on. It's the kind of messy, real-world data challenge that doesn't have a clean textbook answer, and I enjoyed digging into the data and figuring out where the edge cases were. The Summit Consulting Group situation was a good reminder that sometimes the simplest matching rule (same name = same company) can be wrong, and that context like address and state matters a lot. I'd be excited to work on problems like this at iSimcha, especially in the clinical trial space where getting entity resolution right has a direct impact on whether patients find the right trials.