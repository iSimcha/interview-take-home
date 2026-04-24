# Solution Write-Up

## Approach

I built a multi-stage entity linking pipeline in Python that normalizes,
blocks, scores, and clusters records from three source tables into a single
resolved view.

**Stage 1 — Normalization**: Before any comparison, all records are normalized
to remove noise. Company names are lowercased, punctuation stripped, legal
suffixes standardized (`Inc.` → `inc`, `Corporation` → `corp`), and
ampersands expanded (`&` → `and`). Street addresses are similarly normalized
with common abbreviations (`Street` → `st`, `Parkway` → `pkwy`). ZIP codes
are truncated to 5 digits to handle ZIP+4 variants.

**Stage 2 — Blocking**: Rather than comparing all 500 records against each
other (125,000 pairs), I block by `state`. This reduces comparisons to only
records in the same state, which is a safe assumption — the same real-world
company will always be registered in the same state across sources.

**Stage 3 — Scoring**: Each candidate pair within a block is scored using a
weighted combination of fuzzy string similarity:

- Company name similarity (60% weight) — using `rapidfuzz` token sort ratio
- Street address similarity (25% weight)
- ZIP code exact match (15% weight)

Pairs scoring ≥ 0.85 are tagged `name_address_high`, pairs between 0.60–0.85
are tagged `name_address_fuzzy`, and below 0.60 are discarded.

**Stage 4 — Clustering**: Matched pairs are clustered using a union-find
(disjoint set) data structure. This handles transitive matches — if A matches
B and B matches C, all three are grouped into one entity. Each cluster gets
one canonical name (the longest normalized name in the cluster) inserted into
`resolved.entities`, and every source record gets a row in
`resolved.entity_source_links`.

## Trade-offs

**Precision vs. recall**: I tuned the 0.60 threshold conservatively to avoid
false positives. This means some genuine matches with abbreviated names or
different addresses may be missed, but avoids merging distinct companies
incorrectly. For this dataset size, false merges are more damaging than
missed links.

**Blocking strategy**: Blocking by state is fast and safe but misses cases
where a company is registered in a different state than it operates in. A
production system would use multiple blocking keys (e.g., ZIP code, first
token of name) and take the union.

**Address matching for state_registrations**: This table uses agent addresses
rather than company addresses, making address similarity less reliable. I
compensate by weighting name similarity higher (60%).

## Hard Cases

1. **Similar names, different entities**: Companies like `National Services
Inc` appear in multiple states with identical names but are unrelated
   businesses. Blocking by state handles this correctly — they never get
   compared.

2. **Abbreviated vs. full legal names**: `MEADOW CO CORP` vs `Meadow Company
Corporation` — normalization expands `co` → `co` and `corp` → `corp`
   consistently, so fuzzy matching correctly identifies these as the same.

3. **Agent address vs. company address**: `state_registrations` uses the
   registered agent's address, which may differ from the company's operating
   address in other sources. For these records, name similarity carries almost
   all the weight in the match score.

## What I Would Do Next

- Add a second blocking pass using ZIP code to catch cross-state matches
- Use `usaddress` library to parse and normalize street addresses more
  precisely (extract house number, street name separately)
- Tune thresholds using a small hand-labeled validation set
- Replace the flat 0.75 clustered score with the actual pairwise score for
  each record
- Add logging to surface low-confidence matches for human review
