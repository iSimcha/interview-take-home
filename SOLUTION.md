# Solution

## Approach

The pipeline is split across three modules: `normalize.py`, `linker.py`, and `main.py`.

**Normalization**: Company names are uppercased, punctuation stripped, legal suffixes removed entirely (so "Acme Corp" and "Acme Corporation" both become "ACME"), and common abbreviations expanded (TECH → TECHNOLOGY, NATL → NATIONAL). "AND" is dropped so "Baker and Holt" and "Baker Holt" normalize identically. Streets have suite/floor info stripped and type tokens abbreviated. ZIPs are truncated to 5 digits.

**Matching**: Records are compared pairwise across source systems only — never within the same source. Two hard blocks fire before scoring: same-source pairs are rejected immediately, and pairs where both names end in different roman numerals (II vs III) are blocked regardless of everything else. For remaining pairs, a weighted score is computed across five fields — name (65%), street (10%), city (10%), state (10%), ZIP (5%) — and matches are decided through four tiers from exact name + state + geo confirmation down to moderate fuzzy + strong address. A second pass links USAspending subsidiaries to parent entities via the `parent_name` field.

**Clustering**: Matched pairs form a graph. DFS finds connected components, each becoming one entity. IDs are generated via uuid5 over sorted source keys, making the pipeline fully deterministic.

## Trade-offs

Precision over recall — every tier requires geographic confirmation on top of name similarity. This avoids false merges at the cost of potentially missing matches where a company's address differs between sources.

O(n²) comparisons work fine for 500 records but wouldn't scale. For larger datasets I'd add blocking by state or ZIP before comparing pairs.

State registrations use agent addresses rather than company addresses, making address confirmation weaker for that source. Name similarity carries more weight there.

## Hard cases

**"Atlantic Coast Freight" vs "Atlantic Coastal Freight"**: These score ~95% on name similarity alone. The word-level diff check catches that COAST and COASTAL are meaningfully different tokens, and since they're in different states with no address overlap, the match is correctly rejected.

**"Westpoint Industrial Realty II" vs "Westpoint Industrial Realty III"**: Nearly identical names, same address. The roman numeral hard block handles this before any scoring happens.

**"Summit Consulting Group"** appears in both DC and CO as unrelated companies. Geographic confirmation is required even for identical normalized names, so they correctly remain separate entities.

**Riverstone Holdings / Riverstone Federal Programs**: Standard fuzzy matching won't link these since the names share little overlap. The parent_name linking pass catches it using the `parent_name` field in USAspending.

## What I'd do next

- Add blocking by state or ZIP to scale beyond a few thousand records
- Use `usaddress` to parse street addresses into components for more precise comparison
- Generate a review report of low-confidence matches for human inspection before committing

## Note on test suite

Running `pytest` on a populated database will show one expected failure: `test_resolved_tables_exist_and_empty` in `test_smoke.py`. This smoke test checks the resolved tables are empty on a fresh database — it passes before the pipeline runs and fails after, which is intended. All 11 tests in `test_linking.py` pass unconditionally.
