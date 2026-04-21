# iSimcha Take-Home: Entity Linking Across Public Data Sources

Thank you for your time. This assessment is designed to give you a realistic slice of the kind of work iSimcha does
every day: assembling a single, trustworthy view of real-world entities from multiple public data feeds that were never
designed to be linked together.

Budget roughly **4 hours** of focused work. If you finish faster, great; if a piece takes longer, document what you
would have done next rather than rushing. We care about how you think and the quality of your approach, not about
completing every possible improvement.

---

## Quick start

```sh
docker compose up --detach
python3 -m venv .venv && source .venv/bin/activate
pip install --editable '.[dev]'
pytest                           # Smoke tests should pass
python3 -m entity_linking.main   # Prints row counts
```

The database is pre-populated and checked into the repo (under `postgres-data/`), so there is no separate data load
step. The `.env` file in the repo root has the default connection settings (`postgres` / `postgres` on `localhost:5432`,
database `postgres`).

### Inspecting the database

From the command line, use the `psql` shell inside the already-running container:

```sh
docker compose exec --interactive --tty postgres psql --username=postgres
```

The [PostgreSQL extension](https://marketplace.visualstudio.com/items?itemName=ms-ossdata.vscode-pgsql) is provided as a
suggested extension to install. It has a preconfigured connection profile to connect to the database. Other Postgres
clients (DBeaver, TablePlus, DataGrip, JetBrains' Database tool, etc.) works also. Here is the full connection string.

```
postgresql://postgres:postgres@localhost:5432/postgres
```

---

## The problem

The database contains three independent source tables with no shared primary key:

| Source schema.table              | What it represents                     | Primary key       |
| -------------------------------- | -------------------------------------- | ----------------- |
| `sources.sec_companies`          | SEC EDGAR-style public company filings | `cik`             |
| `sources.state_registrations`    | Per-state business registry entries    | `registration_id` |
| `sources.usaspending_recipients` | Federal contract award recipients      | `uei`             |

Each table may refer to the same real-world organization under a different name, with a slightly different address, or
with a subtly different legal form (`Inc.` vs `Incorporated`, `&` vs `and`, abbreviated street types, ZIP+4 vs ZIP-5,
historical vs current addresses, etc.). Some real-world entities appear in all three sources, some in only two, and some
in only one. A handful of records are deliberately ambiguous: two companies with very similar names, or a common name
used by unrelated entities in different states.

Your job is to produce a linked view of the data.

## Deliverables

1. **A reproducible pipeline** that populates the two empty target tables:
   - `resolved.entities`: one row per distinct real-world entity.
   - `resolved.entity_source_links`: one row for every record in the three source tables, pointing at the entity it
     belongs to. Include a `match_score` (0.0–1.0) and a short `match_method` tag so we can see how each decision was
     made.

   Running the pipeline on a fresh database must yield the same output (deterministic; no hand tuning of specific
   records).

2. **A short write-up** (`SOLUTION.md`, under ~500 words) describing:
   - Your approach and why you chose it.
   - The trade-offs you made (precision vs. recall, build time vs. accuracy, etc.).
   - At least three example records you found hard to classify and how you handled them.
   - What you would do next with another day of work.

3. **Tests** demonstrating your linking logic works on at least a few representative cases (clear match, near-miss
   non-match, ambiguous match). Pytest is already configured.

You may use any open source libraries you like: `rapidfuzz`, `recordlinkage`, `splink`, `dedupe`, `usaddress`,
`nameparser`, `pandas`, pure SQL with `pg_trgm` / `fuzzystrmatch` / `unaccent` (all three extensions are already
enabled), or anything else. A short list of suggestions is in `pyproject.toml` under the `suggested` optional dependency
group; you are not required to use any of them.

## What we're looking for

- **Judgment**: your choice of features and scoring rules, and your ability to explain trade-offs.
- **Data understanding**: evidence that you actually looked at the data, noticed the kinds of variation present, and
  designed around them.
- **Engineering quality**: a pipeline that is reproducible, tested, and would be comprehensible to a teammate six months
  from now.
- **Handling of ambiguity**: you do not need perfect precision or recall. We want to see that you recognize ambiguity,
  make defensible calls, and surface uncertainty through `match_score` and `match_method`.

## What we're NOT looking for

- A production-grade entity resolution system.
- Perfect matches on every record.
- Heavy frameworks where something simple would do.
- Novel algorithms. Standard techniques applied well are the goal.

## Submitting

Fork this repository on GitHub, create a branch for your work, and open a pull request back to this repo when you are
ready for review. Include your write-up as `SOLUTION.md` in the repo root. If you iterated on your approach, your
commit history is welcome; we welcome your creativity.

Questions? Reach out at any point. Good luck.
