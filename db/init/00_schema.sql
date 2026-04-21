-- iSimcha entity linking assessment: schema
--
-- Three source tables hold records from independent public feeds. There is no
-- shared primary key across the sources. The candidate is expected to populate
-- the two target tables in the `resolved` schema with their entity resolution
-- output.

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE SCHEMA IF NOT EXISTS sources;
CREATE SCHEMA IF NOT EXISTS resolved;

-- Source 1: SEC EDGAR-style public company filings
CREATE TABLE sources.sec_companies (
    cik              BIGINT PRIMARY KEY,
    company_name     TEXT   NOT NULL,
    ticker           TEXT,
    sic_code         TEXT,
    state_of_inc     TEXT,
    street           TEXT,
    city             TEXT,
    state            TEXT,
    zip_code         TEXT,
    last_filed_date  DATE
);

-- Source 2: State business registry filings
CREATE TABLE sources.state_registrations (
    registration_id  TEXT PRIMARY KEY,
    jurisdiction     TEXT NOT NULL,
    entity_name      TEXT NOT NULL,
    entity_type      TEXT,
    agent_name       TEXT,
    agent_street     TEXT,
    agent_city       TEXT,
    agent_state      TEXT,
    agent_zip        TEXT,
    formation_date   DATE,
    status           TEXT
);

-- Source 3: USAspending.gov-style federal contract recipients
CREATE TABLE sources.usaspending_recipients (
    uei              TEXT PRIMARY KEY,
    recipient_name   TEXT   NOT NULL,
    parent_name      TEXT,
    street           TEXT,
    city             TEXT,
    state            TEXT,
    zip_code         TEXT,
    total_awards     NUMERIC(18, 2),
    last_award_date  DATE
);

-- Target table: the canonical deduplicated list of real-world entities.
-- Populated by the candidate.
CREATE TABLE resolved.entities (
    entity_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name  TEXT NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Target table: maps each source record to exactly one entity.
-- Populated by the candidate.
CREATE TABLE resolved.entity_source_links (
    link_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id        UUID NOT NULL REFERENCES resolved.entities(entity_id) ON DELETE CASCADE,
    source_system    TEXT NOT NULL CHECK (source_system IN ('sec', 'state', 'usaspending')),
    source_record_id TEXT NOT NULL,
    match_score      NUMERIC(4, 3),
    match_method     TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_record_id)
);

CREATE INDEX idx_entity_source_links_entity ON resolved.entity_source_links (entity_id);
CREATE INDEX idx_sec_name_trgm              ON sources.sec_companies          USING gin (company_name   gin_trgm_ops);
CREATE INDEX idx_state_name_trgm            ON sources.state_registrations    USING gin (entity_name    gin_trgm_ops);
CREATE INDEX idx_usasp_name_trgm            ON sources.usaspending_recipients USING gin (recipient_name gin_trgm_ops);
