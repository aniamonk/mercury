-- Mercury schema. Ports the Foundry datasets/ontology:
--   sources              <- sources_registry (+ feed state, replaces hardcoded outlet blocklist)
--   articles             <- cleaned_articles + enrichment columns (MediaReport)
--   entities             <- entities output (Entity); counts/tiers live in the entity_stats view
--   report_entity_links  <- reportEntityLinks
--   stories              <- top_stories_* outputs, cached per scope

CREATE TABLE IF NOT EXISTS sources (
    source_id          TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    source_type        TEXT NOT NULL,
    country            TEXT,
    language           TEXT,
    political_leaning  TEXT,
    feed_url           TEXT,
    active             INTEGER NOT NULL DEFAULT 1,
    poll_seconds       INTEGER,
    etag               TEXT,
    last_modified      TEXT,
    last_polled_at     TEXT,
    last_status        TEXT,
    notes              TEXT
);

CREATE TABLE IF NOT EXISTS articles (
    report_id        TEXT PRIMARY KEY,          -- sha256(url), as in Foundry
    title            TEXT NOT NULL,
    url              TEXT NOT NULL UNIQUE,
    source_id        TEXT REFERENCES sources(source_id),
    source_domain    TEXT,
    description      TEXT,
    language         TEXT,
    source_country   TEXT,
    published_date   TEXT,                      -- ISO-8601 UTC
    tone_score       REAL DEFAULT 0.0,          -- inherited placeholder, kept for schema fidelity
    query_term       TEXT DEFAULT 'rss_feed',
    data_source      TEXT DEFAULT 'rss',
    llm_input_text   TEXT,                      -- title | description, capped at 500 chars
    ingested_at      TEXT NOT NULL,
    cleaning_version TEXT,
    -- enrichment (filled by the enrich worker)
    summary          TEXT,
    topic            TEXT,
    subtopics        TEXT DEFAULT '[]',         -- JSON array from the controlled list
    distilled_topics TEXT DEFAULT '[]',         -- JSON array of dynamic article-level topic labels
    leaning          TEXT,
    status           TEXT NOT NULL DEFAULT 'new',   -- new | enriched | failed
    enriched_at      TEXT,
    enrich_error     TEXT
);
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_date);
CREATE INDEX IF NOT EXISTS idx_articles_topic ON articles(topic);

CREATE TABLE IF NOT EXISTS entities (
    entity_id       TEXT PRIMARY KEY,           -- sha256(canonical name)
    entity_name     TEXT NOT NULL UNIQUE,
    entity_type     TEXT,
    aliases         TEXT NOT NULL DEFAULT '[]', -- JSON array of observed surface forms
    first_seen_date TEXT
);

CREATE TABLE IF NOT EXISTS report_entity_links (
    report_id TEXT NOT NULL REFERENCES articles(report_id),
    entity_id TEXT NOT NULL REFERENCES entities(entity_id),
    linked_at TEXT,
    PRIMARY KEY (report_id, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_links_entity ON report_entity_links(entity_id);

CREATE TABLE IF NOT EXISTS stories (
    scope             TEXT NOT NULL,            -- domestic | poland_uk | focused
    story_id          TEXT NOT NULL,
    story_title       TEXT,
    synthesis         TEXT,
    framing_notes     TEXT,
    member_report_ids TEXT NOT NULL DEFAULT '[]',
    generated_at      TEXT NOT NULL,
    PRIMARY KEY (scope, story_id)
);

CREATE TABLE IF NOT EXISTS story_refresh_state (
    scope               TEXT PRIMARY KEY,
    last_attempted_at   TEXT NOT NULL,
    last_successful_at  TEXT,
    status              TEXT NOT NULL,          -- refreshed | empty | retained_error | retained_invalid
    eligible_articles   INTEGER NOT NULL DEFAULT 0,
    story_count         INTEGER NOT NULL DEFAULT 0,
    detail              TEXT
);

-- Live entity aggregates. In Foundry these were materialized per batch run;
-- computing them here means the trending chart is always current and
-- mention_velocity (a 0.0 placeholder in Foundry) actually works:
-- mentions in the last 24h vs the prior 7-day daily average.
DROP VIEW IF EXISTS entity_stats;
CREATE VIEW entity_stats AS
SELECT
    e.entity_id,
    e.entity_name,
    e.entity_type,
    e.aliases,
    e.first_seen_date,
    COUNT(l.report_id) AS total_mention_count,
    SUM(CASE WHEN datetime(a.published_date) >= datetime('now', '-24 hours') THEN 1 ELSE 0 END)
        AS recent_mention_count,
    CASE
        WHEN COUNT(l.report_id) > 20 THEN 'key_actor'
        WHEN COUNT(l.report_id) > 5 THEN 'regular'
        ELSE 'peripheral'
    END AS salience_tier,
    ROUND(
        CAST(SUM(CASE WHEN datetime(a.published_date) >= datetime('now', '-24 hours') THEN 1 ELSE 0 END) AS REAL)
        / MAX(1.0,
              CAST(SUM(CASE WHEN datetime(a.published_date) < datetime('now', '-24 hours')
                             AND datetime(a.published_date) >= datetime('now', '-8 days')
                        THEN 1 ELSE 0 END) AS REAL) / 7.0),
        2
    ) AS mention_velocity
FROM entities e
JOIN report_entity_links l ON l.entity_id = e.entity_id
JOIN articles a ON a.report_id = l.report_id
GROUP BY e.entity_id;

-- Full-text search over articles
CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
    title, description, summary,
    content='articles', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON articles BEGIN
    INSERT INTO articles_fts(rowid, title, description, summary)
    VALUES (new.rowid, new.title, new.description, new.summary);
END;
CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON articles BEGIN
    INSERT INTO articles_fts(articles_fts, rowid, title, description, summary)
    VALUES ('delete', old.rowid, old.title, old.description, old.summary);
    INSERT INTO articles_fts(rowid, title, description, summary)
    VALUES (new.rowid, new.title, new.description, new.summary);
END;
CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON articles BEGIN
    INSERT INTO articles_fts(articles_fts, rowid, title, description, summary)
    VALUES ('delete', old.rowid, old.title, old.description, old.summary);
END;
