-- Durable clock and deduplication belong to Camel, not the banking core.
CREATE TABLE camel_ingest.clock (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    control_date DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO camel_ingest.clock(singleton) VALUES (TRUE);
CREATE TABLE camel_ingest.batch (
    batch_date DATE PRIMARY KEY,
    closed BOOLEAN NOT NULL DEFAULT FALSE,
    extracted BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK (NOT extracted OR closed)
);
CREATE TABLE camel_ingest.receipt (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id = 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    batch_date DATE NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (topic, partition_id, source_offset)
);
CREATE TABLE camel_ingest.record (
    batch_date DATE NOT NULL REFERENCES camel_ingest.batch,
    kind TEXT NOT NULL CHECK (kind IN ('customer','account','relationship','transaction')),
    business_key TEXT NOT NULL,
    fixed_line TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    PRIMARY KEY (batch_date, kind, business_key),
    FOREIGN KEY (topic, partition_id, source_offset)
        REFERENCES camel_ingest.receipt(topic, partition_id, source_offset)
);
CREATE VIEW camel_ingest.customer AS SELECT * FROM camel_ingest.record WHERE kind = 'customer';
CREATE VIEW camel_ingest.account AS SELECT * FROM camel_ingest.record WHERE kind = 'account';
CREATE VIEW camel_ingest.relationship AS SELECT * FROM camel_ingest.record WHERE kind = 'relationship';
CREATE VIEW camel_ingest.transaction AS SELECT * FROM camel_ingest.record WHERE kind = 'transaction';
CREATE TABLE camel_ingest.publication (
    filename TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
