CREATE TABLE IF NOT EXISTS raw_astros
(
    id UInt64,
    raw_json String,
    _inserted_at DateTime
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS people
(
    craft String,
    name String,
    _inserted_at DateTime
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (craft, name);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_to_people
TO people AS
SELECT
    JSONExtractString(person, 'craft') AS craft,
    JSONExtractString(person, 'name') AS name,
    _inserted_at
FROM raw_astros
ARRAY JOIN JSONExtractArrayRaw(raw_json, 'people') AS person;
