WITH base AS (
    SELECT
        craft,
        count() AS people_count,
        max(_inserted_at) AS last_seen_at
    FROM people
    GROUP BY craft
)
SELECT *
FROM base
