WITH
    source_data AS (
        SELECT
            * EXCEPT (tags),
            LOWER(REPLACE(tags, '-', ' ')) AS tags
        FROM {{ source('ph_news', 'articles_raw') }}
    )
SELECT
    DISTINCT
    id,
    source,
    url,
    title,
    category,
    author,
    date,
    publish_time,
    content,
    tags,
FROM source_data
