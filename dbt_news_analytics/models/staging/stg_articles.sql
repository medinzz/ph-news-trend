-- stg_articles: clean articles_raw
SELECT
    * EXCLUDE (title, tags, content),
    TRIM(title) AS title,
    STRING_SPLIT(tags, ',') AS tags,
    SPLIT_PART(content, 'Read Next', 1) AS content
FROM {{ source('ph_news_raw', 'articles_raw') }}