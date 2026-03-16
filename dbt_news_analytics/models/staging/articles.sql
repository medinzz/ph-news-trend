-- To Clean the data from new lines and remove "Read Next" in content as it points to a different article.

UPDATE articles_raw
SET 
    -- 1. Split by 'Read Next' and take the first part
    -- 2. Replace double newlines with a single space (or '')
    -- 3. Trim the result
    content = TRIM(REPLACE(SPLIT_PART(content, 'Read Next', 1), E'\n\n', ' ')),
    title = TRIM(REPLACE(title, E'\n\n', ' '))
WHERE source = 'inquirer'
AND content LIKE '%Read Next%';