{{ config(materialized='view') }}

WITH source as (
    SELECT *
    FROM {{ source ('reddit_data', 'raw_reddit_comments')}}
),
ranked as(
    select *,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_utc) as rn
    FROM source
)
SELECT
    TRIM(CAST(id as STRING)) AS comment_id,
    TRIM(CAST(REGEXP_REPLACE(link_id, r'^t3_', '')as string)) AS post_id,
    TIMESTAMP_SECONDS(CAST(created_utc AS INT64)) AS comment_time,
    TRIM(CAST(author AS STRING)) AS comment_author,
    TRIM(CAST(body AS STRING)) AS comment_text,
    CAST(ups AS INT64) as num_comment_upvotes,
    TRIM(CAST(link_title AS STRING)) AS post_title
FROM ranked
WHERE rn = 1