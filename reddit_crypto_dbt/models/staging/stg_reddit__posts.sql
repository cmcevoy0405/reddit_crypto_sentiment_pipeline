{{ config(materialized='view') }}

WITH source AS(
    SELECT *
    FROM {{ source('reddit_data', 'raw_reddit_posts') }}
),
ranked as (
    select 
        *,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_utc desc) AS rn
    FROM source
)
SELECT 
    TRIM(CAST(id as STRING)) AS post_id,
    TIMESTAMP_SECONDS(CAST(created_utc AS INT64)) AS post_time,
    TRIM(CAST(author AS STRING)) AS post_author,
    TRIM(CAST(title AS STRING)) AS post_title, 
    TRIM(CAST(selftext AS STRING)) AS body_text, 
    TRIM(CAST(link_flair_text AS STRING)) as flair,
    CAST(upvote_ratio AS FLOAT64) as upvote_ratio, 
    CAST(score AS INT64) as total_votes, 
    CAST(num_comments AS INT64) as num_comments
FROM ranked
where rn = 1