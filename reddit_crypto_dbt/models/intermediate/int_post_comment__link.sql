with posts as(
    SELECT 
        post_id,
        post_title
    FROM {{ ref('stg_reddit__posts') }}
),
comments as (
    SELECT 
        comment_id,
        post_title as comment_post_title
    FROM {{ ref('stg_reddit__comments')}}
),
matched_title as (
    SELECT 
        c.comment_id,
        p.post_id,
        p.post_title
    FROM comments c 
    LEFT JOIN posts p ON c.comment_post_title = p.post_title
),
ranked AS (
    SELECT
        comment_id,
        post_id,
        post_title,
        ROW_NUMBER() OVER (PARTITION BY comment_id ORDER BY post_id) AS rn
    FROM matched_title
)
SELECT 
    comment_id,
    post_id,
    post_title
FROM ranked
where rn = 1