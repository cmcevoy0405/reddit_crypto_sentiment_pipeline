WITH posts AS(
    SELECT *
    FROM {{ ref('stg_reddit__posts') }}
),
comments AS(
    SELECT *
    FROM {{ ref('stg_reddit__comments')}}
)
SELECT 
    post_id,
    comment_id,
    post_time,
    
