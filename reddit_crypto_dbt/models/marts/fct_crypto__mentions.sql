WITH all_counts AS(
    SELECT *
    FROM {{ref('int_posts_mentions')}} AS pm
    JOIN {{ref('int_comment_mentions')}} as cm
    ON pm.post_title = cm.post_title
)
SELECT post_title, COUNT(*)
FROM {{ ref('int_comment_mentions') }}
GROUP BY post_title
HAVING COUNT(*) > 1;