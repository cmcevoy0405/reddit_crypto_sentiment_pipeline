WITH posts AS(
    SELECT *
    FROM {{ ref('stg_reddit__posts') }}
)
SELECT *
git 