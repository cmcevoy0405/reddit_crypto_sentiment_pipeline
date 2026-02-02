{{ config(
    materialized='table'
) }}

WITH comments AS (
  SELECT
    comment_id,
    post_id,
    comment_time,
    comment_text,
    num_comment_upvotes,
    post_title,
    LOWER(COALESCE(comment_text, '')) AS full_comment_text

  FROM {{ ref('stg_reddit__comments') }}
),
posts AS(
  SELECT 
    post_id,
    post_title
  FROM {{ref('stg_reddit__posts')}}
)
 SELECT
    c.comment_id,
    p.post_id,
    c.comment_time,
    c.num_comment_upvotes,
    c.full_comment_text,

  -- Mentions per coin
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(btc|bitcoin|btcusdt)\b')) AS btc_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(eth|ethereum|ethusdt)\b')) AS eth_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(sol|solana|solusdt)\b')) AS sol_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(xrp|ripple|xrpusdt)\b')) AS xrp_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(bnb|binance\scoin|bnbusdt)\b')) AS bnb_mention_count

FROM posts p
LEFT JOIN comments c
ON p.post_id = c.post_id
