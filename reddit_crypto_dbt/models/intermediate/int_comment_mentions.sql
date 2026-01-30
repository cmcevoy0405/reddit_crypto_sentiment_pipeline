{{ config(
    materialized='table'
) }}

WITH base AS (
  SELECT
    comment_id,
    comment_time,
    comment_text,
    num_comment_upvotes,
    post_title,
    LOWER(COALESCE(comment_text, '')) AS full_comment_text

  FROM {{ ref('stg_reddit__comments') }}
)
 SELECT
    comment_id,
    comment_time,
    num_comment_upvotes,
    post_title,
    full_comment_text,

  -- Mentions per coin
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(btc|bitcoin|btcusdt)\b')) AS btc_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(eth|ethereum|ethusdt)\b')) AS eth_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(sol|solana|solusdt)\b')) AS sol_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(xrp|ripple|xrpusdt)\b')) AS xrp_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_comment_text, r'\b(bnb|binance\scoin|bnbusdt)\b')) AS bnb_mention_count

FROM base
