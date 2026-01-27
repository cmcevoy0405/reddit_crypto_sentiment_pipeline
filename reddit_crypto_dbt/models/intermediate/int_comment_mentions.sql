{{ config(
    materialized='table'
) }}

WITH base AS (
  SELECT
    comment_id,
    comment_time,
    comment_text,
    post_title,
    num_comment_upvotes,
    
    -- Combine title + comment once
    LOWER(CONCAT(
      COALESCE(post_title, ''),
      ' ',
      COALESCE(comment_text, '')
    )) AS full_text

  FROM {{ ref('stg_reddit__comments') }}
)
 SELECT
    comment_id,
    comment_time,
    num_comment_upvotes,
    full_text,

  -- Mentions per coin
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_text, r'\b(btc|bitcoin|btcusdt)\b')) AS btc_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_text, r'\b(eth|ethereum|ethusdt)\b')) AS eth_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_text, r'\b(sol|solana|solusdt)\b')) AS sol_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_text, r'\b(xrp|ripple|xrpusdt)\b')) AS xrp_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_text, r'\b(bnb|binance\scoin|bnbusdt)\b')) AS bnb_mention_count

FROM base
