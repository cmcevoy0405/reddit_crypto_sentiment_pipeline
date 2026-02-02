{{ 
    config(
        database='reddit-crypto-sentiment', 
        schema='reddit_data',                 
        materialized='view'
    ) 
}}

WITH posts AS(
    SELECT 
        post_id,
        post_time,
        post_title,
        LOWER(CONCAT(
            COALESCE(post_title, ''),
            ' ',
            COALESCE(body_text, '')
        )) as full_post_text,
        upvote_ratio
    FROM {{ ref('stg_reddit__posts') }}
)
 SELECT
    post_id,
    post_time,
    post_title,
    upvote_ratio,
    full_post_text,

  -- Mentions per coin
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_post_text, r'\b(btc|bitcoin|btcusdt)\b')) AS btc_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_post_text, r'\b(eth|ethereum|ethusdt)\b')) AS eth_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_post_text, r'\b(sol|solana|solusdt)\b')) AS sol_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_post_text, r'\b(xrp|ripple|xrpusdt)\b')) AS xrp_mention_count,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(full_post_text, r'\b(bnb|binance\scoin|bnbusdt)\b')) AS bnb_mention_count

FROM posts



    
