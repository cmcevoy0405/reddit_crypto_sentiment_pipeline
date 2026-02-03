{{ 
    config(
        database='reddit-crypto-sentiment',  
        schema='crypto_data',                
        materialized='table'
    ) 
}}

WITH post_count AS (
    SELECT 
        DATE_TRUNC(post_time, HOUR) as hour,
        SUM(COALESCE(btc_mention_count, 0)) as btc_ment,
        SUM(COALESCE(eth_mention_count, 0)) as eth_ment,
        SUM(COALESCE(sol_mention_count, 0)) as sol_ment,
        SUM(COALESCE(xrp_mention_count, 0)) as xrp_ment,
        SUM(COALESCE(bnb_mention_count, 0)) as bnb_ment
    FROM {{ref('int_posts_mentions')}}
    GROUP BY 1
),
comment_count AS (
    SELECT
        DATE_TRUNC(comment_time, HOUR) as hour,
        SUM(COALESCE(btc_mention_count, 0)) as btc_comment_ment,
        SUM(COALESCE(eth_mention_count, 0)) as eth_comment_ment,
        SUM(COALESCE(sol_mention_count, 0)) as sol_comment_ment,
        SUM(COALESCE(xrp_mention_count, 0)) as xrp_comment_ment,
        SUM(COALESCE(bnb_mention_count, 0)) as bnb_comment_ment
    FROM {{ref('int_comment_mentions')}}
    GROUP BY 1
)

SELECT
        COALESCE(pc.hour, cc.hour) as hour,
        COALESCE(pc.btc_ment,0) + COALESCE(cc.btc_comment_ment,0) AS total_btc_mentions,
        COALESCE(pc.eth_ment,0) + COALESCE(cc.eth_comment_ment,0) AS total_eth_mentions,
        COALESCE(pc.sol_ment,0) + COALESCE(cc.sol_comment_ment,0) AS total_sol_mentions,
        COALESCE(pc.xrp_ment,0) + COALESCE(cc.xrp_comment_ment,0) AS total_xrp_mentions,
        COALESCE(pc.bnb_ment,0) + COALESCE(cc.bnb_comment_ment,0) AS total_bnb_mentions
    FROM post_count as pc
    FULL OUTER JOIN comment_count AS cc
    ON pc.hour = cc.hour
    ORDER BY hour
