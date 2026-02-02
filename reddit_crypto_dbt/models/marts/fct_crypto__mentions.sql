WITH post_count AS (
    SELECT 
        post_id,
        post_title,
        post_time,
        COALESCE(btc_mention_count, 0) as btc_ment,
        COALESCE(eth_mention_count, 0) as eth_ment,
        COALESCE(sol_mention_count, 0) as sol_ment,
        COALESCE(xrp_mention_count, 0) as xrp_ment,
        COALESCE(bnb_mention_count, 0) as bnb_ment
    FROM {{ref('int_posts_mentions')}}
),
comment_count AS (
    SELECT
        post_id,
        SUM(COALESCE(btc_mention_count, 0)) as btc_comment_ment,
        SUM(COALESCE(eth_mention_count, 0)) as eth_comment_ment,
        SUM(COALESCE(sol_mention_count, 0)) as sol_comment_ment,
        SUM(COALESCE(xrp_mention_count, 0)) as xrp_comment_ment,
        SUM(COALESCE(bnb_mention_count, 0)) as bnb_comment_ment
    FROM {{ref('int_comment_mentions')}}
    GROUP BY post_id
)

SELECT
        COALESCE(pc.post_id, cc.post_id) AS post_id,
        pc.post_title,
        pc.post_time,
        COALESCE(pc.btc_ment,0) + COALESCE(cc.btc_comment_ment,0) AS total_btc_mentions,
        COALESCE(pc.eth_ment,0) + COALESCE(cc.eth_comment_ment,0) AS total_eth_mentions,
        COALESCE(pc.sol_ment,0) + COALESCE(cc.sol_comment_ment,0) AS total_sol_mentions,
        COALESCE(pc.xrp_ment,0) + COALESCE(cc.xrp_comment_ment,0) AS total_xrp_mentions,
        COALESCE(pc.bnb_ment,0) + COALESCE(cc.bnb_comment_ment,0) AS total_bnb_mentions
    FROM post_count as pc
    FULL OUTER JOIN comment_count AS cc
    ON pc.post_id = cc.post_id
