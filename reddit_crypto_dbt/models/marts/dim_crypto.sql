{{ 
    config(
        database='reddit-crypto-sentiment',  
        schema='crypto_data',                 
        materialized='table'
    ) 
}}

WITH dim_crypto AS(
    SELECT 
        crypto_id,
        symbol,
    FROM {{ref('stg_crypto')}}
)
SELECT  
    crypto_id,
    symbol,
    CASE 
    WHEN symbol = 'BNBUSDT' then 'Binance Coin'
    WHEN symbol = 'BTCUSDT' then 'Bitcoin'
    WHEN symbol = 'ETHUSDT' then 'Ethereum'
    WHEN symbol = 'SOLUSDT'  then 'Solana'
    WHEN symbol = 'XRPUSDT' then 'XRP'
    END AS crypto_name
FROM dim_crypto