{{ 
    config(
        database='reddit-crypto-sentiment', 
        schema='crypto_data',                 
        materialized='table'
    ) 
}}

WITH fact_crypto AS (
    SELECT 
        crypto_id,
        DATE_TRUNC(open_hour, HOUR) as hour,
        open,
        high,
        low, 
        close
    FROM {{ref('stg_crypto')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key
    (['crypto_id', 'hour']) }} as price_id,
    crypto_id,
    hour,
    open,
    high,
    close
FROM fact_crypto

