WITH fact_crypto AS (
    SELECT 
        crypto_id,
        open_hour,
        open,
        high,
        low, 
        close,
    FROM {{ref('stg_crypto')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key
    (['crypto_id', 'open_hour']) }} as price_id,
    crypto_id,
    open_hour as open_time,
    open,
    high,
    close
FROM fact_crypto

