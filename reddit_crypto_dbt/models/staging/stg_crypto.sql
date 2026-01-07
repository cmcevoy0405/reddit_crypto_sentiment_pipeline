{{ config(materialized='view') }}

WITH source as (
    SELECT *
    FROM {{source ('crypto_data', 'raw_crypto')}}
),
ranked as (
    select *,
    ROW_NUMBER() OVER(PARTITION BY symbol, open_time ORDER BY open_time) as rn
    FROM source
)
SELECT
    {{dbt_utils.generate_surrogate_key([
        'symbol',
        'open_time'
    ])}} as crypto_id,
    TRIM(CAST(symbol as string)) as symbol,
    TIMESTAMP(open_time) AS open_hour,
    CAST(open as FLOAT64) as open,
    CAST(high as FLOAT64) as high,
    CAST(low as FLOAT64) as low,
    CAST(close as FLOAT64) as close
FROM ranked
WHERE rn = 1
