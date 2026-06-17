SELECT
    merchant,
    transaction_count,
    total_amount,
    average_amount,
    fraud_count
FROM {{ ref('stg_merchant_metrics') }}

ORDER by total_amount DESC 