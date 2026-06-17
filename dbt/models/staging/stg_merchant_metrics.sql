SELECT
    merchant,
    transaction_count,
    total_amount,
    average_amount,
    fraud_count
FROM {{ source('warehouse', 'merchant_metrics') }}