SELECT
    merchant,
    transaction_count,
    total_amount,
    average_amount,
    fraud_count,
    warehouse_loaded_at
FROM {{ source('warehouse', 'merchant_metrics') }}
