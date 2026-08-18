SELECT
    transaction_date,
    total_transactions,
    total_amount,
    average_amount,
    fraud_count,
    fraud_rate,
    warehouse_loaded_at
FROM {{ source('warehouse', 'daily_transaction_metrics') }}
