SELECT 
    transaction_date,
    total_transactions,
    total_amount,
    average_amount,
    fraud_count,
    fraud_rate
FROM {{ source('warehouse', 'daily_transaction_metrics') }}