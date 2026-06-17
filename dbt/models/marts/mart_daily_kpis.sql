SELECT
    transaction_date,
    total_transactions,
    total_amount,
    average_amount,
    fraud_count,
    fraud_rate
FROM {{ ref('stg_daily_transaction_metrics') }}