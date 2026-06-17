SELECT (
    SELECT COUNT(*)
    FROM {{ ref('stg_merchant_metrics') }}
    ) AS total_merchants,
    
    SUM(total_transactions) AS total_transactions,
    SUM(total_amount) AS total_revenue,

    ROUND(AVG(average_amount), 2) AS avg_transaction_amount,

    SUM(fraud_count) AS total_fraud_transactions

FROM {{ ref('stg_daily_transaction_metrics') }}
