SELECT
    merchant,
    transaction_count,
    total_amount,
    average_amount,
    ROUND(100.0 * total_amount / SUM(total_amount) OVER (), 2) AS revenue_share_pct
FROM {{ ref('stg_merchant_metrics') }}
ORDER by revenue_share_pct DESC