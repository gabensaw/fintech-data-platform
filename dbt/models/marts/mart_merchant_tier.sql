SELECT
    merchant,
    CASE
        WHEN total_amount >= 40000 THEN 'Platinum'
        WHEN total_amount >= 30000 THEN 'Gold'
        WHEN total_amount >= 20000 THEN 'Silver'
        ELSE 'Bronze'
    END AS tier
FROM {{ ref('stg_merchant_metrics') }}
ORDER BY total_amount DESC