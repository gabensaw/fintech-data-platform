SELECT
    merchant,
    total_amount,
    CASE
        WHEN ntile = 1 THEN 'Platinum'
        WHEN ntile = 2 THEN 'Gold'
        WHEN ntile = 3 THEN 'Silver'
        ELSE 'Bronze'
    END AS tier
FROM (
    SELECT
        merchant,
        total_amount,
        NTILE(4) OVER (
            ORDER BY total_amount DESC
        ) AS ntile
    FROM {{ ref('stg_merchant_metrics') }}
) t