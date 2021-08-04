-- promotion % (Monthly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_promo_pct_monthly;


WITH
time_range AS (
    SELECT
        CASE
            WHEN LAST_DAY(CURRENT_DATE) = CURRENT_DATE THEN DATE_TRUNC('MONTH', CURRENT_DATE)
            ELSE DATE_ADD('MONTH', -1, DATE_TRUNC('MONTH', CURRENT_DATE))
        END::DATE AS complete_month
        , DATE_TRUNC('MONTH', CURRENT_DATE)::DATE AS current_month
        , DATE_ADD('MONTH', -11, complete_month)::DATE AS month_12_prior
        , DATE_ADD('MONTH', -12, current_month)::DATE AS complete_month_yoy_prev
        , DATE_ADD('MONTH', -12, month_12_prior)::DATE AS month_12_prior_yoy_prev
)
, monthly_agg AS (
    SELECT
        op.delivery_month
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('MONTH', 12, delivery_month)::DATE AS next_month
        , COUNT(DISTINCT product_id)                                                                AS products
        , COUNT(DISTINCT CASE WHEN sub_total < sub_total_base OR od.amount > 0 THEN product_id end) as promoted_products
        , COUNT(DISTINCT CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN product_id
            END) AS mtd_yoy_products
        , COUNT(DISTINCT CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                    AND (sub_total < sub_total_base OR od.amount > 0)
                THEN product_id
            END) AS mtd_yoy_promoted_products
    FROM
        metrics.order_product                op
        LEFT JOIN weee_p01.gb_order_discount od ON
                od.order_id = op.order_id
                AND op.product_id = od.parameter_1
                AND od.type IN ('trade_in', 'promotion_discount')
    WHERE
        payment_mode = 'F'
        AND product_type = 'grocery'
        AND delivery_day <= CURRENT_DATE
        AND (-- 12 months prior - current complete month
            (delivery_month BETWEEN (SELECT month_12_prior FROM time_range)
                AND (SELECT current_month FROM time_range))
            -- YoY
            OR (delivery_month BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND (SELECT complete_month_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        op.delivery_month
        , 'All' AS region
        , DATE_ADD('MONTH', 12, delivery_month)::DATE AS next_month
        , COUNT(DISTINCT product_id)                                                                AS products
        , COUNT(DISTINCT CASE WHEN sub_total < sub_total_base OR od.amount > 0 THEN product_id end) as promoted_products
        , COUNT(DISTINCT CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN product_id
            END) AS mtd_yoy_products
        , COUNT(DISTINCT CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                    AND (sub_total < sub_total_base OR od.amount > 0)
                THEN product_id
            END) AS mtd_yoy_promoted_products
    FROM
        metrics.order_product                op
        LEFT JOIN weee_p01.gb_order_discount od ON
                od.order_id = op.order_id
                AND op.product_id = od.parameter_1
                AND od.type IN ('trade_in', 'promotion_discount')
    WHERE
        payment_mode = 'F'
        AND product_type = 'grocery'
        AND delivery_day <= CURRENT_DATE
        AND (-- 12 months prior - current complete month
            (delivery_month BETWEEN (SELECT month_12_prior FROM time_range)
                AND (SELECT current_month FROM time_range))
            -- YoY
            OR (delivery_month BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND (SELECT complete_month_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
)
SELECT
    curr.region
    , curr.delivery_month
    , curr.products
    , curr.promoted_products
    , prev.products AS prev_products
    , prev.promoted_products AS prev_promoted_products
    , prev.mtd_yoy_products
    , prev.mtd_yoy_promoted_products
INTO sandbox.sz_temp_app_wbr_promo_pct_monthly
FROM
    monthly_agg      AS curr
    LEFT JOIN monthly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_month = curr.delivery_month
WHERE curr.delivery_month >= (SELECT month_12_prior FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8;


GRANT SELECT ON sandbox.sz_temp_app_wbr_promo_pct_monthly TO chartio;
