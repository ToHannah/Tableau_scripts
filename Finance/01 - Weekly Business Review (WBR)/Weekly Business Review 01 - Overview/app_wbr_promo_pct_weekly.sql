-- promotion % (Weekly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_promo_pct_weekly;


WITH
    time_range AS (
        SELECT
            CASE
                WHEN DATE_PART('DOW', CURRENT_DATE) = 6 THEN DATE_ADD('DAY', -6, CURRENT_DATE)
                ELSE DATE_ADD('DAY', -DATE_PART('DOW', CURRENT_DATE)::INT - 7, CURRENT_DATE)
            END::DATE                                                                   AS complete_week
            , DATE_ADD('DAY', -DATE_PART('DOW', CURRENT_DATE)::INT, CURRENT_DATE)::DATE AS current_week
            , DATE_ADD('WEEK', -5, complete_week)::DATE                                 AS week_6_prior
            , DATE_ADD('WEEK', -52, current_week)::DATE                                 AS complete_week_yoy_prev
            , DATE_ADD('WEEK', -52, week_6_prior)::DATE                                 AS week_6_prior_yoy_prev
    )

    , weekly_agg AS (
    SELECT
        op.delivery_week
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('WEEK', 52, op.delivery_week)::DATE AS next_week
        , COUNT(DISTINCT product_id)                                                                AS products
        , COUNT(DISTINCT CASE WHEN sub_total < sub_total_base OR od.amount > 0 THEN product_id end) as promoted_products
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
        AND (-- 6 weeks prior - current complete week
            (delivery_week BETWEEN (SELECT week_6_prior FROM time_range)
                AND (SELECT current_week FROM time_range))
            -- YoY
            OR (delivery_week BETWEEN (SELECT week_6_prior_yoy_prev FROM time_range)
            AND (SELECT complete_week_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        op.delivery_week
        , 'All' AS region
        , DATE_ADD('WEEK', 52, op.delivery_week)::DATE AS next_week
        , COUNT(DISTINCT product_id)                                                                AS products
        , COUNT(DISTINCT CASE WHEN sub_total < sub_total_base OR od.amount > 0 THEN product_id end) as promoted_products
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
        AND (-- 6 weeks prior - current complete week
            (delivery_week BETWEEN (SELECT week_6_prior FROM time_range)
                AND (SELECT current_week FROM time_range))
            -- YoY
            OR (delivery_week BETWEEN (SELECT week_6_prior_yoy_prev FROM time_range)
            AND (SELECT complete_week_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
)
SELECT
    curr.region
    , curr.delivery_week
    , curr.products
    , curr.promoted_products
    , prev.products AS prev_products
    , prev.promoted_products AS prev_promoted_products
INTO sandbox.sz_temp_app_wbr_promo_pct_weekly
FROM
    weekly_agg      AS curr
    LEFT JOIN weekly_agg AS prev
              ON prev.region = curr.region
                  AND prev.next_week = curr.delivery_week
WHERE curr.delivery_week >= (SELECT week_6_prior FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6;


GRANT SELECT ON sandbox.sz_temp_app_wbr_promo_pct_weekly TO chartio;
