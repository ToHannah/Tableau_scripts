-- promotion given up (Weekly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_promo_amount_weekly;


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
        , 'Sale' AS promo_type
        , SUM(CASE
                  WHEN op.price_type = 'sale' AND op.sub_total < op.sub_total_base
                      THEN (op.sub_total_base - op.sub_total)
                  ELSE 0
              END)                                             AS promo_discount
    FROM
        metrics.order_product                op
--         LEFT JOIN weee_p01.gb_order_discount od ON
--                 od.order_id = op.order_id
--                 AND op.product_id = od.parameter_1
--                 AND od.type IN ('trade_in', 'promotion_discount')
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
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        op.delivery_week
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('WEEK', 52, op.delivery_week)::DATE AS next_week
        , 'Lightning' AS promo_type
        , SUM(CASE
                  WHEN op.price_type = 'lightening' AND op.sub_total < op.sub_total_base
                      THEN (op.sub_total_base - op.sub_total)
                  ELSE 0
              END)                                             AS promo_discount
    FROM
        metrics.order_product                op
--         LEFT JOIN weee_p01.gb_order_discount od ON
--                 od.order_id = op.order_id
--                 AND op.product_id = od.parameter_1
--                 AND od.type IN ('trade_in', 'promotion_discount')
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
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        op.delivery_week
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('WEEK', 52, op.delivery_week)::DATE AS next_week
        , '$68_Upsell' AS promo_type
        , SUM(CASE
                  WHEN od.amount > 0 AND od.type = 'trade_in'
                      THEN od.amount
                  ELSE 0
              END)                                             AS promo_discount
    FROM
        metrics.order_product                op
        LEFT JOIN weee_p01.gb_order_discount od ON
                od.order_id = op.order_id
                AND op.product_id = od.parameter_1
                AND od.type IN ('trade_in')
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
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        op.delivery_week
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('WEEK', 52, op.delivery_week)::DATE AS next_week
        , 'BOGO' AS promo_type
        , SUM(CASE
                  WHEN od.amount > 0 AND od.type = 'promotion_discount'
                      THEN od.amount
                  ELSE 0
              END)                                             AS promo_discount
    FROM
        metrics.order_product                op
        LEFT JOIN weee_p01.gb_order_discount od ON
                od.order_id = op.order_id
                AND op.product_id = od.parameter_1
                AND od.type IN ('promotion_discount')
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
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
)
SELECT
    NVL(curr.region, prev.region) AS region
    , NVL(curr.delivery_week, prev.delivery_week) AS delivery_week
    , NVL(curr.promo_type, prev.promo_type) AS promo_type
    , NVL(curr.promo_discount, 0) AS promo_discount
    , NVL(prev.promo_discount, 0) AS prev_promo_discount
INTO sandbox.sz_temp_app_wbr_promo_amount_weekly
FROM
    weekly_agg      AS curr
    FULL OUTER JOIN weekly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_week = curr.delivery_week
        AND prev.promo_type = curr.promo_type
WHERE curr.delivery_week BETWEEN (SELECT week_6_prior FROM time_range) AND (SELECT current_week FROM time_range)
ORDER BY 1, 2, 3, 4, 5;


GRANT SELECT ON sandbox.sz_temp_app_wbr_promo_amount_weekly TO chartio;
