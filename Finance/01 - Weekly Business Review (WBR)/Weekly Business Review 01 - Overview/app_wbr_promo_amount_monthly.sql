-- promotion given up (Monthly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_promo_amount_monthly;


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
        , 'Sale' AS promo_type
        , SUM(CASE
                  WHEN op.price_type = 'sale' AND op.sub_total < op.sub_total_base
                      THEN (op.sub_total_base - op.sub_total)
                  ELSE 0
            END)                                             AS promo_discount
        , SUM(CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN
                    NVL(CASE
                        WHEN op.price_type IN ('sale') AND op.sub_total < op.sub_total_base
                        THEN (op.sub_total_base - op.sub_total)
                    END,0)
            END) AS mtd_yoy_promo_discount
    FROM
        metrics.order_product                op
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
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        op.delivery_month
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('MONTH', 12, delivery_month)::DATE AS next_month
        , 'Lightning' AS promo_type
        , SUM(CASE
                  WHEN op.price_type = 'lightening' AND op.sub_total < op.sub_total_base
                      THEN (op.sub_total_base - op.sub_total)
                   ELSE 0
            END)                                             AS promo_discount
        , SUM(CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN
                    NVL(CASE
                        WHEN op.price_type IN ('lightening') AND op.sub_total < op.sub_total_base
                        THEN (op.sub_total_base - op.sub_total)
                    END,0)
            END) AS mtd_yoy_promo_discount
    FROM
        metrics.order_product                op
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
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        op.delivery_month
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('MONTH', 12, delivery_month)::DATE AS next_month
        , '$68_Upsell' AS promo_type
        , SUM(CASE
                  WHEN od.amount > 0 AND od.type = 'trade_in'
                      THEN od.amount
                  ELSE 0
            END)                                             AS upsell68
        , SUM(CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN
                    NVL(CASE WHEN price_type = 'trade_in' THEN od.amount ELSE 0 END, 0)
            END) AS mtd_yoy_promo_discount
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
        AND (-- 12 months prior - current complete month
            (delivery_month BETWEEN (SELECT month_12_prior FROM time_range)
                AND (SELECT current_month FROM time_range))
            -- YoY
            OR (delivery_month BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND (SELECT complete_month_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        op.delivery_month
        , CASE WHEN op.sales_region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE NVL(op.sales_region_id || ' - ' || op.sales_region_title,'unknown') END AS region
        , DATE_ADD('MONTH', 12, delivery_month)::DATE AS next_month
        , 'BOGO' AS promo_type
        , SUM(CASE
                  WHEN od.amount > 0 AND od.type = 'promotion_discount'
                      THEN od.amount
                  ELSE 0
            END)                                             AS promo_discount
        , SUM(CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE
                    AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN
                    NVL(CASE WHEN price_type <> 'trade_in' THEN od.amount ELSE 0 END,0)
            END) AS mtd_yoy_promo_discount
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
        AND (-- 12 months prior - current complete month
            (delivery_month BETWEEN (SELECT month_12_prior FROM time_range)
                AND (SELECT current_month FROM time_range))
            -- YoY
            OR (delivery_month BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND (SELECT complete_month_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
)
SELECT
    NVL(curr.region, prev.region) AS region
    , NVL(curr.delivery_month, prev.delivery_month) AS delivery_month
    , NVL(curr.promo_type, prev.promo_type) AS promo_type
    , NVL(curr.promo_discount, 0) AS promo_discount
    , NVL(prev.promo_discount, 0) AS prev_promo_discount
    , NVL(prev.mtd_yoy_promo_discount, 0) AS mtd_yoy_promo_discount
INTO sandbox.sz_temp_app_wbr_promo_amount_monthly
FROM
    monthly_agg      AS curr
    FULL OUTER JOIN monthly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_month = curr.delivery_month
        and prev.promo_type = curr.promo_type
WHERE curr.delivery_month BETWEEN (SELECT month_12_prior FROM time_range) AND (SELECT current_month FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6;


GRANT SELECT ON sandbox.sz_temp_app_wbr_promo_amount_monthly TO chartio;
