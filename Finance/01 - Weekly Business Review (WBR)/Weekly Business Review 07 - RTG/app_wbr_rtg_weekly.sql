-- RTG metrics (Weekly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_rtg_weekly;

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
, rtg_orders AS (
    SELECT ro.order_id
    FROM metrics.rtg_sales_order AS ro
    WHERE ro.delivery_day <= CURRENT_DATE
        AND (-- 6 weeks prior - current complete week
            (ro.delivery_week BETWEEN (SELECT week_6_prior FROM time_range)
                AND (SELECT current_week FROM time_range))
            -- YoY
            OR (ro.delivery_week BETWEEN (SELECT week_6_prior_yoy_prev FROM time_range)
            AND (SELECT complete_week_yoy_prev FROM time_range))
        )
    GROUP BY 1
    ORDER BY 1
)
, first_time_buyers AS (
    SELECT DISTINCT
        user_id
        , FIRST_VALUE(order_id) OVER (
            PARTITION BY user_id
            ORDER BY delivery_day, order_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )  AS first_delivery_order_id
    FROM metrics.rtg_sales_order AS ro
    ORDER BY 1
)
, delivery_fee_weekly AS (
    SELECT
        DATE_ADD('DAY', -DATE_PART('DOW', de.delivery_date)::INT, de.delivery_date)::DATE AS delivery_week
--         de.delivery_date
        , so.id AS region_id
        , so.id||' - '||so.title AS region
        , SUM(NVL(de.dispatch_fee,0))                                                           AS driver_fee
    FROM metrics.dispatch AS de
        JOIN weee_p01.sales_org_mapping AS som
            ON som.sales_org_id = de.sales_org_id
            AND som.type = 'gb_product_sales'
        JOIN weee_p01.gb_sales_org AS so
            ON so.id = som.ref_sales_org_id
    WHERE
        de.delivery_date <= CURRENT_DATE
        AND (-- 6 weeks prior - current complete week
            (de.delivery_date BETWEEN (SELECT week_6_prior FROM time_range)
                AND (SELECT current_week FROM time_range)+6)
            -- YoY
            OR (de.delivery_date) BETWEEN (SELECT week_6_prior_yoy_prev FROM time_range)
            AND (SELECT complete_week_yoy_prev FROM time_range)+6)
        AND de.delivery_plan_type = 'restaurant'--'hot_delivery'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
)
, discount_weekly AS (
    SELECT
        delivery_week
        , sales_region_id
        , sales_region_id||' - '||sales_region_title AS region
        , SUM(NVL(discount,0)) AS discount
    FROM metrics.order AS o
    WHERE
        o.order_id IN (SELECT order_id FROM rtg_orders)
    GROUP BY 1,2,3
    ORDER BY 1,2,3
)
, base AS (
    SELECT
        ro.user_id
        , ro.order_id
        , ro.invoice_id
        , ro.delivery_day
        , ro.delivery_week
        , DATE_TRUNC('MONTH', ro.delivery_day)::DATE AS delivery_month
        , so.id AS region_id
        , so.id||' - '||so.title AS region
        , ro.original_price
        , ro.quantity
        , ro.product_id
        , ro.sub_total
        , CASE WHEN f.first_delivery_order_id IS NULL THEN 0 ELSE 1 END AS new_user_flag
        , r.refund_amount
    FROM
        metrics.rtg_sales_order     AS ro
        LEFT JOIN weee_p01.sales_org_mapping AS som
            ON som.sales_org_id = ro.sales_org_id
            AND som.type = 'gb_product_sales'
        LEFT JOIN weee_p01.gb_sales_org AS so
            ON so.id = som.ref_sales_org_id
        LEFT JOIN first_time_buyers AS f
            ON f.first_delivery_order_id = ro.order_id
        LEFT JOIN metrics.return AS r
            ON r.order_id = ro.order_id
		    AND r.product_id = ro.product_id
    WHERE ro.delivery_day <= CURRENT_DATE
        AND (-- 6 weeks prior - current complete week
            (ro.delivery_week BETWEEN (SELECT week_6_prior FROM time_range)
                AND (SELECT current_week FROM time_range))
            -- YoY
            OR (ro.delivery_week BETWEEN (SELECT week_6_prior_yoy_prev FROM time_range)
            AND (SELECT complete_week_yoy_prev FROM time_range))
        )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
, base_agg AS (
    SELECT
        b.delivery_week
        , CASE WHEN b.region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE b.region END AS region
        , DATE_ADD('WEEK', 52, b.delivery_week)::DATE AS next_week
        , COUNT(DISTINCT user_id)          AS active_users
        , COUNT(DISTINCT CASE WHEN new_user_flag = 1 THEN user_id END) AS new_users
        , COUNT(DISTINCT order_id) AS orders
        , SUM(b.sub_total)                 AS revenue
        , SUM(NVL(b.original_price,0) * quantity) AS cogs -- original_price -- avg_price
        , SUM(NVL(b.refund_amount,0))             AS refunds
    FROM
        base AS   b
    GROUP BY 1,2,3
    ORDER BY 1,2,3
)
, weekly_agg AS (
    SELECT ba.*
        , NVL(dis.discount,0) AS discounts
        , NVL(de.driver_fee,0) AS delivery_costs
        , revenue-cogs-refunds-discounts-delivery_costs AS margin
    FROM base_agg AS ba
        LEFT JOIN discount_weekly AS dis
            ON dis.delivery_week = ba.delivery_week
            AND dis.region = ba.region
        LEFT JOIN delivery_fee_weekly AS de
            ON de.delivery_week = ba.delivery_week
            AND de.region = ba.region
    ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12
)
SELECT
    curr.region
    , curr.delivery_week
    , curr.active_users
    , curr.new_users
    , curr.revenue
    , curr.cogs
    , curr.refunds
    , curr.discounts
    , curr.delivery_costs
    , curr.orders
    , curr.margin
    , prev.active_users AS prev_active_users
    , prev.new_users AS prev_new_users
    , prev.revenue AS prev_revenue
    , prev.cogs AS prev_cogs
    , prev.refunds AS prev_refunds
    , prev.discounts AS prev_discounts
    , prev.delivery_costs AS prev_delivery_costs
    , prev.orders AS prev_orders
    , prev.margin AS prev_margin
INTO sandbox.sz_temp_app_wbr_rtg_weekly
FROM
    weekly_agg      AS curr
    LEFT JOIN weekly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_week = curr.delivery_week
WHERE curr.delivery_week >= (SELECT week_6_prior FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;

GRANT SELECT ON sandbox.sz_temp_app_wbr_rtg_weekly TO chartio;
