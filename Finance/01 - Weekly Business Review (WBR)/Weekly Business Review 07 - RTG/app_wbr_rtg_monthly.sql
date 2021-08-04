-- RTG metrics (Monthly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_rtg_monthly;


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
, rtg_orders AS (
    SELECT ro.order_id
    FROM metrics.rtg_sales_order AS ro
    WHERE (-- 12 months prior - current complete month
            (ro.delivery_day BETWEEN (SELECT month_12_prior FROM time_range)
                AND LAST_DAY((SELECT current_month FROM time_range)))
            -- YoY
            OR (ro.delivery_day BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND LAST_DAY((SELECT complete_month_yoy_prev FROM time_range)))
        )
        AND ro.delivery_day <= CURRENT_DATE
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
, delivery_fee_monthly AS (
    SELECT
        DATE_TRUNC('MONTH', de.delivery_date)::DATE AS delivery_month
--         delivery_date AS delivery_day
        , so.id AS region_id
        , so.id||' - '||so.title AS region
        , SUM(NVL(de.dispatch_fee,0))                                                            AS driver_fee
        , SUM(CASE WHEN delivery_date BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN NVL(dispatch_fee,0) END) AS mtd_yoy_delivery_costs
    FROM metrics.dispatch AS de
        JOIN weee_p01.sales_org_mapping AS som
            ON som.sales_org_id = de.sales_org_id
            AND som.type = 'gb_product_sales'
        JOIN weee_p01.gb_sales_org AS so
            ON so.id = som.ref_sales_org_id
    WHERE
        (-- 12 months prior - current complete month
            (de.delivery_date BETWEEN (SELECT month_12_prior FROM time_range)
                AND LAST_DAY((SELECT current_month FROM time_range)))
            -- YoY
            OR (de.delivery_date BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND LAST_DAY((SELECT complete_month_yoy_prev FROM time_range)))
        )
        AND de.delivery_date <= CURRENT_DATE
        AND de.delivery_plan_type = 'restaurant'--'hot_delivery'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
)
, discount_monthly AS (
    SELECT
        delivery_month
--          delivery_day
         , sales_region_id
         , sales_region_id||' - '||sales_region_title AS region
         , SUM(NVL(discount,0)) AS discount
         , SUM(CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN NVL(discount,0) END) AS mtd_yoy_discounts
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
        , DATE_TRUNC('MONTH', ro.delivery_day)::DATE AS delivery_month
        , so.id AS region_id
        , so.id||' - '||so.title AS region
        , ro.product_id
        , ro.original_price
        , ro.quantity
        , ro.sub_total
        , CASE WHEN f.first_delivery_order_id IS NULL THEN 0 ELSE 1 END AS new_user_flag
        , r.refund_amount
    FROM
        metrics.rtg_sales_order     AS ro
        JOIN weee_p01.sales_org_mapping AS som
            ON som.sales_org_id = ro.sales_org_id
            AND som.type = 'gb_product_sales'
        JOIN weee_p01.gb_sales_org AS so
            ON so.id = som.ref_sales_org_id
        LEFT JOIN first_time_buyers AS f
            ON f.first_delivery_order_id = ro.order_id
        LEFT JOIN metrics.return AS r
            ON r.order_id = ro.order_id
		    AND r.product_id = ro.product_id
    WHERE (-- 12 months prior - current complete month
            (ro.delivery_day BETWEEN (SELECT month_12_prior FROM time_range)
                AND LAST_DAY((SELECT current_month FROM time_range)))
            -- YoY
            OR (ro.delivery_day BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND LAST_DAY((SELECT complete_month_yoy_prev FROM time_range)))
        )
        AND ro.delivery_day <= CURRENT_DATE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
, base_agg AS (
    SELECT
        b.delivery_month
        , CASE WHEN b.region_id IN (3,16) THEN '3 - MAIL ORDER - WEST' ELSE b.region END AS region
        , DATE_ADD('MONTH', 12, b.delivery_month)::DATE AS next_month
        , COUNT(DISTINCT user_id)          AS active_users
        , COUNT(DISTINCT CASE WHEN new_user_flag = 1 THEN user_id END) AS new_users
        , COUNT(DISTINCT order_id) AS orders
        , SUM(b.sub_total)                 AS revenue
        , SUM(NVL(b.original_price,0) * quantity) AS cogs -- original_price -- avg_price
        , SUM(NVL(b.refund_amount,0))             AS refunds
        -- mtd yoy versions
        , COUNT(DISTINCT CASE WHEN b.delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN user_id END)          AS mtd_yoy_active_users
        , COUNT(DISTINCT CASE WHEN b.delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN order_id END)          AS mtd_yoy_orders
        , COUNT(DISTINCT CASE WHEN b.delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                AND new_user_flag = 1 THEN user_id END) AS mtd_yoy_new_users
        , SUM(CASE WHEN b.delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN b.sub_total END)                 AS mtd_yoy_revenue
        , SUM(CASE WHEN b.delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN NVL(b.original_price,0) * quantity END) AS mtd_yoy_cogs -- original_price -- avg_price
        , SUM(CASE WHEN b.delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE
                THEN NVL(b.refund_amount,0) END)             AS mtd_yoy_refunds
    FROM
        base AS   b
    GROUP BY 1,2,3
    ORDER BY 1,2,3
)
, monthly_agg AS (
    SELECT
        ba.*
        , NVL(dis.discount,0) AS discounts
        , NVL(de.driver_fee,0) AS delivery_costs
        , revenue-cogs-refunds-discounts-delivery_costs AS margin
        , dis.mtd_yoy_discounts
        , de.mtd_yoy_delivery_costs
        , mtd_yoy_revenue-mtd_yoy_cogs-mtd_yoy_refunds-mtd_yoy_discounts-mtd_yoy_delivery_costs AS mtd_yoy_margin
    FROM base_agg AS ba
        LEFT JOIN discount_monthly AS dis ON dis.delivery_month = ba.delivery_month
            AND dis.region = ba.region
        LEFT JOIN delivery_fee_monthly AS de ON de.delivery_month = ba.delivery_month
            AND de.region = ba.region
)
SELECT
    curr.region
    , curr.delivery_month
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
    , prev.mtd_yoy_active_users
    , prev.mtd_yoy_new_users
    , prev.mtd_yoy_revenue
    , prev.mtd_yoy_cogs
    , prev.mtd_yoy_refunds
    , prev.mtd_yoy_discounts
    , prev.mtd_yoy_delivery_costs
    , prev.mtd_yoy_orders
    , prev.mtd_yoy_margin
INTO sandbox.sz_temp_app_wbr_rtg_monthly
FROM
    monthly_agg      AS curr
    LEFT JOIN monthly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_month = curr.delivery_month
WHERE curr.delivery_month >= (SELECT month_12_prior FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9 ,10, 11, 12, 13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29;


GRANT SELECT ON sandbox.sz_temp_app_wbr_rtg_monthly TO chartio;
