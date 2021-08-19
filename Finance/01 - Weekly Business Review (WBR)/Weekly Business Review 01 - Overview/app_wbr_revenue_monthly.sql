-- Revenue (Monthly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_rev_monthly;
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
)  --SELECT * FROM time_range
, monthly_agg AS (
    SELECT
        CASE WHEN op.sales_region_id IN (3,16) THEN '3&16 - MAIL ORDER' ELSE op.sales_region_id||' - '||op.sales_region_title END AS region
        , NVL(CASE WHEN op.addr_sales_org_id IN (3,16) THEN '3&16 - MAIL ORDER' ELSE op.addr_sales_org_id||' - '||op.addr_sales_org_title END, 'Unknown') AS addr_region
        , delivery_month
        , DATE_ADD('MONTH', 12, delivery_month)::DATE AS next_month
        , op.parent_num||' - '||op.parent_category AS department
        , SUM(sub_total)                      AS revenue
        , SUM(CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE THEN sub_total END) AS mtd_yoy_revenue
        , COUNT(DISTINCT op.product_id) AS product_cnt
        , COUNT(DISTINCT CASE WHEN delivery_day BETWEEN DATE_ADD('WEEK', -52, DATE_TRUNC('MONTH', CURRENT_DATE))::DATE AND DATE_ADD('WEEK',-52,CURRENT_DATE)::DATE THEN op.product_id END) AS mtd_yoy_product_cnt
    FROM metrics.order_product AS op
    WHERE
        payment_mode = 'F'
        AND delivery_day <= CURRENT_DATE
        AND (-- 12 months prior - current complete month
            (delivery_month BETWEEN (SELECT month_12_prior FROM time_range)
                AND (SELECT current_month FROM time_range))
            -- YoY
            OR (delivery_month BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND (SELECT complete_month_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5
)
SELECT
    NVL(curr.region, prev.region) AS region
    , NVL(curr.addr_region, prev.addr_region) AS addr_region
    , NVL(curr.delivery_month, prev.next_month) AS delivery_month
    , NVL(curr.department, prev.department) AS department
    , NVL(curr.revenue, 0) AS revenue
    , NVL(prev.revenue, 0) AS prev_revenue
    , NVL(prev.mtd_yoy_revenue,0) AS mtd_yoy_revenue
    , NVL(curr.product_cnt, 0) AS product_cnt
    , NVL(prev.product_cnt, 0) AS prev_product_cnt
    , NVL(prev.mtd_yoy_product_cnt,0) AS mtd_yoy_product_cnt
INTO sandbox.sz_temp_app_wbr_rev_monthly
FROM
    monthly_agg      AS curr
    FULL OUTER JOIN monthly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_month = curr.delivery_month
        AND prev.department = curr.department
        AND prev.addr_region = curr.addr_region
WHERE NVL(curr.delivery_month, prev.next_month) BETWEEN (SELECT month_12_prior FROM time_range)
    AND (SELECT current_month FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
GRANT SELECT ON sandbox.sz_temp_app_wbr_rev_monthly TO chartio;
