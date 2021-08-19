-- Gross Revenue
-- Revenue (Weekly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_rev_weekly;
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
        CASE WHEN op.sales_region_id IN (3,16) THEN '3&16 - MAIL ORDER' ELSE op.sales_region_id||' - '||op.sales_region_title END AS region
        , NVL(CASE WHEN op.addr_sales_org_id IN (3,16) THEN '3&16 - MAIL ORDER' ELSE op.addr_sales_org_id||' - '||op.addr_sales_org_title END, 'Unknown') AS addr_region
        , delivery_week
        , DATE_ADD('WEEK', 52, delivery_week)::DATE AS next_week
        , op.parent_num||' - '||op.parent_category AS department
        , SUM(sub_total)                      AS revenue
        , COUNT(DISTINCT op.product_id) AS prduct_cnt
    FROM metrics.order_product AS op
    WHERE
        payment_mode = 'F'
        AND delivery_day <= CURRENT_DATE
        AND (-- 6 weeks prior - current complete week
            (delivery_week BETWEEN (SELECT week_6_prior FROM time_range)
                AND (SELECT current_week FROM time_range))
            -- YoY
            OR (delivery_week BETWEEN (SELECT week_6_prior_yoy_prev FROM time_range)
            AND (SELECT complete_week_yoy_prev FROM time_range))
        )
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5
)
SELECT
    NVL(curr.region, prev.region, 'unknown') AS region
    , NVL(curr.addr_region, prev.addr_region, 'unknown') AS addr_region
    , NVL(curr.delivery_week, prev.next_week) AS delivery_week
    , NVL(curr.department, prev.department, 'unknown') AS department
    , NVL(curr.revenue, 0) AS revenue
    , NVL(prev.revenue, 0) AS prev_revenue
    , NVL(curr.prduct_cnt, 0) AS product_cnt
    , NVL(prev.prduct_cnt, 0) AS prev_product_cnt
INTO sandbox.sz_temp_app_wbr_rev_weekly
FROM
    weekly_agg      AS curr
    FULL OUTER JOIN weekly_agg AS prev
        ON prev.region = curr.region
        AND prev.next_week = curr.delivery_week
        AND prev.department = curr.department
        AND prev.addr_region = curr.addr_region
WHERE NVL(curr.delivery_week, prev.next_week) BETWEEN (SELECT week_6_prior FROM time_range)
    AND (SELECT current_week FROM time_range)
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8 ;
GRANT SELECT ON sandbox.sz_temp_app_wbr_rev_weekly TO chartio;
