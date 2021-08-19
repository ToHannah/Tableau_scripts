-- In Stock Rate (monthly)
DROP TABLE IF EXISTS sandbox.sz_temp_app_wbr_instock_monthly;
WITH
time_range AS (
    SELECT
    CASE
        WHEN LAST_DAY(CURRENT_DATE) = CURRENT_DATE THEN DATE_TRUNC('MONTH', CURRENT_DATE)
        ELSE DATE_ADD('MONTH', -1, DATE_TRUNC('MONTH', CURRENT_DATE))
    END::DATE AS complete_month
    , DATE_ADD('MONTH', -11, complete_month)::DATE AS month_12_prior
    , DATE_ADD('MONTH', -12, complete_month)::DATE AS complete_month_yoy_prev
    , DATE_ADD('MONTH', -12, month_12_prior)::DATE AS month_12_prior_yoy_prev
)
, dnr_exclude AS ( -- exclude DNR by Jiong's request
    SELECT DISTINCT group_id
    FROM weee_p01.pi_product_group
    WHERE group_id IN (7,27,501,521,702,707,713,1902,1905,1907,1907,1916,1925,1939,1939,1950,1964,2409,2426,4804,4807,1437,2928,1457
                      ,929,930,300084,1437,200135,200141,2928,1457,300095)
)
, sku_frame AS (
SELECT DISTINCT
    pc.num || ' - ' || pc.label_en AS department
    --, c.num || ' - ' || c.label_en AS category
    , p.storage_type
    , gi.group_id AS group_id
    , g.tier
    , g.ethnicity
    , p.id                         AS product_id
    , p.short_title_en || ' - '||p.title AS product_title
FROM
    weee_p01.gb_product        AS p
        -- taxonomy info
    JOIN weee_p01.gb_catalogue AS c
         ON c.num = p.catalogue_num
    JOIN weee_p01.gb_catalogue AS pc
         ON pc.num = c.parent_num
         AND pc.num NOT IN (18,50,51,99)
    JOIN weee_p01.pi_product_group_item AS gi
        ON gi.product_id = p.id
    JOIN weee_p01.pi_product_group AS g
        ON g.group_id = gi.group_id
WHERE gi.group_id NOT IN (SELECT group_id FROM dnr_exclude)
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
)

, time_frame AS (
SELECT delivery_month, delivery_week, delivery_day
FROM metrics.order AS o
WHERE
    -- 12 months prior - current complete month
    (delivery_month BETWEEN  (SELECT month_12_prior FROM time_range)
        AND (SELECT complete_month FROM time_range))
    -- YoY
    OR (delivery_month BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
        AND (SELECT complete_month_yoy_prev FROM time_range))
GROUP BY 1,2,3
ORDER BY 1,2,3)
, final_frame AS (
SELECT
    so1.id                         AS sales_org_id
    , so1.id || ' - ' || so1.title AS sales_org
    , so2.id || ' - ' || so2.title AS price_region
    , tf.delivery_month
    , tf.delivery_week
    , tf.delivery_day
    , sf.department
--     , sf.category
    , sf.group_id
    , sf.tier
    , sf.ethnicity
    , CASE
        WHEN ps.product_id IS NULL THEN 1
        WHEN date_part(w, tf.delivery_day)+1 between ps.start_week and ps.end_week then 1
        ELSE 0
      END AS seasonality_flag_sku
    , sf.product_id
    , sf.product_title
    , sf.storage_type
FROM
    weee_p01.gb_sales_org           AS so1
        -- price region
    JOIN weee_p01.sales_org_mapping AS som
         ON som.type = 'gb_product_sales'
             AND som.sales_org_id = so1.id
    JOIN weee_p01.gb_sales_org      AS so2
         ON so2.id = som.ref_sales_org_id
    JOIN time_frame                 AS tf ON TRUE
    JOIN sku_frame                  AS sf ON TRUE
    LEFT JOIN weee_p01.pi_product_season AS ps
        ON ps.product_id = sf.product_id
        AND ps.status <> 'X'
GROUP BY 1, 2, 3, 4, 5, 6, 7 ,8, 9, 10, 11, 12, 13, 14
ORDER BY 1, 2, 3, 4, 5, 6, 7 ,8, 9, 10, 11, 12, 13, 14)
, availability AS (
    SELECT
        lh.day  AS delivery_date
        , lh.sales_org_id AS sales_org_id
        , lh.product_id
        , NVL(lh.available, -1) AS availability
    FROM
        weee_p01.product_list_history AS lh
    WHERE
        -- 12 months prior - current complete month
        (lh.day BETWEEN  (SELECT month_12_prior FROM time_range)
            AND LAST_DAY((SELECT complete_month FROM time_range)))
        -- YoY
        OR (lh.day BETWEEN (SELECT month_12_prior_yoy_prev FROM time_range)
            AND LAST_DAY((SELECT complete_month_yoy_prev FROM time_range)))
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4)
, base AS (
    SELECT
        price_region, delivery_month, delivery_week, delivery_day, department, group_id, tier, ethnicity, storage_type
        , DATE_ADD('MONTH', 12, f.delivery_month)::DATE AS yoy_month
        , DATE_ADD('WEEK', 52, f.delivery_week)::DATE AS yoy_week
        , MAX(seasonality_flag_sku) AS seasonality_flag
        , MAX(NVL(a.availability, 0)) AS group_availability

    FROM
        final_frame            AS f
        LEFT JOIN availability AS a
                  ON a.product_id = f.product_id
                      AND a.delivery_date = f.delivery_day
                      AND a.sales_org_id = f.sales_org_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
, monthly_avg AS (
    SELECT
        price_region, delivery_month, department, group_id, tier, ethnicity, storage_type, seasonality_flag
        , yoy_month
        , AVG(group_availability::FLOAT) AS monthly_group_availability
    FROM base
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY 1,2,3,4,5,6,7,8,9
)

SELECT curr.*, prev.monthly_group_availability AS prev_availability
INTO sandbox.sz_temp_app_wbr_instock_monthly
FROM monthly_avg AS curr
    JOIN monthly_avg AS prev
        ON prev.yoy_month = curr.delivery_month
        AND prev.price_region = curr.price_region
        AND prev.group_id = curr.group_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1,2,3,4,5,6,7,8,9,10,11;
GRANT SELECT ON sandbox.sz_temp_app_wbr_instock_monthly TO chartio;
