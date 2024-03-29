WITH
sku_frame AS (
SELECT
    pc.num || ' - ' || pc.label_en AS department
    , c.num || ' - ' || c.label_en AS category
    , p.storage_type
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
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)

, sku_group_frame AS (
    SELECT DISTINCT
        gi.product_id, g.group_id ,g.ethnicity, g.tier
        , CASE WHEN ps.product_id IS NOT NULL THEN 1 ELSE 0 END AS seasonality_flag_sku
        , MAX(seasonality_flag_sku) OVER (PARTITION BY g.group_id) AS seasonality_flag
    FROM weee_p01.pi_product_group AS g
        JOIN weee_p01.pi_product_group_item AS gi
            ON gi.group_id = g.group_id
        LEFT JOIN weee_p01.pi_product_season AS ps
            ON ps.product_id = gi.product_id
            AND ps.status <> 'X'
    WHERE g.group_id NOT IN (7,27,501,521,702,707,713,1902,1905,1907,1907,1916,1925,1939,1939,1950,1964,2409,2426,4804,4807,1437,2928,1457
                      ,929,930,300084,1437,200135,200141,2928,1457,300095)
    ORDER BY 1,2,3,4
)

, time_frame AS (
SELECT delivery_day
FROM metrics.order AS o
WHERE
    o.delivery_day BETWEEN DATE_ADD('DAY', -71, CURRENT_DATE)::DATE AND CURRENT_DATE
GROUP BY 1
ORDER BY 1)

, final_frame AS (
SELECT
    so1.id                         AS sales_org_id
    , so1.id || ' - ' || so1.title AS sales_org
    , so2.id || ' - ' || so2.title AS price_region
    , tf.delivery_day
    , sf.product_id
    , sf.group_id
    , sf.tier
    , sf.ethnicity
    , sf.seasonality_flag
    , sf.seasonality_flag_sku
FROM
    weee_p01.gb_sales_org           AS so1
        -- price region
    JOIN weee_p01.sales_org_mapping AS som
         ON som.type = 'gb_product_sales'
             AND som.sales_org_id = so1.id
    JOIN weee_p01.gb_sales_org      AS so2
         ON so2.id = som.ref_sales_org_id
    JOIN time_frame                 AS tf ON TRUE
    JOIN sku_group_frame                  AS sf ON TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
, availability AS (
    SELECT
        lh.day  AS delivery_day
        , lh.sales_org_id AS sales_org_id
        , lh.product_id
        , NVL(lh.available, -1) AS availability
    FROM
        weee_p01.product_list_history AS lh
    WHERE lh.day BETWEEN DATE_ADD('DAY', -71, CURRENT_DATE)::DATE AND CURRENT_DATE
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4)
SELECT
    f.*
    , sf.department
    , sf.category
    , sf.storage_type
    , sf.product_title
    , NVL(a.availability,-1) AS availability
FROM final_frame AS f
    JOIN sku_frame AS sf
        ON sf.product_id = f.product_id
    LEFT JOIN availability      AS a
    ON a.product_id = f.product_id
        AND a.delivery_day = f.delivery_day
        AND a.sales_org_id = f.sales_org_id
ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
