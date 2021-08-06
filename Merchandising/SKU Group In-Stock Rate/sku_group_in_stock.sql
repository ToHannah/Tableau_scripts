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
ORDER BY 1, 2, 3, 4, 5)

, sku_group_frame AS (
    SELECT product_id, sgm.group AS group_id, sgm.tier
    FROM sandbox.sku_group_mapping AS sgm
    GROUP BY 1,2,3
    ORDER BY 1,2,3
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
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7)
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
ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12
