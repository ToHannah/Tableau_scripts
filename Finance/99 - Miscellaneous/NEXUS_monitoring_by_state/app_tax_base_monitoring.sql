WITH
base AS (
    SELECT
        CASE WHEN op.sales_org_id IN (3, 16) THEN 'Mail Order' ELSE 'Local Delivery' END AS biz_type
        , ai.area_en                                                                    AS state
        , op.delivery_month
        , op.order_id
        , NVL(i.group_invoice_id, i.id)                                                 AS delivery_id
        , CASE WHEN op.product_type = 'hotdish' THEN 'prepared food' ELSE 'grocery' END AS product_type
        , op.biz_type AS biz_type_code
        , SUM(op.sub_total)                                                             AS gross_revenue
        , SUM(r.refund_amount)                                                          AS refund_amount
    FROM
        metrics.order_product         AS op
        JOIN weee_p01.gb_zipcode_area AS za
             ON za.zipcode = op.addr_zipcode
        JOIN weee_p01.area_info       AS ai
             ON ai.id = za.state
        JOIN weee_p01.gb_invoice      AS i
             ON i.id = op.invoice_id
        LEFT JOIN metrics.return      AS r
                  ON r.order_id = op.order_id
                      AND r.product_id = op.product_id
    WHERE
        op.delivery_day BETWEEN '2019-01-01' AND CURRENT_DATE
        AND op.payment_mode = 'F'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ORDER BY 1, 2, 3, 4, 5, 6, 7
)  -- SELECT order_id, COUNT(*) AS cnt FROM base GROUP BY 1 HAVING cnt>1 ORDER BY 1;
    -- sanity check: passed

SELECT b.*, o.discount AS discount_amount
FROM base AS b
    JOIN metrics.order AS o
        ON o.order_id = b.order_id
ORDER BY 1,2,3,4,5,6,7,8
