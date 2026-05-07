-- ============================================================
-- dbt/models/staging/stg_orders.sql
-- Staging layer: clean and type-cast raw ERP orders
-- Author: Jahnav Jayanth Reddy Kukkala
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('erp', 'raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        product_id,
        product_name,
        supplier_id,
        supplier_name,
        warehouse,
        CAST(order_date    AS DATE)    AS order_date,
        CAST(delivery_date AS DATE)    AS delivery_date,
        CAST(quantity      AS INTEGER) AS quantity,
        CAST(unit_cost     AS FLOAT)   AS unit_cost,
        CAST(total_cost    AS FLOAT)   AS total_cost,
        UPPER(status)                  AS status,
        category,

        -- Derived fields
        DATEDIFF('day', order_date, delivery_date)  AS lead_time_days,
        DATE_TRUNC('month', order_date)             AS order_month,
        DATE_TRUNC('week',  order_date)             AS order_week,
        EXTRACT('year'  FROM order_date)            AS order_year,
        EXTRACT('month' FROM order_date)            AS order_month_num,
        EXTRACT('dow'   FROM order_date)            AS order_dow

    FROM source
    WHERE order_id IS NOT NULL
      AND quantity  > 0
      AND total_cost >= 0
)

SELECT * FROM cleaned
