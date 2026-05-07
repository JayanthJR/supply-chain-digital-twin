-- ============================================================
-- dbt/models/marts/mart_demand_forecast_input.sql
-- Analytics mart: aggregated demand signals for forecasting
-- Author: Jahnav Jayanth Reddy Kukkala
-- ============================================================

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'DELIVERED'
),

monthly_demand AS (
    SELECT
        product_id,
        product_name,
        category,
        order_month,
        order_year,
        order_month_num,
        SUM(quantity)               AS total_quantity,
        SUM(total_cost)             AS total_spend,
        COUNT(DISTINCT order_id)    AS order_count,
        COUNT(DISTINCT supplier_id) AS supplier_count,
        AVG(lead_time_days)         AS avg_lead_time,
        AVG(unit_cost)              AS avg_unit_cost,

        -- Lag features for forecasting
        LAG(SUM(quantity), 1) OVER (
            PARTITION BY product_id ORDER BY order_month
        ) AS qty_lag_1m,

        LAG(SUM(quantity), 3) OVER (
            PARTITION BY product_id ORDER BY order_month
        ) AS qty_lag_3m,

        LAG(SUM(quantity), 12) OVER (
            PARTITION BY product_id ORDER BY order_month
        ) AS qty_lag_12m,

        -- 3-month rolling average
        AVG(SUM(quantity)) OVER (
            PARTITION BY product_id
            ORDER BY order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS qty_rolling_3m_avg

    FROM orders
    GROUP BY
        product_id, product_name, category,
        order_month, order_year, order_month_num
)

SELECT
    *,
    -- Month-over-month growth
    CASE
        WHEN qty_lag_1m IS NOT NULL AND qty_lag_1m > 0
        THEN ROUND((total_quantity - qty_lag_1m) / qty_lag_1m * 100, 2)
        ELSE NULL
    END AS mom_growth_pct,

    -- Year-over-year growth
    CASE
        WHEN qty_lag_12m IS NOT NULL AND qty_lag_12m > 0
        THEN ROUND((total_quantity - qty_lag_12m) / qty_lag_12m * 100, 2)
        ELSE NULL
    END AS yoy_growth_pct

FROM monthly_demand
ORDER BY product_id, order_month


-- ============================================================
-- dbt/models/marts/mart_supplier_performance.sql
-- Supplier reliability and lead time analytics
-- ============================================================
