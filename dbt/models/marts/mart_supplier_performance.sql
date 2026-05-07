-- ============================================================
-- dbt/models/marts/mart_supplier_performance.sql
-- Supplier reliability, lead time, and spend analytics
-- Author: Jahnav Jayanth Reddy Kukkala
-- ============================================================

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

supplier_stats AS (
    SELECT
        supplier_id,
        supplier_name,
        COUNT(DISTINCT order_id)                              AS total_orders,
        SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
        SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(total_cost)                                        AS total_spend,
        AVG(lead_time_days)                                    AS avg_lead_time_days,
        MIN(lead_time_days)                                    AS min_lead_time_days,
        MAX(lead_time_days)                                    AS max_lead_time_days,
        STDDEV(lead_time_days)                                 AS lead_time_stddev,
        COUNT(DISTINCT product_id)                             AS products_supplied,
        COUNT(DISTINCT warehouse)                              AS warehouses_served

    FROM orders
    GROUP BY supplier_id, supplier_name
),

scored AS (
    SELECT
        *,
        ROUND(delivered_orders * 1.0 / NULLIF(total_orders, 0) * 100, 1) AS delivery_rate_pct,
        ROUND(cancelled_orders * 1.0 / NULLIF(total_orders, 0) * 100, 1) AS cancellation_rate_pct,

        -- Reliability score (0-100): penalizes high lead time variance and cancellations
        ROUND(
            100
            - (lead_time_stddev * 2)
            - (cancelled_orders * 1.0 / NULLIF(total_orders, 0) * 20),
        1) AS reliability_score,

        CASE
            WHEN delivery_rate_pct >= 95 THEN 'PREFERRED'
            WHEN delivery_rate_pct >= 85 THEN 'APPROVED'
            ELSE 'AT_RISK'
        END AS supplier_tier

    FROM supplier_stats
)

SELECT * FROM scored
ORDER BY reliability_score DESC
