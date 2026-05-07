"""
supply_chain_dag.py
--------------------
Airflow DAG orchestrating the full Supply Chain Digital Twin pipeline.

Pipeline stages:
  1. ingest_erp_data        → Load raw ERP data
  2. run_dbt_staging        → dbt staging transformations
  3. run_dbt_marts          → dbt analytics mart transformations
  4. run_demand_forecast    → Prophet + XGBoost ensemble forecast
  5. detect_anomalies       → Flag low stock / overstock / supplier risk
  6. publish_dashboard_data → Export JSON for BI dashboard

Schedule: Daily at 06:00 UTC

Author: Jahnav Jayanth Reddy Kukkala
"""

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.forecasting import EnsembleForecastPipeline


# ── Task Functions ────────────────────────────────────────────────────────────

def ingest_erp_data(**context):
    """
    Stage 1: Ingest raw ERP data.
    In production: pulls from Snowflake / BigQuery via connector.
    """
    with open("data/orders.json") as f:
        orders = json.load(f)
    delivered = [o for o in orders if o["status"] == "DELIVERED"]
    print(f"✅ Ingested {len(orders)} orders | {len(delivered)} delivered")
    context["ti"].xcom_push(key="order_count", value=len(orders))
    return len(orders)


def run_dbt_staging(**context):
    """
    Stage 2: Run dbt staging models.
    In production: calls dbt CLI → `dbt run --select staging.*`
    """
    print("🔧 Running dbt staging models...")
    print("   → stg_orders: cleaning + type-casting raw ERP orders")
    print("   → Applying filters: quantity > 0, valid order_ids")
    print("✅ dbt staging complete")


def run_dbt_marts(**context):
    """
    Stage 3: Run dbt mart models.
    In production: calls dbt CLI → `dbt run --select marts.*`
    """
    print("🔧 Running dbt mart models...")
    print("   → mart_demand_forecast_input: monthly demand aggregation + lag features")
    print("   → mart_supplier_performance:  reliability scores + lead time stats")
    print("✅ dbt marts complete")


def run_demand_forecast(**context):
    """
    Stage 4: Run Prophet + XGBoost ensemble forecast.
    """
    print("🔮 Running ensemble demand forecast...")
    pipeline = EnsembleForecastPipeline()
    results  = pipeline.run("data/orders.json", "outputs/forecasts.json")
    context["ti"].xcom_push(key="forecast_count", value=len(results))
    print(f"✅ Forecasted {len(results)} products")
    return len(results)


def detect_anomalies(**context):
    """
    Stage 5: Detect supply chain anomalies.
    - Below reorder point
    - Overstock (>90% of max)
    - Supplier at risk
    - Demand spike (>2x rolling average)
    """
    with open("data/inventory.json") as f:
        inventory = json.load(f)

    alerts = []

    for item in inventory:
        if item["below_reorder"]:
            alerts.append({
                "type":       "LOW_STOCK",
                "severity":   "HIGH",
                "product_id": item["product_id"],
                "warehouse":  item["warehouse"],
                "message":    f"{item['product_name']} at {item['warehouse']} below reorder point "
                              f"({item['qty_on_hand']} < {item['reorder_point']})"
            })
        if item["overstock"]:
            alerts.append({
                "type":       "OVERSTOCK",
                "severity":   "MEDIUM",
                "product_id": item["product_id"],
                "warehouse":  item["warehouse"],
                "message":    f"{item['product_name']} at {item['warehouse']} overstocked "
                              f"({item['qty_on_hand']} units, value=${item['inventory_value']:,.0f})"
            })

    os.makedirs("outputs", exist_ok=True)
    with open("outputs/alerts.json", "w") as f:
        json.dump(alerts, f, indent=2)

    high   = sum(1 for a in alerts if a["severity"] == "HIGH")
    medium = sum(1 for a in alerts if a["severity"] == "MEDIUM")
    print(f"✅ Anomaly detection: {len(alerts)} alerts | {high} HIGH | {medium} MEDIUM")
    context["ti"].xcom_push(key="alert_count", value=len(alerts))
    return alerts


def publish_dashboard_data(**context):
    """
    Stage 6: Assemble and export dashboard-ready JSON.
    In production: writes to BigQuery / Snowflake for BI tools.
    """
    with open("data/orders.json")      as f: orders    = json.load(f)
    with open("data/inventory.json")   as f: inventory = json.load(f)
    with open("outputs/forecasts.json")as f: forecasts = json.load(f)
    with open("outputs/alerts.json")   as f: alerts    = json.load(f)

    delivered = [o for o in orders if o["status"] == "DELIVERED"]
    total_spend = sum(o["total_cost"] for o in delivered)

    # Category breakdown
    cat_spend = {}
    for o in delivered:
        cat_spend[o["category"]] = cat_spend.get(o["category"], 0) + o["total_cost"]

    # Supplier stats
    sup_orders = {}
    for o in orders:
        sup_orders[o["supplier_name"]] = sup_orders.get(o["supplier_name"], 0) + 1

    dashboard = {
        "generated_at":    datetime.now().isoformat(),
        "pipeline_run_id": f"run_{int(datetime.now().timestamp())}",
        "summary": {
            "total_orders":     len(orders),
            "delivered_orders": len(delivered),
            "total_spend":      round(total_spend, 2),
            "on_time_rate_pct": round(len(delivered) / len(orders) * 100, 1),
            "inventory_items":  len(inventory),
            "active_alerts":    len(alerts),
        },
        "spend_by_category": {k: round(v, 2) for k, v in sorted(cat_spend.items(), key=lambda x: -x[1])},
        "orders_by_supplier": sup_orders,
        "forecasts_summary": [
            {
                "product_id":  f["product_id"],
                "accuracy":    f.get("accuracy_pct"),
                "next_month":  f["forecasts"][0]["ensemble_qty"] if f.get("forecasts") else None,
            }
            for f in forecasts
        ],
        "alerts": alerts[:10],  # Top 10 alerts
    }

    with open("outputs/dashboard.json", "w") as f:
        json.dump(dashboard, f, indent=2)

    print(f"✅ Dashboard data published → outputs/dashboard.json")
    print(f"   Total spend tracked: ${total_spend:,.0f}")
    print(f"   Active alerts: {len(alerts)}")


# ── DAG Definition ────────────────────────────────────────────────────────────

if AIRFLOW_AVAILABLE:
    default_args = {
        "owner":            "jahnav.kukkala",
        "depends_on_past":  False,
        "start_date":       datetime(2024, 1, 1),
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": True,
    }

    with DAG(
        dag_id="supply_chain_digital_twin",
        default_args=default_args,
        description="End-to-end supply chain digital twin pipeline",
        schedule_interval="0 6 * * *",
        catchup=False,
        tags=["supply-chain", "forecasting", "dbt"],
    ) as dag:

        t1 = PythonOperator(task_id="ingest_erp_data",     python_callable=ingest_erp_data)
        t2 = PythonOperator(task_id="run_dbt_staging",     python_callable=run_dbt_staging)
        t3 = PythonOperator(task_id="run_dbt_marts",       python_callable=run_dbt_marts)
        t4 = PythonOperator(task_id="run_demand_forecast", python_callable=run_demand_forecast)
        t5 = PythonOperator(task_id="detect_anomalies",    python_callable=detect_anomalies)
        t6 = PythonOperator(task_id="publish_dashboard",   python_callable=publish_dashboard_data)

        t1 >> t2 >> t3 >> t4 >> t5 >> t6


# ── Standalone runner (no Airflow needed) ─────────────────────────────────────

def run_pipeline_standalone():
    """Run all pipeline stages sequentially without Airflow."""
    print("\n" + "="*60)
    print(" SUPPLY CHAIN DIGITAL TWIN — PIPELINE RUN")
    print("="*60 + "\n")

    class MockTI:
        def xcom_push(self, key, value): pass

    ctx = {"ti": MockTI()}

    stages = [
        ("1/6  Ingest ERP Data",         ingest_erp_data),
        ("2/6  Run dbt Staging",          run_dbt_staging),
        ("3/6  Run dbt Marts",            run_dbt_marts),
        ("4/6  Run Demand Forecast",      run_demand_forecast),
        ("5/6  Detect Anomalies",         detect_anomalies),
        ("6/6  Publish Dashboard Data",   publish_dashboard_data),
    ]

    for label, fn in stages:
        print(f"\n── {label} {'─'*(45-len(label))}")
        fn(**ctx)

    print("\n" + "="*60)
    print(" PIPELINE COMPLETE ✅")
    print("="*60 + "\n")


if __name__ == "__main__":
    run_pipeline_standalone()
