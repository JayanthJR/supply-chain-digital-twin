"""
main.py
--------
Entry point for the Supply Chain Digital Twin.
Runs the full pipeline end-to-end without Airflow.

Author: Jahnav Jayanth Reddy Kukkala
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.generate_erp_data import generate_orders, generate_inventory, PRODUCTS, SUPPLIERS, WAREHOUSES
from airflow.dags.supply_chain_dag import run_pipeline_standalone
import json

if __name__ == "__main__":
    # Step 0: Generate data if not present
    if not os.path.exists("data/orders.json"):
        print("📦 Generating synthetic ERP data...")
        os.makedirs("data", exist_ok=True)
        orders    = generate_orders(2000)
        inventory = generate_inventory(PRODUCTS, WAREHOUSES)
        with open("data/orders.json",    "w") as f: json.dump(orders,    f, indent=2)
        with open("data/inventory.json", "w") as f: json.dump(inventory, f, indent=2)
        with open("data/suppliers.json", "w") as f: json.dump(SUPPLIERS, f, indent=2)
        with open("data/products.json",  "w") as f: json.dump(PRODUCTS,  f, indent=2)
        print(f"✅ Data generated: {len(orders)} orders, {len(inventory)} inventory records\n")

    # Step 1–6: Run full pipeline
    run_pipeline_standalone()

    # Print output summary
    print("📂 Output files:")
    for fname in ["outputs/forecasts.json", "outputs/alerts.json", "outputs/dashboard.json"]:
        if os.path.exists(fname):
            size = os.path.getsize(fname)
            print(f"   {fname}  ({size:,} bytes)")
