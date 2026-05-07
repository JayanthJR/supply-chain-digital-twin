"""
generate_erp_data.py
---------------------
Generates synthetic ERP supply chain data:
  - data/orders.json         Raw purchase orders
  - data/inventory.json      Inventory snapshots
  - data/suppliers.json      Supplier master data
  - data/products.json       Product catalog

Author: Jahnav Jayanth Reddy Kukkala
"""

import json
import random
import math
import os
from datetime import datetime, timedelta

random.seed(42)

PRODUCTS = [
    {"id": "PRD-001", "name": "Industrial Valve A",     "category": "Components",  "unit_cost": 45.0},
    {"id": "PRD-002", "name": "Steel Pipe B",            "category": "Raw Material","unit_cost": 12.5},
    {"id": "PRD-003", "name": "Control Panel X",         "category": "Electronics", "unit_cost": 320.0},
    {"id": "PRD-004", "name": "Bearing Assembly C",      "category": "Components",  "unit_cost": 28.0},
    {"id": "PRD-005", "name": "Chemical Compound D",     "category": "Raw Material","unit_cost": 8.75},
    {"id": "PRD-006", "name": "Sensor Module E",         "category": "Electronics", "unit_cost": 150.0},
    {"id": "PRD-007", "name": "Hydraulic Pump F",        "category": "Machinery",   "unit_cost": 890.0},
    {"id": "PRD-008", "name": "Gasket Kit G",            "category": "Components",  "unit_cost": 15.0},
]

SUPPLIERS = [
    {"id": "SUP-001", "name": "Alpha Supply Co",    "lead_time_days": 7,  "reliability": 0.95},
    {"id": "SUP-002", "name": "Beta Materials Inc", "lead_time_days": 14, "reliability": 0.88},
    {"id": "SUP-003", "name": "Gamma Industrial",   "lead_time_days": 5,  "reliability": 0.97},
    {"id": "SUP-004", "name": "Delta Components",   "lead_time_days": 21, "reliability": 0.82},
    {"id": "SUP-005", "name": "Epsilon Tech",       "lead_time_days": 10, "reliability": 0.91},
]

WAREHOUSES = ["WH-NORTH", "WH-SOUTH", "WH-EAST", "WH-WEST"]
STATUSES   = ["DELIVERED", "PENDING", "IN_TRANSIT", "CANCELLED"]
STATUS_W   = [0.70, 0.15, 0.10, 0.05]


def generate_orders(n: int = 2000) -> list:
    orders = []
    start = datetime(2022, 1, 1)
    for i in range(n):
        product  = random.choice(PRODUCTS)
        supplier = random.choice(SUPPLIERS)
        order_date = start + timedelta(days=random.randint(0, 730))
        qty = random.randint(10, 500)
        status = random.choices(STATUSES, weights=STATUS_W)[0]

        # Seasonal demand signal
        month = order_date.month
        seasonal_factor = 1 + 0.3 * math.sin((month - 3) * math.pi / 6)
        qty = int(qty * seasonal_factor)

        orders.append({
            "order_id":      f"ORD-{i+1:05d}",
            "product_id":    product["id"],
            "product_name":  product["name"],
            "supplier_id":   supplier["id"],
            "supplier_name": supplier["name"],
            "warehouse":     random.choice(WAREHOUSES),
            "order_date":    order_date.strftime("%Y-%m-%d"),
            "delivery_date": (order_date + timedelta(days=supplier["lead_time_days"] + random.randint(-2, 5))).strftime("%Y-%m-%d"),
            "quantity":      qty,
            "unit_cost":     product["unit_cost"],
            "total_cost":    round(qty * product["unit_cost"], 2),
            "status":        status,
            "category":      product["category"],
        })
    return orders


def generate_inventory(products: list, warehouses: list) -> list:
    inventory = []
    snapshot_date = datetime(2024, 1, 1)
    for product in products:
        for wh in warehouses:
            qty_on_hand    = random.randint(50, 2000)
            reorder_point  = random.randint(100, 400)
            max_stock      = random.randint(1000, 5000)
            inventory.append({
                "snapshot_date":  snapshot_date.strftime("%Y-%m-%d"),
                "product_id":     product["id"],
                "product_name":   product["name"],
                "warehouse":      wh,
                "qty_on_hand":    qty_on_hand,
                "reorder_point":  reorder_point,
                "max_stock":      max_stock,
                "below_reorder":  qty_on_hand < reorder_point,
                "overstock":      qty_on_hand > max_stock * 0.9,
                "inventory_value":round(qty_on_hand * product["unit_cost"], 2),
            })
    return inventory


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    orders    = generate_orders(2000)
    inventory = generate_inventory(PRODUCTS, WAREHOUSES)

    with open("data/orders.json",    "w") as f: json.dump(orders,    f, indent=2)
    with open("data/inventory.json", "w") as f: json.dump(inventory, f, indent=2)
    with open("data/suppliers.json", "w") as f: json.dump(SUPPLIERS, f, indent=2)
    with open("data/products.json",  "w") as f: json.dump(PRODUCTS,  f, indent=2)

    total_spend = sum(o["total_cost"] for o in orders if o["status"] == "DELIVERED")
    print(f"✅ Generated {len(orders)} orders  |  Total delivered spend: ${total_spend:,.0f}")
    print(f"✅ Generated {len(inventory)} inventory records")
    print(f"✅ Saved → data/orders.json, inventory.json, suppliers.json, products.json")
