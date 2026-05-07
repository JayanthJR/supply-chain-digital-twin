"""
forecasting.py
---------------
Prophet + XGBoost ensemble demand forecasting pipeline.
Trains both models, blends predictions, and outputs
a 90-day forward forecast per product.

In production: Prophet and XGBoost installed via requirements.
In this repo:  pure-Python fallback so pipeline runs without dependencies.

Author: Jahnav Jayanth Reddy Kukkala
"""

import json
import math
import random
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple


# ── Time Series Builder ───────────────────────────────────────────────────────

def build_time_series(orders: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Aggregate delivered orders into monthly time series per product.
    Returns {product_id: [{date, quantity, total_cost}, ...]}
    """
    series = defaultdict(lambda: defaultdict(lambda: {"quantity": 0, "total_cost": 0.0, "order_count": 0}))

    for o in orders:
        if o["status"] != "DELIVERED":
            continue
        month = o["order_date"][:7]  # YYYY-MM
        pid   = o["product_id"]
        series[pid][month]["quantity"]    += o["quantity"]
        series[pid][month]["total_cost"]  += o["total_cost"]
        series[pid][month]["order_count"] += 1

    result = {}
    for pid, months in series.items():
        result[pid] = [
            {"date": m, "quantity": v["quantity"],
             "total_cost": round(v["total_cost"], 2),
             "order_count": v["order_count"]}
            for m, v in sorted(months.items())
        ]
    return result


# ── Prophet-style Decomposition Model ────────────────────────────────────────

class ProphetLiteModel:
    """
    Lightweight Prophet-inspired model.
    Decomposes time series into: trend + seasonality + noise.
    In production, swap with: from prophet import Prophet
    """

    def __init__(self, seasonality_periods: int = 12):
        self.seasonality_periods = seasonality_periods
        self.trend_slope    = 0.0
        self.trend_intercept = 0.0
        self.seasonal_factors = {}
        self._fitted = False

    def fit(self, dates: List[str], values: List[float]) -> "ProphetLiteModel":
        n = len(values)
        if n < 3:
            self.trend_slope = 0
            self.trend_intercept = sum(values) / n if n else 0
            self._fitted = True
            return self

        # Linear trend via least squares
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(values) / n
        num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, values))
        den = sum((xi - x_mean) ** 2 for xi in x) or 1
        self.trend_slope     = num / den
        self.trend_intercept = y_mean - self.trend_slope * x_mean

        # Seasonal factors by month
        monthly_totals = defaultdict(list)
        for date, val in zip(dates, values):
            month_num = int(date.split("-")[1])
            trend_val = self.trend_intercept + self.trend_slope * dates.index(date)
            if trend_val > 0:
                monthly_totals[month_num].append(val / trend_val)

        self.seasonal_factors = {
            m: sum(v) / len(v) for m, v in monthly_totals.items()
        }
        self._fitted = True
        return self

    def predict(self, future_dates: List[str], last_index: int) -> List[float]:
        preds = []
        for i, date in enumerate(future_dates):
            idx        = last_index + i + 1
            trend      = self.trend_intercept + self.trend_slope * idx
            month_num  = int(date.split("-")[1])
            seasonal   = self.seasonal_factors.get(month_num, 1.0)
            preds.append(max(0, trend * seasonal))
        return preds


# ── XGBoost-style Gradient Boosting (pure Python) ────────────────────────────

class XGBoostLiteModel:
    """
    Simplified gradient boosting regressor.
    Uses lag features: [lag1, lag3, lag6, rolling_mean_3, month_sin, month_cos].
    In production, swap with: import xgboost as xgb
    """

    def __init__(self, n_estimators: int = 50, learning_rate: float = 0.1):
        self.n_estimators  = n_estimators
        self.learning_rate = learning_rate
        self.trees: List[Dict] = []
        self.base_pred     = 0.0
        self._fitted       = False

    def _build_features(self, values: List[float], idx: int) -> List[float]:
        """Build feature vector for index idx."""
        def safe_get(i): return values[i] if 0 <= i < len(values) else values[0] if values else 0

        lag1 = safe_get(idx - 1)
        lag3 = safe_get(idx - 3)
        lag6 = safe_get(idx - 6)
        roll3 = sum(safe_get(idx - k) for k in range(1, 4)) / 3
        # Approximate month from index (12-month cycle)
        angle = (idx % 12) * 2 * math.pi / 12
        return [lag1, lag3, lag6, roll3, math.sin(angle), math.cos(angle)]

    def _simple_tree_predict(self, tree: Dict, features: List[float]) -> float:
        """Single stump: split on one feature."""
        feat_idx   = tree["feat_idx"]
        threshold  = tree["threshold"]
        left_val   = tree["left_val"]
        right_val  = tree["right_val"]
        return left_val if features[feat_idx] <= threshold else right_val

    def fit(self, values: List[float]) -> "XGBoostLiteModel":
        n = len(values)
        if n < 6:
            self.base_pred = sum(values) / n if values else 0
            self._fitted   = True
            return self

        self.base_pred = sum(values) / n
        residuals      = [v - self.base_pred for v in values]

        for _ in range(self.n_estimators):
            # Build a decision stump on best feature
            X = [self._build_features(values, i) for i in range(n)]
            best_tree  = None
            best_loss  = float("inf")

            for feat_idx in range(len(X[0])):
                feat_vals = sorted(set(x[feat_idx] for x in X))
                for threshold in feat_vals[::max(1, len(feat_vals)//5)]:
                    left  = [r for x, r in zip(X, residuals) if x[feat_idx] <= threshold]
                    right = [r for x, r in zip(X, residuals) if x[feat_idx] >  threshold]
                    if not left or not right:
                        continue
                    lv   = sum(left)  / len(left)
                    rv   = sum(right) / len(right)
                    loss = sum((r - lv)**2 for r in left) + sum((r - rv)**2 for r in right)
                    if loss < best_loss:
                        best_loss = loss
                        best_tree = {"feat_idx": feat_idx, "threshold": threshold,
                                     "left_val": lv, "right_val": rv}

            if best_tree:
                self.trees.append(best_tree)
                preds     = [self._simple_tree_predict(best_tree, x) for x in X]
                residuals = [r - self.learning_rate * p for r, p in zip(residuals, preds)]

        self._fitted = True
        return self

    def predict_next(self, values: List[float], steps: int = 3) -> List[float]:
        preds   = []
        history = list(values)
        for i in range(steps):
            features = self._build_features(history, len(history))
            pred     = self.base_pred
            for tree in self.trees:
                pred += self.learning_rate * self._simple_tree_predict(tree, features)
            pred = max(0, pred)
            preds.append(pred)
            history.append(pred)
        return preds


# ── Ensemble Forecaster ───────────────────────────────────────────────────────

class EnsembleForecastPipeline:
    """
    Blends Prophet + XGBoost predictions (60/40 weighted average).
    Produces 3-month forward forecasts per product.
    """

    def __init__(self, prophet_weight: float = 0.6, xgb_weight: float = 0.4):
        self.prophet_weight = prophet_weight
        self.xgb_weight     = xgb_weight

    def _next_months(self, last_date: str, n: int) -> List[str]:
        year, month = int(last_date[:4]), int(last_date[5:7])
        result = []
        for _ in range(n):
            month += 1
            if month > 12:
                month = 1
                year += 1
            result.append(f"{year}-{month:02d}")
        return result

    def forecast_product(self, product_id: str, series: List[Dict],
                         forecast_months: int = 3) -> Dict:
        dates  = [s["date"] for s in series]
        values = [float(s["quantity"]) for s in series]

        if len(values) < 3:
            return {"product_id": product_id, "error": "Insufficient data"}

        # Prophet-lite
        prophet = ProphetLiteModel()
        prophet.fit(dates, values)
        future_dates   = self._next_months(dates[-1], forecast_months)
        prophet_preds  = prophet.predict(future_dates, len(values) - 1)

        # XGBoost-lite
        xgb = XGBoostLiteModel()
        xgb.fit(values)
        xgb_preds = xgb.predict_next(values, steps=forecast_months)

        # Ensemble blend
        ensemble = [
            round(self.prophet_weight * p + self.xgb_weight * x)
            for p, x in zip(prophet_preds, xgb_preds)
        ]

        # Historical accuracy (last 3 months back-test)
        if len(values) >= 6:
            bt_prophet = prophet.predict(dates[-3:], len(values) - 4)
            bt_xgb     = xgb.predict_next(values[:-3], steps=3)
            bt_blend   = [self.prophet_weight * p + self.xgb_weight * x
                          for p, x in zip(bt_prophet, bt_xgb)]
            actuals    = values[-3:]
            mape = sum(abs(a - f) / max(a, 1) for a, f in zip(actuals, bt_blend)) / 3 * 100
            accuracy   = round(max(0, 100 - mape), 1)
        else:
            accuracy = None

        return {
            "product_id":     product_id,
            "forecast_months": forecast_months,
            "historical_avg":  round(sum(values) / len(values), 1),
            "accuracy_pct":    accuracy,
            "forecasts": [
                {
                    "month":          date,
                    "prophet_qty":    round(p),
                    "xgb_qty":        round(x),
                    "ensemble_qty":   e,
                    "confidence_low": round(e * 0.85),
                    "confidence_high":round(e * 1.15),
                }
                for date, p, x, e in zip(future_dates, prophet_preds, xgb_preds, ensemble)
            ]
        }

    def run(self, orders_path: str, output_path: str) -> List[Dict]:
        with open(orders_path) as f:
            orders = json.load(f)

        time_series = build_time_series(orders)
        results     = []

        print(f"🔮 Forecasting demand for {len(time_series)} products...")
        for pid, series in time_series.items():
            result = self.forecast_product(pid, series)
            results.append(result)
            acc = f"{result.get('accuracy_pct', 'N/A')}%" if result.get('accuracy_pct') else "N/A"
            print(f"   {pid} | {len(series)} months history | accuracy={acc}")

        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)

        avg_acc = sum(r["accuracy_pct"] for r in results if r.get("accuracy_pct")) / len(results)
        print(f"\n✅ Forecasts saved → {output_path}")
        print(f"   Avg forecast accuracy: {avg_acc:.1f}%")
        return results


if __name__ == "__main__":
    pipeline = EnsembleForecastPipeline()
    pipeline.run("data/orders.json", "outputs/forecasts.json")
