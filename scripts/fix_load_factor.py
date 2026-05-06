import pandas as pd
import numpy as np
import sqlite3

np.random.seed(42)

print("="*50)
print("  LOAD FACTOR RECALCULATOR")
print("="*50)

# ── LOAD DATA ─────────────────────────────────────────
print("\nLoading data...")
riders_df = pd.read_csv("data/riders.csv")
orders_df = pd.read_csv("data/orders.csv", 
                         usecols=["order_id","zone_id","hour_of_day","date","is_weekend"])

print(f"  Riders: {len(riders_df):,}")
print(f"  Orders: {len(orders_df):,}")

# ── SHIFT HOURS ───────────────────────────────────────
SHIFT_HOURS = {
    "Morning":   set(range(6, 16)),
    "Afternoon": set(range(13, 23)),
    "Night":     set(range(21, 24)) | set(range(0, 7)),
}

# ── BUILD RIDER COUNT PER ZONE PER HOUR ───────────────
print("\nCalculating riders available per zone per hour...")

active_riders = riders_df[riders_df["status"] == "Active"]

# riders_per_zone_hour[zone][hour] = expected available count
# Apply average absence rate to get realistic available count
zone_hour_riders = {}
for zone in active_riders["current_zone"].unique():
    zone_hour_riders[zone] = {}
    zone_riders = active_riders[active_riders["current_zone"] == zone]
    
    for hour in range(24):
        # Count riders whose shift covers this hour
        on_shift = zone_riders[
            zone_riders["shift"].apply(lambda s: hour in SHIFT_HOURS[s])
        ]
        # Apply average absence rate
        avg_absence = on_shift["absence_rate"].mean() if len(on_shift) > 0 else 0.165
        expected_available = max(1, int(len(on_shift) * (1 - avg_absence)))
        zone_hour_riders[zone][hour] = expected_available

print("  ✓ Zone-hour rider counts calculated")

# Preview
print("\n  Riders available at dinner peak (hour 19) by zone:")
for zone in sorted(zone_hour_riders.keys()):
    count = zone_hour_riders[zone].get(19, 0)
    print(f"    {zone}: {count} riders")

# ── CALCULATE CORRECT LOAD FACTORS ────────────────────
print("\nCalculating order volumes per zone per hour per day...")

# Group orders by zone + hour + date to get hourly order counts
order_counts = orders_df.groupby(
    ["zone_id", "hour_of_day", "date"]
).size().reset_index(name="order_count")

print(f"  ✓ {len(order_counts):,} zone-hour-day combinations")

# Calculate load factor for each combination
def calc_load_factor(row):
    zone    = row["zone_id"]
    hour    = row["hour_of_day"]
    orders  = row["order_count"]
    riders  = zone_hour_riders.get(zone, {}).get(hour, 1)
    lf      = round(orders / max(riders, 1), 3)
    # Cap at realistic maximum
    return min(lf, 2.5)

print("\nCalculating load factors...")
order_counts["new_load_factor"]      = order_counts.apply(calc_load_factor, axis=1)
order_counts["new_available_riders"] = order_counts.apply(
    lambda r: zone_hour_riders.get(r["zone_id"], {}).get(r["hour_of_day"], 1), axis=1
)

# ── SHOW SUMMARY BEFORE UPDATING ──────────────────────
print("\nLoad factor summary by zone (dinner peak hour 19):")
peak   = order_counts[order_counts["hour_of_day"] == 19]
by_zone = peak.groupby("zone_id").agg(
    avg_orders=("order_count", "mean"),
    avg_riders=("new_available_riders", "mean"),
    avg_lf=("new_load_factor", "mean"),
    max_lf=("new_load_factor", "max"),
).reset_index()

for _, r in by_zone.iterrows():
    status = "✅ OK" if r["avg_lf"] <= 1.8 else "⚠️ HIGH" if r["avg_lf"] <= 2.5 else "🔴 CRITICAL"
    print(f"  {r['zone_id']}  AvgOrders:{r['avg_orders']:>6.0f}  "
          f"AvgRiders:{r['avg_riders']:>5.0f}  "
          f"AvgLF:{r['avg_lf']:.2f}  MaxLF:{r['max_lf']:.2f}  {status}")

# ── UPDATE SQLITE DATABASE ────────────────────────────
print("\nUpdating database with corrected load factors...")
print("(This will take a few minutes for 5.6M rows)")

conn = sqlite3.connect("database/deliveries.db")

# Build lookup dict for fast update
lf_lookup = {}
for _, row in order_counts.iterrows():
    key = (row["zone_id"], int(row["hour_of_day"]), row["date"])
    lf_lookup[key] = (row["new_load_factor"], int(row["new_available_riders"]))

# Update in batches
cur      = conn.cursor()
batch    = []
updated  = 0

cur.execute("SELECT order_id, zone_id, hour_of_day, date FROM orders")
rows = cur.fetchmany(100000)

while rows:
    for order_id, zone_id, hour, date in rows:
        key = (zone_id, hour, date)
        if key in lf_lookup:
            lf, avail = lf_lookup[key]
            batch.append((lf, avail, order_id))
    
    cur.executemany(
        "UPDATE orders SET load_factor=?, available_riders=? WHERE order_id=?",
        batch
    )
    updated += len(batch)
    batch    = []
    
    if updated % 500000 == 0:
        print(f"  ... {updated:,} rows updated")
    
    rows = cur.fetchmany(100000)

conn.commit()
conn.close()

print(f"  ✓ {updated:,} rows updated in database")

# ── ALSO UPDATE CSV ───────────────────────────────────
print("\nUpdating orders.csv...")
orders_full = pd.read_csv("data/orders.csv")
orders_full = orders_full.merge(
    order_counts[["zone_id","hour_of_day","date","new_load_factor","new_available_riders"]],
    on=["zone_id","hour_of_day","date"],
    how="left"
)
orders_full["load_factor"]      = orders_full["new_load_factor"].fillna(orders_full["load_factor"])
orders_full["available_riders"] = orders_full["new_available_riders"].fillna(orders_full["available_riders"]).astype(int)
orders_full = orders_full.drop(columns=["new_load_factor","new_available_riders"])
orders_full.to_csv("data/orders.csv", index=False)
print(f"  ✓ orders.csv updated")

print("\n" + "="*50)
print("  LOAD FACTOR FIX COMPLETE")
print("="*50)
print("\nExpected load factors after fix:")
print("  Peak hours (12-14, 18-21): 0.8 — 1.8")
print("  Off peak hours (0-6):      0.1 — 0.4")
print("  Normal hours:              0.4 — 1.0")
print("\nRun Query 1 again in DB Browser to verify")
print("="*50)