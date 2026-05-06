import pandas as pd
import sqlite3
import numpy as np

np.random.seed(42)

print("="*50)
print("  RESTAURANT REMAPPER")
print("="*50)

# ── LOAD RESTAURANTS ──────────────────────────────────
print("\n[1/4] Loading restaurants_zoned.csv...")
rest_df = pd.read_csv("data/restaurants_zoned.csv")

# Keep only mapped restaurants (not Z00)
rest_df = rest_df[rest_df["zone_id"] != "Z00"].copy()

# Clean up
rest_df = rest_df[["restaurant_id","name","cuisine",
                    "latitude","longitude","zone_id",
                    "zone_name","delivery","takeaway"]]

# Add platform rating (simulated — Noon internal rating)
rest_df["platform_rating"] = np.round(
    np.random.uniform(2.5, 5.0, len(rest_df)), 1
)

# Add avg prep time per restaurant
# 15% are slow restaurants
rest_df["is_slow"] = np.random.random(len(rest_df)) < 0.15
rest_df["avg_prep_minutes"] = np.where(
    rest_df["is_slow"],
    np.random.randint(28, 55, len(rest_df)),
    np.random.randint(7, 22, len(rest_df))
)

print(f"   ✓ {len(rest_df):,} restaurants loaded")
print(f"   ✓ Zones covered: {rest_df['zone_id'].nunique()}")
print(f"   ✓ Slow restaurants: {rest_df['is_slow'].sum()}")

# ── BUILD ZONE RESTAURANT LOOKUP ──────────────────────
print("\n[2/4] Building zone restaurant lookup...")
zone_restaurants = {}
for zone in rest_df["zone_id"].unique():
    zone_df = rest_df[rest_df["zone_id"] == zone]
    zone_restaurants[zone] = zone_df["restaurant_id"].tolist()
    print(f"   Zone {zone}: {len(zone_df)} restaurants")

# ── REMAP ORDERS ──────────────────────────────────────
print("\n[3/4] Remapping restaurant IDs in orders...")
print("   Loading orders.csv (large file)...")

# Process in chunks to save memory
chunk_size = 100000
total_processed = 0
first_chunk = True

for chunk in pd.read_csv("data/orders.csv", chunksize=chunk_size):
    # Map each order to a real restaurant ID based on zone
    def get_real_restaurant(row):
        zone = row["zone_id"]
        if zone in zone_restaurants and len(zone_restaurants[zone]) > 0:
            # Use deterministic selection based on synthetic ID
            # so same synthetic ID always maps to same real restaurant
            synthetic_num = int(row["restaurant_id"].split("_R")[-1])
            idx = synthetic_num % len(zone_restaurants[zone])
            return zone_restaurants[zone][idx]
        return row["restaurant_id"]

    chunk["restaurant_id"] = chunk.apply(get_real_restaurant, axis=1)

    # Save chunk
    mode = "w" if first_chunk else "a"
    header = first_chunk
    chunk.to_csv("data/orders_remapped.csv",
                 mode=mode, header=header, index=False)
    first_chunk = False
    total_processed += len(chunk)

    if total_processed % 500000 == 0:
        print(f"   ... {total_processed:,} orders remapped")

print(f"   ✓ {total_processed:,} orders remapped")

# Replace original orders file
import os
os.replace("data/orders_remapped.csv", "data/orders.csv")
print("   ✓ orders.csv updated with real restaurant IDs")

# ── SAVE RESTAURANTS TO CSV ───────────────────────────
rest_df.to_csv("data/restaurants.csv", index=False)
print("\n   ✓ data/restaurants.csv saved")

# ── LOAD INTO SQLITE ──────────────────────────────────
print("\n[4/4] Loading restaurants into SQLite...")
conn = sqlite3.connect("database/deliveries.db")

# Drop and recreate restaurants table
conn.execute("DROP TABLE IF EXISTS restaurants")
conn.execute("""
    CREATE TABLE restaurants (
        restaurant_id    TEXT PRIMARY KEY,
        name             TEXT,
        cuisine          TEXT,
        latitude         REAL,
        longitude        REAL,
        zone_id          TEXT,
        zone_name        TEXT,
        delivery         TEXT,
        takeaway         TEXT,
        platform_rating  REAL,
        is_slow          INTEGER,
        avg_prep_minutes INTEGER
    )
""")

rest_df.to_sql("restaurants", conn,
               if_exists="append", index=False)
conn.commit()

count = pd.read_sql(
    "SELECT COUNT(*) as c FROM restaurants", conn
).iloc[0]["c"]
print(f"   ✓ {count:,} restaurants loaded into SQLite")
conn.close()

print("\n" + "="*50)
print("  REMAPPING COMPLETE")
print("="*50)
print("\nNext: reload orders into SQLite via db_loader.py")
print("Then run analysis.sql")