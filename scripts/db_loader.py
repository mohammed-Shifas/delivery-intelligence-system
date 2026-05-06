import pandas as pd
import sqlite3
import os

DB_PATH = "database/deliveries.db"

print("="*50)
print("  DATABASE LOADER")
print("="*50)

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

cur.executescript("""
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous  = NORMAL;
    PRAGMA cache_size   = -64000;
    PRAGMA temp_store   = MEMORY;
""")

print("\n[1/6] Creating tables...")
cur.executescript("""
DROP TABLE IF EXISTS delivery_events;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS riders;
DROP TABLE IF EXISTS vendors;
DROP TABLE IF EXISTS zones;

CREATE TABLE zones (
    zone_id        TEXT PRIMARY KEY,
    zone_name      TEXT,
    population     INTEGER,
    avg_income     TEXT
);

CREATE TABLE vendors (
    vendor_id          TEXT PRIMARY KEY,
    vendor_name        TEXT,
    primary_zone       TEXT,
    secondary_zone     TEXT,
    contract_start     TEXT,
    vendor_status      TEXT,
    performance_score  REAL,
    rider_count        INTEGER
);

CREATE TABLE riders (
    rider_id                 TEXT PRIMARY KEY,
    vendor_id                TEXT,
    vendor_name              TEXT,
    home_zone                TEXT,
    current_zone             TEXT,
    is_zone_transfer         INTEGER,
    transfer_reason          TEXT,
    shift                    TEXT,
    shift_start              INTEGER,
    shift_end                INTEGER,
    orders_per_hour_capacity REAL,
    vehicle_type             TEXT,
    join_date                TEXT,
    status                   TEXT,
    avg_rating               REAL,
    absence_rate             REAL,
    completion_rate          REAL,
    overtime_flag            INTEGER,
    overtime_minutes         INTEGER,
    overtime_reason          TEXT,
    experience_days          INTEGER
);

CREATE TABLE orders (
    order_id             TEXT PRIMARY KEY,
    date                 TEXT,
    restaurant_id        TEXT,
    zone_id              TEXT,
    rider_id             TEXT,
    vendor_id            TEXT,
    order_time           TEXT,
    pickup_time          TEXT,
    delivery_time        TEXT,
    prep_minutes         INTEGER,
    delivery_minutes     INTEGER,
    total_minutes        INTEGER,
    status               TEXT,
    order_value          REAL,
    load_factor          REAL,
    available_riders     INTEGER,
    hour_of_day          INTEGER,
    is_weekend           INTEGER,
    is_slow_restaurant   INTEGER,
    is_traffic_delay     INTEGER
);

CREATE TABLE delivery_events (
    event_id       TEXT PRIMARY KEY,
    order_id       TEXT,
    event_type     TEXT,
    delay_minutes  INTEGER,
    zone_id        TEXT,
    restaurant_id  TEXT,
    rider_id       TEXT,
    vendor_id      TEXT,
    event_time     TEXT
);
""")
print("   ✓ All tables created")

def load_table(name, path, conn):
    print(f"\n[Loading] {name}...")
    df = pd.read_csv(path)
    df.to_sql(name, conn, if_exists="append", index=False, chunksize=10000)
    count = pd.read_sql(f"SELECT COUNT(*) as c FROM {name}", conn).iloc[0]["c"]
    print(f"   ✓ {count:,} rows loaded into {name}")
    return count

print("\n[2/6] Loading zones...")
load_table("zones", "data/zones.csv", conn)

print("\n[3/6] Loading vendors...")
load_table("vendors", "data/vendors.csv", conn)

print("\n[4/6] Loading riders...")
load_table("riders", "data/riders.csv", conn)

print("\n[5/6] Loading orders (large file — takes a few minutes)...")
chunk_size = 50000
total = 0
for chunk in pd.read_csv("data/orders.csv", chunksize=chunk_size):
    chunk.to_sql("orders", conn, if_exists="append", index=False)
    total += len(chunk)
    if total % 500000 == 0:
        print(f"   ... {total:,} rows loaded")
conn.commit()
print(f"   ✓ {total:,} rows loaded into orders")

print("\n[6/6] Loading delivery events...")
load_table("delivery_events", "data/delivery_events.csv", conn)

print("\n[Indexes] Creating indexes...")
cur.executescript("""
    CREATE INDEX IF NOT EXISTS idx_orders_zone   ON orders(zone_id);
    CREATE INDEX IF NOT EXISTS idx_orders_date   ON orders(date);
    CREATE INDEX IF NOT EXISTS idx_orders_rider  ON orders(rider_id);
    CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
    CREATE INDEX IF NOT EXISTS idx_orders_hour   ON orders(hour_of_day);
    CREATE INDEX IF NOT EXISTS idx_orders_vendor ON orders(vendor_id);
    CREATE INDEX IF NOT EXISTS idx_riders_vendor ON riders(vendor_id);
    CREATE INDEX IF NOT EXISTS idx_riders_zone   ON riders(current_zone);
    CREATE INDEX IF NOT EXISTS idx_events_order  ON delivery_events(order_id);
    CREATE INDEX IF NOT EXISTS idx_events_zone   ON delivery_events(zone_id);
    CREATE INDEX IF NOT EXISTS idx_events_type   ON delivery_events(event_type);
""")
conn.commit()
print("   ✓ Indexes created")

print("\n" + "="*50)
print("  DATABASE SUMMARY")
print("="*50)
for t in ["zones","vendors","riders","orders","delivery_events"]:
    c = pd.read_sql(f"SELECT COUNT(*) as c FROM {t}", conn).iloc[0]["c"]
    print(f"  {t:<20} {c:>10,} rows")

size_mb = os.path.getsize(DB_PATH) / (1024*1024)
print(f"\n  Database size: {size_mb:.1f} MB")
print(f"  Location: {DB_PATH}")
print("\n  ✓ Database ready")
print("="*50)

conn.close()