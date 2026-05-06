import pandas as pd

# ── DEFINE LONDON DELIVERY ZONES ──────────────────────────
# Each zone has a centre point and rough boundaries
# We assign restaurants to zones based on their coordinates

zones = [
    {
        "zone_id": "Z01",
        "zone_name": "Central London",
        "lat_min": 51.505, "lat_max": 51.520,
        "lon_min": -0.145, "lon_max": -0.095,
        "population": 15000,
        "avg_income": "High"
    },
    {
        "zone_id": "Z02",
        "zone_name": "West End",
        "lat_min": 51.510, "lat_max": 51.525,
        "lon_min": -0.145, "lon_max": -0.115,
        "population": 22000,
        "avg_income": "High"
    },
    {
        "zone_id": "Z03",
        "zone_name": "South Bank",
        "lat_min": 51.495, "lat_max": 51.510,
        "lon_min": -0.130, "lon_max": -0.080,
        "population": 18000,
        "avg_income": "Mid"
    },
    {
        "zone_id": "Z04",
        "zone_name": "East London",
        "lat_min": 51.510, "lat_max": 51.530,
        "lon_min": -0.095, "lon_max": -0.040,
        "population": 35000,
        "avg_income": "Mid"
    },
    {
        "zone_id": "Z05",
        "zone_name": "North London",
        "lat_min": 51.525, "lat_max": 51.545,
        "lon_min": -0.130, "lon_max": -0.080,
        "population": 28000,
        "avg_income": "Mid"
    },
    {
        "zone_id": "Z06",
        "zone_name": "Canary Wharf",
        "lat_min": 51.495, "lat_max": 51.512,
        "lon_min": -0.040, "lon_max": -0.005,
        "population": 12000,
        "avg_income": "High"
    },
    {
        "zone_id": "Z07",
        "zone_name": "South West London",
        "lat_min": 51.460, "lat_max": 51.495,
        "lon_min": -0.175, "lon_max": -0.110,
        "population": 42000,
        "avg_income": "High"
    },
    {
        "zone_id": "Z08",
        "zone_name": "Hackney & Shoreditch",
        "lat_min": 51.525, "lat_max": 51.550,
        "lon_min": -0.080, "lon_max": -0.030,
        "population": 31000,
        "avg_income": "Mid"
    },
]

def assign_zone(lat, lon, zones):
    for zone in zones:
        if (zone["lat_min"] <= lat <= zone["lat_max"] and
                zone["lon_min"] <= lon <= zone["lon_max"]):
            return zone["zone_id"], zone["zone_name"]
    return "Z00", "Other"

# ── LOAD RESTAURANT DATA ───────────────────────────────────
print("Loading restaurant data...")
df = pd.read_csv('data/restaurants_raw.csv')
print(f"Loaded {len(df)} restaurants")

# ── ASSIGN ZONES ───────────────────────────────────────────
print("Assigning zones...")
df[['zone_id', 'zone_name']] = df.apply(
    lambda row: pd.Series(assign_zone(row['latitude'], row['longitude'], zones)),
    axis=1
)

# ── ZONE SUMMARY ───────────────────────────────────────────
print("\nRestaurants per zone:")
zone_summary = df.groupby(['zone_id', 'zone_name']).size().reset_index(name='restaurant_count')
print(zone_summary.to_string())

# ── SAVE ZONE DEFINITIONS ──────────────────────────────────
zones_df = pd.DataFrame(zones)
zones_df.to_csv('data/zones.csv', index=False)
print(f"\nSaved zone definitions to data/zones.csv")

# ── SAVE UPDATED RESTAURANTS ───────────────────────────────
df.to_csv('data/restaurants_zoned.csv', index=False)
print(f"Saved {len(df)} zoned restaurants to data/restaurants_zoned.csv")

# ── QUICK STATS ────────────────────────────────────────────
mapped = len(df[df['zone_id'] != 'Z00'])
unmapped = len(df[df['zone_id'] == 'Z00'])
print(f"\nMapped to zones: {mapped}")
print(f"Outside zones: {unmapped}")