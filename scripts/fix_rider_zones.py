import pandas as pd
import numpy as np

np.random.seed(42)

print("Loading riders...")
riders_df = pd.read_csv("data/riders.csv")
vendors_df = pd.read_csv("data/vendors.csv")

# Target rider count per zone based on order share
# More orders = more riders needed
ZONE_RIDER_TARGETS = {
    "Z01": 650,   # Downtown Dubai     12% orders — high density
    "Z02": 600,   # Dubai Marina       11% orders
    "Z03": 560,   # Deira              10% orders
    "Z04": 500,   # Bur Dubai           9% orders
    "Z05": 550,   # Jumeirah           10% orders
    "Z06": 490,   # Business Bay        9% orders
    "Z07": 440,   # Al Barsha           8% orders
    "Z08": 440,   # Karama              8% orders
    "Z09": 330,   # Mirdif              6% orders
    "Z10": 380,   # JLT                 7% orders
    "Z11": 280,   # Silicon Oasis       5% orders
    "Z12": 280,   # Al Quoz             5% orders
}
# Total = 5500

print("Redistributing riders across zones...")

# Get active riders only
active_riders = riders_df[riders_df["status"] == "Active"].copy()
inactive_riders = riders_df[riders_df["status"] != "Active"].copy()

# Reassign current_zone for active riders based on targets
new_zones = []
for zone, target in ZONE_RIDER_TARGETS.items():
    new_zones.extend([zone] * target)

# Shuffle and assign
np.random.shuffle(new_zones)
active_riders = active_riders.reset_index(drop=True)

# Trim or pad if needed
if len(new_zones) > len(active_riders):
    new_zones = new_zones[:len(active_riders)]
elif len(new_zones) < len(active_riders):
    extra = len(active_riders) - len(new_zones)
    zones_list = list(ZONE_RIDER_TARGETS.keys())
    new_zones.extend(np.random.choice(zones_list, extra).tolist())

active_riders["current_zone"] = new_zones

# Fix zone transfers to match new zone assignment
active_riders["is_zone_transfer"] = active_riders.apply(
    lambda r: r["current_zone"] != r["home_zone"], axis=1
)
active_riders["transfer_reason"] = active_riders.apply(
    lambda r: np.random.choice(
        ["Shortage cover", "Peak support", "Temp reassignment"]
    ) if r["is_zone_transfer"] else "", axis=1
)

# Reassign shifts proportionally per zone
# More morning+afternoon riders in high demand zones
SHIFT_WEIGHTS = {
    "Z01": [0.38, 0.47, 0.15],  # Downtown — heavy dinner coverage
    "Z02": [0.38, 0.47, 0.15],
    "Z03": [0.42, 0.43, 0.15],
    "Z04": [0.42, 0.43, 0.15],
    "Z05": [0.40, 0.45, 0.15],
    "Z06": [0.45, 0.42, 0.13],  # Business Bay — heavy lunch (office workers)
    "Z07": [0.40, 0.45, 0.15],
    "Z08": [0.40, 0.45, 0.15],
    "Z09": [0.40, 0.43, 0.17],
    "Z10": [0.43, 0.44, 0.13],
    "Z11": [0.40, 0.44, 0.16],
    "Z12": [0.40, 0.44, 0.16],
}

def assign_shift(zone):
    weights = SHIFT_WEIGHTS.get(zone, [0.40, 0.45, 0.15])
    return np.random.choice(["Morning", "Afternoon", "Night"], p=weights)

active_riders["shift"] = active_riders["current_zone"].apply(assign_shift)

# Combine back with inactive riders
riders_final = pd.concat([active_riders, inactive_riders], ignore_index=True)
riders_final.to_csv("data/riders.csv", index=False)

# Summary
print("\nRider distribution by zone (active riders):")
zone_counts = active_riders.groupby("current_zone").size().reset_index(name="rider_count")
for _, row in zone_counts.iterrows():
    target = ZONE_RIDER_TARGETS.get(row["current_zone"], 0)
    print(f"  {row['current_zone']}  {row['rider_count']:>4} riders  (target: {target})")

print(f"\nShift distribution:")
for shift, count in active_riders["shift"].value_counts().items():
    print(f"  {shift:<12} {count:>5} riders")

print(f"\nZone transfers: {active_riders['is_zone_transfer'].sum()}")
print("\n✓ riders.csv updated with balanced zone distribution")
print("Next: run fix_load_factor.py to recalculate load factors in orders")