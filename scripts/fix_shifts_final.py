import pandas as pd
import numpy as np

np.random.seed(42)

print("Loading riders...")
df = pd.read_csv("data/riders.csv")

# Real shift start hours based on actual Noon data
# Spread from 08:00 to 19:00
# More riders start in afternoon to cover dinner peak
SHIFT_STARTS = [8,9,10,11,12,13,14,15,16,17,18,19]
SHIFT_WEIGHTS = [0.10,0.08,0.07,0.08,0.12,0.10,0.08,0.12,0.10,0.08,0.05,0.02]

def assign_shift_start(zone):
    # Business Bay and office zones — more morning starts
    if zone in ["Z06", "Z10"]:
        weights = [0.15,0.12,0.10,0.10,0.10,0.08,0.08,0.10,0.08,0.05,0.03,0.01]
    # Residential high demand zones — more evening starts
    elif zone in ["Z01", "Z02", "Z05"]:
        weights = [0.06,0.06,0.06,0.07,0.10,0.10,0.08,0.12,0.12,0.10,0.08,0.05]
    else:
        weights = SHIFT_WEIGHTS
    return np.random.choice(SHIFT_STARTS, p=weights)

print("Assigning real shift windows...")
df["shift_start"] = df["current_zone"].apply(assign_shift_start)
df["shift_end"]   = (df["shift_start"] + 11) % 24
df["shift_label"] = df.apply(
    lambda r: f"{r['shift_start']:02d}00-{r['shift_end']:02d}00", axis=1
)

# Remove old shift column and replace
df = df.drop(columns=["shift"])
df = df.rename(columns={"shift_label": "shift"})

# Orders per hour capacity from real data
df["orders_per_hour_capacity"] = np.round(
    np.random.normal(0.79, 0.15, len(df)).clip(0.4, 1.67), 2
)

df.to_csv("data/riders.csv", index=False)

print(f"\n✓ {len(df):,} riders updated with real shift windows")
print("\nShift start distribution:")
for hour, count in df["shift_start"].value_counts().sort_index().items():
    bar = "█" * (count // 50)
    print(f"  {hour:02d}:00  {count:>5}  {bar}")

print(f"\nSample shifts:")
print(df[["rider_id","current_zone","shift","shift_start","shift_end"]].head(10).to_string())
print("\n✓ riders.csv updated")
print("Next: run db_loader.py")