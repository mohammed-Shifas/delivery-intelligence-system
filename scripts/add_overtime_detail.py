import pandas as pd
import numpy as np

np.random.seed(99)

print("Loading riders.csv...")
df = pd.read_csv("data/riders.csv")

# Only riders with overtime_flag = True get overtime minutes
# Everyone else gets 0

def assign_ot_minutes(flag):
    if not flag:
        return 0, ""
    
    # Distribution of overtime duration
    category = np.random.choice(
        ["micro", "short", "standard", "long", "extended"],
        p=[0.35, 0.30, 0.20, 0.12, 0.03]
    )
    
    ranges = {
        "micro":    (10, 40),
        "short":    (41, 90),
        "standard": (91, 180),
        "long":     (181, 300),
        "extended": (301, 480),
    }
    
    lo, hi = ranges[category]
    minutes = int(np.random.randint(lo, hi + 1))
    
    reason = np.random.choice(
        ["High demand", "Rider shortage", "Order in progress", "Zone coverage gap"],
        p=[0.40, 0.30, 0.20, 0.10]
    )
    
    return minutes, reason

print("Calculating overtime minutes...")
results = df["overtime_flag"].apply(lambda f: pd.Series(assign_ot_minutes(f)))
df["overtime_minutes"] = results[0].astype(int)
df["overtime_reason"]  = results[1].fillna("")

# Save back
df.to_csv("data/riders.csv", index=False)

# Summary
ot_riders = df[df["overtime_minutes"] > 0]
print(f"\n✓ Riders with overtime: {len(ot_riders)}")
print(f"\nOvertime duration breakdown:")
bins = [(0,0),(1,40),(41,90),(91,180),(181,300),(301,480)]
labels = ["No OT","Micro (10-40m)","Short (41-90m)","Standard (91-180m)","Long (181-300m)","Extended (301-480m)"]
for (lo,hi),label in zip(bins,labels):
    if lo == 0:
        count = len(df[df["overtime_minutes"]==0])
    else:
        count = len(df[(df["overtime_minutes"]>=lo)&(df["overtime_minutes"]<=hi)])
    pct = count/len(df)*100
    print(f"  {label:<25} {count:>5} riders  ({pct:.1f}%)")

print(f"\nOvertime reason breakdown:")
for reason, count in ot_riders["overtime_reason"].value_counts().items():
    print(f"  {reason:<25} {count:>5} riders")

print(f"\nAvg overtime minutes (OT riders only): {ot_riders['overtime_minutes'].mean():.0f} mins")
print(f"Max overtime:                          {ot_riders['overtime_minutes'].max()} mins")
print(f"\n✓ riders.csv updated with overtime_minutes and overtime_reason")