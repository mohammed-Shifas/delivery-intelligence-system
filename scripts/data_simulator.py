import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import csv

np.random.seed(42)
random.seed(42)

NUM_VENDORS   = 168
NUM_RIDERS    = 5500
NUM_DAYS      = 90
START_DATE    = datetime(2026, 1, 1)
WEEKDAY_MIN   = 40000
WEEKDAY_MAX   = 60000
WEEKEND_MIN   = 70000
WEEKEND_MAX   = 90000

ZONE_IDS = ["Z01","Z02","Z03","Z04","Z05","Z06","Z07","Z08","Z09","Z10","Z11","Z12"]
ZONE_INFO = {
    "Z01":{"name":"Downtown Dubai",  "pop":28000,"inc":"High"},
    "Z02":{"name":"Dubai Marina",    "pop":35000,"inc":"High"},
    "Z03":{"name":"Deira",           "pop":62000,"inc":"Low"},
    "Z04":{"name":"Bur Dubai",       "pop":55000,"inc":"Low"},
    "Z05":{"name":"Jumeirah",        "pop":32000,"inc":"High"},
    "Z06":{"name":"Business Bay",    "pop":25000,"inc":"High"},
    "Z07":{"name":"Al Barsha",       "pop":48000,"inc":"Mid"},
    "Z08":{"name":"Karama",          "pop":58000,"inc":"Low"},
    "Z09":{"name":"Mirdif",          "pop":42000,"inc":"Mid"},
    "Z10":{"name":"JLT",             "pop":30000,"inc":"High"},
    "Z11":{"name":"Silicon Oasis",   "pop":38000,"inc":"Mid"},
    "Z12":{"name":"Al Quoz",         "pop":45000,"inc":"Low"},
}
ZONE_SHARE = np.array([0.12,0.11,0.10,0.09,0.10,0.09,0.08,0.08,0.06,0.07,0.05,0.05])
HOURLY_W   = np.array([0.3,0.2,0.1,0.1,0.1,0.2,0.8,1.8,2.2,2.0,1.8,2.8,
                        3.5,3.8,2.8,1.8,2.0,2.8,3.8,4.0,3.5,3.0,2.0,0.8])
HOURLY_W   = HOURLY_W / HOURLY_W.sum()

SHIFT_HOURS = {
    "Morning":   set(range(6,16)),
    "Afternoon": set(range(13,23)),
    "Night":     set(range(21,24))|set(range(0,7)),
}

PREFIXES = ["Al Faris","Buraq","Emirates","Gulf","Desert","Al Noor","Falcon",
            "Al Majd","Pearl","Oasis","Al Safa","Crescent","Horizon","Al Amin",
            "Sunrise","Al Waha","Prime","Al Reem","Velocity","Metro",
            "Al Baraka","Swift","Apex","Zenith"]
SUFFIXES = ["Delivery","Logistics","Riders","Fleet","Express",
            "Operations","Transport","Services","Solutions","Dispatch"]

def make_names(n):
    names, idx = [], 0
    while len(names) < n:
        p = PREFIXES[idx % len(PREFIXES)]
        s = SUFFIXES[(idx // len(PREFIXES)) % len(SUFFIXES)]
        num = idx // (len(PREFIXES)*len(SUFFIXES)) + 1
        name = f"{p} {s}" if num==1 else f"{p} {s} {num}"
        if name not in names: names.append(name)
        idx += 1
    return names

print("="*55)
print("  DELIVERY INTELLIGENCE — SIMULATOR")
print("  Dubai | 5,500 Riders | 90 Days")
print("="*55)

# ── VENDORS ──────────────────────────────────────────
print("\n[1/5] Generating vendors...")
vnames  = make_names(NUM_VENDORS)
vendors = []
for i in range(NUM_VENDORS):
    pz = ZONE_IDS[i % 12]
    sz = random.choice([z for z in ZONE_IDS if z!=pz]) if random.random()<0.30 else ""
    vendors.append({
        "vendor_id":f"V{i+1:03d}","vendor_name":vnames[i],
        "primary_zone":pz,"secondary_zone":sz,
        "contract_start":(START_DATE-timedelta(days=random.randint(30,730))).strftime("%Y-%m-%d"),
        "vendor_status":random.choices(["Active","Probation","Suspended"],[0.82,0.13,0.05])[0],
        "performance_score":round(random.uniform(45,96),1),"rider_count":0,
    })
vendors_df = pd.DataFrame(vendors)
print(f"   ✓ {len(vendors_df)} vendors")

# ── RIDERS ───────────────────────────────────────────
print("\n[2/5] Generating riders...")
active_v  = vendors_df[vendors_df["vendor_status"]=="Active"]
tot_score = active_v["performance_score"].sum()
alloc = {}; assigned = 0
for _,v in active_v.iterrows():
    c = max(8, int(v["performance_score"]/tot_score*NUM_RIDERS))
    alloc[v["vendor_id"]] = c; assigned += c
fv = active_v.iloc[0]["vendor_id"]
alloc[fv] = alloc.get(fv,0)+(NUM_RIDERS-assigned)

riders = []
r_num  = 1
for vid,count in alloc.items():
    vrow = vendors_df[vendors_df["vendor_id"]==vid].iloc[0]
    pz, sz = vrow["primary_zone"], vrow["secondary_zone"]
    for _ in range(count):
        hz  = sz if (sz and random.random()<0.25) else pz
        xfr = random.random()<0.12
        cz  = random.choice([z for z in ZONE_IDS if z!=hz]) if xfr else hz
        sh  = random.choices(["Morning","Afternoon","Night"],[0.40,0.45,0.15])[0]
        riders.append({
            "rider_id":f"R{r_num:05d}","vendor_id":vid,"vendor_name":vrow["vendor_name"],
            "home_zone":hz,"current_zone":cz,"is_zone_transfer":xfr,
            "transfer_reason":random.choice(["Shortage cover","Peak support","Temp reassignment"]) if xfr else "",
            "shift":sh,"vehicle_type":random.choices(["Bike","Car","Walker"],[0.72,0.20,0.08])[0],
            "join_date":(START_DATE-timedelta(days=random.randint(1,900))).strftime("%Y-%m-%d"),
            "status":random.choices(["Active","Inactive","Suspended"],[0.87,0.09,0.04])[0],
            "avg_rating":round(random.uniform(3.2,5.0),1),
            "absence_rate":round(min(0.40,max(0.01,np.random.beta(2,10))),3),
            "completion_rate":round(min(0.99,max(0.65,np.random.beta(9,1.5))),3),
            "overtime_flag":random.random()<0.18,"experience_days":random.randint(1,900),
        })
        r_num += 1

riders_df = pd.DataFrame(riders)
for vid in alloc:
    vendors_df.loc[vendors_df["vendor_id"]==vid,"rider_count"] = len(riders_df[riders_df["vendor_id"]==vid])

sc = riders_df["shift"].value_counts()
print(f"   ✓ {len(riders_df):,} riders  "
      f"(Morning:{sc.get('Morning',0)} Afternoon:{sc.get('Afternoon',0)} Night:{sc.get('Night',0)})")
print(f"   ✓ Zone transfers:{riders_df['is_zone_transfer'].sum()}  Overtime:{riders_df['overtime_flag'].sum()}")

# ── AVAILABILITY POOLS ───────────────────────────────
print("\n[3/5] Building availability pools...")
active_r = riders_df[riders_df["status"]=="Active"]
pool     = {z:{h:[] for h in range(24)} for z in ZONE_IDS}
for _,r in active_r.iterrows():
    for h in SHIFT_HOURS[r["shift"]]:
        pool[r["current_zone"]][h].append((r["rider_id"],r["vendor_id"],float(r["absence_rate"])))

pool_np = {}
for z in ZONE_IDS:
    pool_np[z] = {}
    for h in range(24):
        e = pool[z][h]
        if e:
            pool_np[z][h] = (np.array([x[0] for x in e]),
                             np.array([x[1] for x in e]),
                             np.array([x[2] for x in e]))
        else:
            pool_np[z][h] = None
print("   ✓ Pools ready")

# ── RESTAURANT IDS PER ZONE ──────────────────────────
REST_PER_ZONE = 50
SLOW_N        = int(REST_PER_ZONE * 0.15)
rest_ids   = {z: [f"{z}_R{i:03d}" for i in range(1,REST_PER_ZONE+1)] for z in ZONE_IDS}
slow_set   = {z: set(rest_ids[z][:SLOW_N]) for z in ZONE_IDS}

# ── ORDERS — WRITE IN DAILY CHUNKS ───────────────────
print("\n[4/5] Generating orders (writing in daily chunks)...")

ORDER_COLS = ["order_id","date","restaurant_id","zone_id","rider_id","vendor_id",
              "order_time","pickup_time","delivery_time",
              "prep_minutes","delivery_minutes","total_minutes",
              "status","order_value","load_factor","available_riders",
              "hour_of_day","is_weekend","is_slow_restaurant","is_traffic_delay"]
EVENT_COLS = ["event_id","order_id","event_type","delay_minutes",
              "zone_id","restaurant_id","rider_id","vendor_id","event_time"]

orders_path = "data/orders.csv"
events_path = "data/delivery_events.csv"

# Write headers
with open(orders_path,"w",newline="") as f: csv.writer(f).writerow(ORDER_COLS)
with open(events_path,"w",newline="") as f: csv.writer(f).writerow(EVENT_COLS)

total_orders = 0
total_events = 0
ord_num      = 1
evt_num      = 1

for day_num in range(NUM_DAYS):
    cur_date  = START_DATE + timedelta(days=day_num)
    is_wkend  = cur_date.weekday() >= 4
    day_str   = cur_date.strftime("%Y-%m-%d")

    if is_wkend:
        day_total = int(np.random.randint(WEEKEND_MIN, WEEKEND_MAX+1))
    else:
        day_total = int(np.random.randint(WEEKDAY_MIN, WEEKDAY_MAX+1))

    hourly = np.random.multinomial(day_total, HOURLY_W)

    day_orders = []
    day_events = []

    for hour in range(24):
        h_total = int(hourly[hour])
        if h_total == 0:
            continue

        zone_counts = np.random.multinomial(h_total, ZONE_SHARE)

        for zi, z_count in enumerate(zone_counts):
            if z_count == 0:
                continue
            zone = ZONE_IDS[zi]
            n    = int(z_count)

            p = pool_np[zone][hour]
            if p is not None:
                rids_a, vids_a, ab_a = p
                present = np.random.random(len(rids_a)) > ab_a
                avail_r = rids_a[present]
                avail_v = vids_a[present]
            else:
                avail_r = np.array([])
                avail_v = np.array([])

            n_avail     = max(1, len(avail_r))
            load_factor = round(n / n_avail, 3)

            rests      = np.random.choice(rest_ids[zone], n)
            is_slow    = np.array([r in slow_set[zone] for r in rests])
            prep       = np.where(is_slow, np.random.randint(28,56,n), np.random.randint(7,23,n))
            base_del   = np.random.randint(12,31,n)
            is_traf    = np.random.random(n) < 0.18
            traf_ext   = np.where(is_traf, np.random.randint(8,23,n), 0)
            delv       = base_del + traf_ext
            total      = prep + delv
            mins_off   = np.random.randint(0,60,n)
            vals       = np.round(np.random.uniform(10,120,n),2)

            if len(avail_r) > 0:
                idx_c  = np.random.randint(0, len(avail_r), n)
                r_ids  = avail_r[idx_c]
                v_ids  = avail_v[idx_c]
            else:
                r_ids  = np.full(n,"UNASSIGNED")
                v_ids  = np.full(n,"UNASSIGNED")

            if load_factor > 2.0:    p_s=[0.50,0.38,0.12]
            elif load_factor > 1.5:  p_s=[0.65,0.28,0.07]
            elif load_factor > 1.0:  p_s=[0.80,0.17,0.03]
            else:                     p_s=[0.91,0.08,0.01]

            statuses = np.random.choice(["Delivered","Late","Cancelled"],n,p=p_s)
            if len(avail_r)==0:
                statuses = np.random.choice(["Cancelled","Late"],n,p=[0.6,0.4])

            for j in range(n):
                ot  = cur_date + timedelta(hours=hour, minutes=int(mins_off[j]))
                pt  = ot + timedelta(minutes=int(prep[j]))
                dt  = pt + timedelta(minutes=int(delv[j]))
                oid = f"ORD{ord_num:07d}"
                ord_num += 1

                day_orders.append([
                    oid, day_str, rests[j], zone, r_ids[j], v_ids[j],
                    ot.strftime("%Y-%m-%d %H:%M:%S"),
                    pt.strftime("%Y-%m-%d %H:%M:%S"),
                    dt.strftime("%Y-%m-%d %H:%M:%S"),
                    int(prep[j]), int(delv[j]), int(total[j]),
                    statuses[j], float(vals[j]),
                    load_factor, n_avail, hour, is_wkend,
                    bool(is_slow[j]), bool(is_traf[j])
                ])

                if statuses[j]=="Late":
                    if is_slow[j] and prep[j]>25:
                        day_events.append([f"EVT{evt_num:07d}",oid,"PrepDelay",
                            int(prep[j])-20,zone,rests[j],r_ids[j],v_ids[j],
                            ot.strftime("%Y-%m-%d %H:%M:%S")])
                        evt_num+=1
                    if is_traf[j]:
                        day_events.append([f"EVT{evt_num:07d}",oid,"TrafficDelay",
                            int(traf_ext[j]),zone,rests[j],r_ids[j],v_ids[j],
                            pt.strftime("%Y-%m-%d %H:%M:%S")])
                        evt_num+=1
                    if r_ids[j]=="UNASSIGNED":
                        day_events.append([f"EVT{evt_num:07d}",oid,"RiderShortage",
                            random.randint(15,45),zone,rests[j],"UNASSIGNED","UNASSIGNED",
                            ot.strftime("%Y-%m-%d %H:%M:%S")])
                        evt_num+=1

    # Write this day's data to CSV
    with open(orders_path,"a",newline="") as f:
        w = csv.writer(f)
        w.writerows(day_orders)
    with open(events_path,"a",newline="") as f:
        w = csv.writer(f)
        w.writerows(day_events)

    total_orders += len(day_orders)
    total_events += len(day_events)

    if (day_num+1) % 10 == 0:
        print(f"   ... Day {day_num+1}/{NUM_DAYS} — {total_orders:,} orders  {total_events:,} events")

print(f"   ✓ {total_orders:,} orders saved")
print(f"   ✓ {total_events:,} events saved")

# ── SAVE REMAINING FILES ─────────────────────────────
print("\n[5/5] Saving vendors, riders, zones...")
vendors_df.to_csv("data/vendors.csv",index=False)
riders_df.to_csv("data/riders.csv",index=False)
zones_df = pd.DataFrame([
    {"zone_id":z,"zone_name":ZONE_INFO[z]["name"],
     "population":ZONE_INFO[z]["pop"],"avg_income":ZONE_INFO[z]["inc"]}
    for z in ZONE_IDS
])
zones_df.to_csv("data/zones.csv",index=False)
print(f"   ✓ vendors.csv  {len(vendors_df):,} rows")
print(f"   ✓ riders.csv   {len(riders_df):,} rows")
print(f"   ✓ zones.csv    {len(zones_df):,} rows")

# ── QUICK SUMMARY FROM FILE ──────────────────────────
print("\n" + "="*55)
print("   SIMULATION COMPLETE")
print("="*55)
print(f"\n  Total orders:    {total_orders:,}")
print(f"  Total events:    {total_events:,}")
print(f"  Avg/day:         {total_orders//NUM_DAYS:,}")
print(f"\n  Riders:          {len(riders_df):,}")
print(f"  Zone transfers:  {riders_df['is_zone_transfer'].sum()}")
print(f"  Overtime risk:   {riders_df['overtime_flag'].sum()}")
print(f"  Avg absence:     {riders_df['absence_rate'].mean()*100:.1f}%")
print("\n  Files ready in data/ folder")
print("  Next step: run db_loader.py")
print("="*55)