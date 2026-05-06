import pandas as pd
import sqlite3

DB_PATH = "database/deliveries.db"
OUTPUT  = "data/DeliveryIntelligence_Analysis.xlsx"

print("="*50)
print("  EXPORTING ANALYSIS TO EXCEL")
print("="*50)

conn = sqlite3.connect(DB_PATH)

# ── ALL 6 QUERIES ─────────────────────────────────────

Q1_DEMAND = """
SELECT
    o.zone_id, z.zone_name, z.avg_income,
    o.hour_of_day, o.is_weekend,
    ROUND(COUNT(o.order_id)*1.0/COUNT(DISTINCT o.date),1) AS avg_daily_orders,
    ROUND(SUM(o.order_value)/COUNT(DISTINCT o.date),2)    AS avg_daily_revenue,
    ROUND(AVG(o.order_value),2)                           AS avg_order_value,
    ROUND(SUM(CASE WHEN o.status='Late'
          THEN 1.0 ELSE 0 END)/COUNT(o.order_id)*100,1)  AS late_pct,
    ROUND(SUM(CASE WHEN o.status='Cancelled'
          THEN 1.0 ELSE 0 END)/COUNT(o.order_id)*100,1)  AS cancelled_pct,
    CASE
        WHEN o.hour_of_day BETWEEN 12 AND 14 THEN 'LUNCH PEAK'
        WHEN o.hour_of_day BETWEEN 18 AND 21 THEN 'DINNER PEAK'
        WHEN o.hour_of_day BETWEEN 7  AND 11 THEN 'MORNING'
        WHEN o.hour_of_day BETWEEN 15 AND 17 THEN 'AFTERNOON'
        ELSE 'OFF PEAK'
    END AS time_slot
FROM orders o
JOIN zones z ON o.zone_id = z.zone_id
GROUP BY o.zone_id, z.zone_name, z.avg_income,
         o.hour_of_day, o.is_weekend
ORDER BY avg_daily_orders DESC
"""

Q2_COVERAGE = """
SELECT
    r.current_zone AS zone_id,
    z.zone_name,
    hours.hour_of_day,
    COUNT(r.rider_id)                       AS riders_on_shift,
    ROUND(COUNT(r.rider_id)*0.835,0)        AS riders_available,
    ROUND(COUNT(r.rider_id)*0.835*0.79,1)   AS hourly_order_capacity,
    SUM(CASE WHEN r.overtime_flag=1
        THEN 1 ELSE 0 END)                  AS overtime_riders,
    SUM(CASE WHEN r.is_zone_transfer=1
        THEN 1 ELSE 0 END)                  AS transferred_riders,
    ROUND(AVG(r.absence_rate)*100,1)        AS avg_absence_pct
FROM riders r
JOIN zones z ON r.current_zone = z.zone_id
CROSS JOIN (
    SELECT 0 AS hour_of_day UNION SELECT 1  UNION SELECT 2
    UNION SELECT 3  UNION SELECT 4  UNION SELECT 5
    UNION SELECT 6  UNION SELECT 7  UNION SELECT 8
    UNION SELECT 9  UNION SELECT 10 UNION SELECT 11
    UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
    UNION SELECT 15 UNION SELECT 16 UNION SELECT 17
    UNION SELECT 18 UNION SELECT 19 UNION SELECT 20
    UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
) hours
WHERE r.status = 'Active'
AND (
    (r.shift_end > r.shift_start
     AND hours.hour_of_day >= r.shift_start
     AND hours.hour_of_day <  r.shift_end)
    OR
    (r.shift_end < r.shift_start
     AND (hours.hour_of_day >= r.shift_start
          OR  hours.hour_of_day <  r.shift_end))
)
GROUP BY r.current_zone, z.zone_name, hours.hour_of_day
ORDER BY riders_available DESC
"""

Q3_GAPS = """
SELECT
    d.zone_id, d.zone_name, d.avg_income,
    d.hour_of_day, d.is_weekend, d.time_slot,
    d.avg_daily_orders, d.late_pct, d.cancelled_pct,
    COALESCE(rc.riders_available,0)        AS riders_available,
    COALESCE(rc.hourly_order_capacity,0)   AS hourly_capacity,
    COALESCE(rc.overtime_riders,0)         AS overtime_riders,
    COALESCE(rc.transferred_riders,0)      AS transferred_riders,
    ROUND(d.avg_daily_orders/
          MAX(1.0,COALESCE(rc.hourly_order_capacity,1)),2) AS load_factor,
    ROUND(d.avg_daily_orders-
          COALESCE(rc.hourly_order_capacity,0),1)          AS order_gap,
    CASE
        WHEN d.avg_daily_orders-COALESCE(rc.hourly_order_capacity,0)>0
             THEN 'SHORTAGE'
        WHEN d.avg_daily_orders-COALESCE(rc.hourly_order_capacity,0)<0
             THEN 'SURPLUS'
        ELSE 'BALANCED'
    END AS gap_type,
    CASE
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=2.0
             THEN 'CRITICAL'
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=1.5
             THEN 'HIGH'
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=1.0
             THEN 'NORMAL'
        ELSE 'SURPLUS'
    END AS zone_status
FROM (
    SELECT o.zone_id, z.zone_name, z.avg_income,
           o.hour_of_day, o.is_weekend,
           ROUND(COUNT(o.order_id)*1.0/COUNT(DISTINCT o.date),1) AS avg_daily_orders,
           ROUND(SUM(CASE WHEN o.status='Late'
                 THEN 1.0 ELSE 0 END)/COUNT(o.order_id)*100,1)   AS late_pct,
           ROUND(SUM(CASE WHEN o.status='Cancelled'
                 THEN 1.0 ELSE 0 END)/COUNT(o.order_id)*100,1)   AS cancelled_pct,
           CASE
               WHEN o.hour_of_day BETWEEN 12 AND 14 THEN 'LUNCH PEAK'
               WHEN o.hour_of_day BETWEEN 18 AND 21 THEN 'DINNER PEAK'
               WHEN o.hour_of_day BETWEEN 7  AND 11 THEN 'MORNING'
               WHEN o.hour_of_day BETWEEN 15 AND 17 THEN 'AFTERNOON'
               ELSE 'OFF PEAK'
           END AS time_slot
    FROM orders o
    JOIN zones z ON o.zone_id = z.zone_id
    GROUP BY o.zone_id, z.zone_name, z.avg_income,
             o.hour_of_day, o.is_weekend
) d
LEFT JOIN (
    SELECT r.current_zone AS zone_id, hours.hour_of_day,
           ROUND(COUNT(r.rider_id)*0.835,0)      AS riders_available,
           ROUND(COUNT(r.rider_id)*0.835*0.79,1) AS hourly_order_capacity,
           SUM(CASE WHEN r.overtime_flag=1 THEN 1 ELSE 0 END) AS overtime_riders,
           SUM(CASE WHEN r.is_zone_transfer=1 THEN 1 ELSE 0 END) AS transferred_riders
    FROM riders r
    CROSS JOIN (
        SELECT 0 AS hour_of_day UNION SELECT 1  UNION SELECT 2
        UNION SELECT 3  UNION SELECT 4  UNION SELECT 5
        UNION SELECT 6  UNION SELECT 7  UNION SELECT 8
        UNION SELECT 9  UNION SELECT 10 UNION SELECT 11
        UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
        UNION SELECT 15 UNION SELECT 16 UNION SELECT 17
        UNION SELECT 18 UNION SELECT 19 UNION SELECT 20
        UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
    ) hours
    WHERE r.status='Active'
    AND (
        (r.shift_end>r.shift_start
         AND hours.hour_of_day>=r.shift_start
         AND hours.hour_of_day<r.shift_end)
        OR
        (r.shift_end<r.shift_start
         AND (hours.hour_of_day>=r.shift_start
              OR hours.hour_of_day<r.shift_end))
    )
    GROUP BY r.current_zone, hours.hour_of_day
) rc ON d.zone_id=rc.zone_id AND d.hour_of_day=rc.hour_of_day
ORDER BY load_factor DESC
"""

Q4_RESTAURANTS = """
SELECT
    o.restaurant_id,
    COALESCE(r.name,'Unknown')       AS restaurant_name,
    COALESCE(r.cuisine,'Unknown')    AS cuisine,
    o.zone_id, z.zone_name,
    r.platform_rating,
    COUNT(o.order_id)                AS total_orders,
    ROUND(AVG(o.prep_minutes),1)     AS avg_prep_minutes,
    ROUND(AVG(o.delivery_minutes),1) AS avg_delivery_minutes,
    ROUND(AVG(o.total_minutes),1)    AS avg_total_minutes,
    ROUND(SUM(CASE WHEN o.status='Late'
          THEN 1.0 ELSE 0 END)/
          COUNT(o.order_id)*100,1)   AS late_pct,
    ROUND(MAX(o.prep_minutes),0)     AS max_prep_minutes,
    CASE
        WHEN AVG(o.prep_minutes)>=35 THEN 'CRITICAL'
        WHEN AVG(o.prep_minutes)>=28 THEN 'HIGH'
        WHEN AVG(o.prep_minutes)>=22 THEN 'MONITOR'
        ELSE 'NORMAL'
    END AS restaurant_status
FROM orders o
LEFT JOIN restaurants r ON o.restaurant_id=r.restaurant_id
JOIN zones z ON o.zone_id=z.zone_id
GROUP BY o.restaurant_id, r.name, r.cuisine,
         o.zone_id, z.zone_name, r.platform_rating
HAVING COUNT(o.order_id)>=100
ORDER BY avg_prep_minutes DESC
"""

Q5_VENDORS = """
SELECT
    vr.vendor_id, vr.vendor_name,
    vr.primary_zone, vr.zone_name,
    vr.vendor_status,
    vr.total_riders, vr.active_riders,
    vr.inactive_riders, vr.suspended_riders,
    vr.avg_absence_pct, vr.avg_completion_pct,
    vr.avg_rider_rating, vr.overtime_riders,
    vr.overtime_pct, vr.transferred_riders,
    vr.avg_experience_days, vr.avg_overtime_minutes,
    vr.vendor_score,
    CASE
        WHEN vr.vendor_score>=80 THEN 'TOP PERFORMER'
        WHEN vr.vendor_score>=60 THEN 'PERFORMING'
        WHEN vr.vendor_score>=40 THEN 'NEEDS IMPROVEMENT'
        ELSE 'CRITICAL'
    END AS vendor_tier
FROM (
    SELECT
        r.vendor_id, r.vendor_name,
        v.primary_zone, z.zone_name,
        v.vendor_status,
        COUNT(r.rider_id)                        AS total_riders,
        SUM(CASE WHEN r.status='Active'
            THEN 1 ELSE 0 END)                   AS active_riders,
        SUM(CASE WHEN r.status='Inactive'
            THEN 1 ELSE 0 END)                   AS inactive_riders,
        SUM(CASE WHEN r.status='Suspended'
            THEN 1 ELSE 0 END)                   AS suspended_riders,
        ROUND(AVG(r.absence_rate)*100,1)         AS avg_absence_pct,
        ROUND(AVG(r.completion_rate)*100,1)      AS avg_completion_pct,
        ROUND(AVG(r.avg_rating),2)               AS avg_rider_rating,
        SUM(CASE WHEN r.overtime_flag=1
            THEN 1 ELSE 0 END)                   AS overtime_riders,
        ROUND(SUM(CASE WHEN r.overtime_flag=1
              THEN 1.0 ELSE 0 END)/
              COUNT(r.rider_id)*100,1)            AS overtime_pct,
        SUM(CASE WHEN r.is_zone_transfer=1
            THEN 1 ELSE 0 END)                   AS transferred_riders,
        ROUND(AVG(r.experience_days),0)          AS avg_experience_days,
        ROUND(AVG(r.overtime_minutes),0)         AS avg_overtime_minutes,
        ROUND(
            (AVG(r.completion_rate)*40)+
            ((1.0-AVG(r.absence_rate))*30)+
            ((AVG(r.avg_rating)-1.0)/4.0*20)+
            (CASE WHEN v.vendor_status='Active'
                  THEN 10.0 ELSE 0.0 END)
        ,1) AS vendor_score
    FROM riders r
    JOIN vendors v ON r.vendor_id=v.vendor_id
    JOIN zones z   ON v.primary_zone=z.zone_id
    GROUP BY r.vendor_id, r.vendor_name,
             v.primary_zone, z.zone_name, v.vendor_status
) vr
ORDER BY vendor_score DESC
"""

Q6_RECOMMENDATIONS = """
SELECT
    d.zone_id, d.zone_name, d.avg_income,
    d.hour_of_day, d.time_slot, d.is_weekend,
    d.avg_daily_orders, rc.riders_available,
    rc.hourly_order_capacity AS hourly_capacity,
    ROUND(d.avg_daily_orders/
          MAX(1.0,COALESCE(rc.hourly_order_capacity,1)),2) AS load_factor,
    ROUND(d.avg_daily_orders-
          COALESCE(rc.hourly_order_capacity,0),1)          AS order_gap,
    CASE
        WHEN d.avg_daily_orders-COALESCE(rc.hourly_order_capacity,0)>0
             THEN 'SHORTAGE'
        ELSE 'SURPLUS'
    END AS gap_type,
    CASE
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=2.0
             THEN 'CRITICAL'
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=1.5
             THEN 'HIGH'
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=1.0
             THEN 'NORMAL'
        ELSE 'SURPLUS'
    END AS zone_status,
    d.late_pct,
    COALESCE(rc.overtime_riders,0)    AS overtime_riders,
    COALESCE(rc.transferred_riders,0) AS transferred_riders,
    ROUND(MAX(0.0,d.avg_daily_orders-
          COALESCE(rc.hourly_order_capacity,0))/0.79,0) AS additional_riders_needed,
    CASE
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=2.0
             THEN 'Add 3 overlapping shifts'
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))>=1.5
             THEN 'Add 1 overlapping shift'
        WHEN d.avg_daily_orders/
             MAX(1.0,COALESCE(rc.hourly_order_capacity,1))<0.5
             THEN 'Redeploy excess riders'
        ELSE 'Current coverage adequate'
    END AS shift_recommendation,
    ROUND(d.avg_daily_orders/
          MAX(1.0,COALESCE(rc.hourly_order_capacity,1))*
          CASE d.time_slot
              WHEN 'DINNER PEAK' THEN 3.0
              WHEN 'LUNCH PEAK'  THEN 2.5
              WHEN 'AFTERNOON'   THEN 1.5
              ELSE 1.2
          END,2) AS action_priority_score
FROM (
    SELECT o.zone_id, z.zone_name, z.avg_income,
           o.hour_of_day, o.is_weekend,
           ROUND(COUNT(o.order_id)*1.0/COUNT(DISTINCT o.date),1) AS avg_daily_orders,
           ROUND(SUM(CASE WHEN o.status='Late'
                 THEN 1.0 ELSE 0 END)/COUNT(o.order_id)*100,1)   AS late_pct,
           CASE
               WHEN o.hour_of_day BETWEEN 12 AND 14 THEN 'LUNCH PEAK'
               WHEN o.hour_of_day BETWEEN 18 AND 21 THEN 'DINNER PEAK'
               WHEN o.hour_of_day BETWEEN 15 AND 17 THEN 'AFTERNOON'
               ELSE 'OTHER'
           END AS time_slot
    FROM orders o
    JOIN zones z ON o.zone_id=z.zone_id
    GROUP BY o.zone_id, z.zone_name, z.avg_income,
             o.hour_of_day, o.is_weekend
    HAVING time_slot != 'OTHER'
) d
LEFT JOIN (
    SELECT r.current_zone AS zone_id, hours.hour_of_day,
           ROUND(COUNT(r.rider_id)*0.835,0)      AS riders_available,
           ROUND(COUNT(r.rider_id)*0.835*0.79,1) AS hourly_order_capacity,
           SUM(CASE WHEN r.overtime_flag=1 THEN 1 ELSE 0 END) AS overtime_riders,
           SUM(CASE WHEN r.is_zone_transfer=1 THEN 1 ELSE 0 END) AS transferred_riders
    FROM riders r
    CROSS JOIN (
        SELECT 0 AS hour_of_day UNION SELECT 1  UNION SELECT 2
        UNION SELECT 3  UNION SELECT 4  UNION SELECT 5
        UNION SELECT 6  UNION SELECT 7  UNION SELECT 8
        UNION SELECT 9  UNION SELECT 10 UNION SELECT 11
        UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
        UNION SELECT 15 UNION SELECT 16 UNION SELECT 17
        UNION SELECT 18 UNION SELECT 19 UNION SELECT 20
        UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
    ) hours
    WHERE r.status='Active'
    AND (
        (r.shift_end>r.shift_start
         AND hours.hour_of_day>=r.shift_start
         AND hours.hour_of_day<r.shift_end)
        OR
        (r.shift_end<r.shift_start
         AND (hours.hour_of_day>=r.shift_start
              OR hours.hour_of_day<r.shift_end))
    )
    GROUP BY r.current_zone, hours.hour_of_day
) rc ON d.zone_id=rc.zone_id AND d.hour_of_day=rc.hour_of_day
ORDER BY action_priority_score DESC
"""

# ── RUN ALL QUERIES AND EXPORT TO EXCEL ───────────────
queries = {
    "Q1 - Demand Heatmap":         Q1_DEMAND,
    "Q2 - Rider Coverage":         Q2_COVERAGE,
    "Q3 - Gap Analysis":           Q3_GAPS,
    "Q4 - Slow Restaurants":       Q4_RESTAURANTS,
    "Q5 - Vendor Performance":     Q5_VENDORS,
    "Q6 - Recommendations":        Q6_RECOMMENDATIONS,
}

print("\nRunning queries and exporting to Excel...")

with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
    for sheet_name, query in queries.items():
        print(f"\n  Running {sheet_name}...")
        df = pd.read_sql_query(query, conn)
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"  ✓ {len(df):,} rows exported to tab '{sheet_name}'")

conn.close()

print(f"\n{'='*50}")
print(f"  EXPORT COMPLETE")
print(f"{'='*50}")
print(f"\n  File: {OUTPUT}")
print(f"  Tabs: {len(queries)}")
print(f"\n  Open in Excel then connect Power BI to this file")
print(f"{'='*50}")