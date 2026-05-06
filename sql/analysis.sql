-- ════════════════════════════════════════════════════════════
-- LAST-MILE DELIVERY INTELLIGENCE — MASTER ANALYSIS QUERY
-- Dubai Operations | 90-Day Period | 5,500 Riders | 168 Vendors
-- Author: Mohammed Shifas
--
-- Business Questions Answered:
-- Q1: Where and when is demand highest? (weekday vs weekend)
-- Q2: Where are riders short during peak hours?
-- Q3: Which zones need rider reallocation and when?
-- Q4: Which restaurants are causing delivery delays?
-- Q5: Which vendors are underperforming?
-- Q6: What is the priority action recommendation?
-- ════════════════════════════════════════════════════════════

WITH

-- ────────────────────────────────────────────────────────────
-- CTE 1: DEMAND HEATMAP
-- Split by weekday vs weekend to show real demand patterns
-- ────────────────────────────────────────────────────────────
demand AS (
    SELECT
        o.zone_id,
        z.zone_name,
        z.avg_income,
        o.hour_of_day,
        o.is_weekend,
        COUNT(o.order_id)                              AS total_orders_90d,
        ROUND(COUNT(o.order_id) * 1.0 /
              COUNT(DISTINCT o.date), 1)               AS avg_daily_orders,
        ROUND(SUM(o.order_value) /
              COUNT(DISTINCT o.date), 2)               AS avg_daily_revenue,
        ROUND(AVG(o.order_value), 2)                   AS avg_order_value,
        ROUND(SUM(CASE WHEN o.status = 'Late'
              THEN 1.0 ELSE 0 END) /
              COUNT(o.order_id) * 100, 1)              AS late_pct,
        ROUND(SUM(CASE WHEN o.status = 'Cancelled'
              THEN 1.0 ELSE 0 END) /
              COUNT(o.order_id) * 100, 1)              AS cancelled_pct,
        CASE
            WHEN o.hour_of_day BETWEEN 12 AND 14 THEN 'LUNCH PEAK'
            WHEN o.hour_of_day BETWEEN 18 AND 21 THEN 'DINNER PEAK'
            WHEN o.hour_of_day BETWEEN 7  AND 11 THEN 'MORNING'
            WHEN o.hour_of_day BETWEEN 15 AND 17 THEN 'AFTERNOON'
            ELSE 'OFF PEAK'
        END                                            AS time_slot
    FROM orders o
    JOIN zones z ON o.zone_id = z.zone_id
    GROUP BY o.zone_id, z.zone_name, z.avg_income,
             o.hour_of_day, o.is_weekend
),

-- ────────────────────────────────────────────────────────────
-- CTE 2: RIDER COVERAGE
-- Riders on shift per zone per hour using real shift windows
-- ────────────────────────────────────────────────────────────
rider_coverage AS (
    SELECT
        r.current_zone                                 AS zone_id,
        hours.hour_of_day,
        COUNT(r.rider_id)                              AS riders_on_shift,
        ROUND(COUNT(r.rider_id) * 0.835, 0)            AS riders_available,
        ROUND(COUNT(r.rider_id) * 0.835 * 0.79, 1)    AS hourly_order_capacity,
        SUM(CASE WHEN r.overtime_flag = 1
            THEN 1 ELSE 0 END)                         AS overtime_riders,
        SUM(CASE WHEN r.is_zone_transfer = 1
            THEN 1 ELSE 0 END)                         AS transferred_riders,
        ROUND(AVG(r.absence_rate) * 100, 1)            AS avg_absence_pct
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
    GROUP BY r.current_zone, hours.hour_of_day
),
-- ────────────────────────────────────────────────────────────
-- CTE 3: GAP ANALYSIS
-- Demand vs coverage — finds shortages and surpluses
-- ────────────────────────────────────────────────────────────
gap_analysis AS (
    SELECT
        d.zone_id,
        d.zone_name,
        d.avg_income,
        d.hour_of_day,
        d.is_weekend,
        d.time_slot,
        d.avg_daily_orders,
        d.avg_daily_revenue,
        d.late_pct,
        d.cancelled_pct,
        COALESCE(rc.riders_available, 0)               AS riders_available,
        COALESCE(rc.hourly_order_capacity, 0)          AS hourly_capacity,
        COALESCE(rc.overtime_riders, 0)                AS overtime_riders,
        COALESCE(rc.transferred_riders, 0)             AS transferred_riders,
        COALESCE(rc.avg_absence_pct, 0)                AS avg_absence_pct,
        ROUND(
            d.avg_daily_orders /
            MAX(1.0, COALESCE(rc.hourly_order_capacity, 1))
        , 2)                                           AS load_factor,
        ROUND(
            d.avg_daily_orders -
            COALESCE(rc.hourly_order_capacity, 0)
        , 1)                                           AS order_gap,
        CASE
            WHEN d.avg_daily_orders -
                 COALESCE(rc.hourly_order_capacity,0) > 0
                 THEN 'SHORTAGE'
            WHEN d.avg_daily_orders -
                 COALESCE(rc.hourly_order_capacity,0) < 0
                 THEN 'SURPLUS — riders available to redeploy'
            ELSE 'BALANCED'
        END                                            AS gap_type,
        CASE
            WHEN d.avg_daily_orders /
                 MAX(1.0,COALESCE(rc.hourly_order_capacity,1)) >= 2.0
                 THEN 'CRITICAL — Immediate reallocation needed'
            WHEN d.avg_daily_orders /
                 MAX(1.0,COALESCE(rc.hourly_order_capacity,1)) >= 1.5
                 THEN 'HIGH — Additional riders required'
            WHEN d.avg_daily_orders /
                 MAX(1.0,COALESCE(rc.hourly_order_capacity,1)) >= 1.0
                 THEN 'NORMAL — Monitor closely'
            WHEN d.avg_daily_orders /
                 MAX(1.0,COALESCE(rc.hourly_order_capacity,1)) < 0.5
                 THEN 'SURPLUS — Riders can be redeployed'
            ELSE 'BALANCED'
        END                                            AS zone_status
    FROM demand d
    LEFT JOIN rider_coverage rc
           ON d.zone_id      = rc.zone_id
          AND d.hour_of_day  = rc.hour_of_day
),

-- ────────────────────────────────────────────────────────────
-- CTE 4: SLOW RESTAURANTS
-- Real restaurant names from OSM data
-- ────────────────────────────────────────────────────────────
slow_restaurants AS (
    SELECT
        o.restaurant_id,
        COALESCE(r.name, 'Unknown')                    AS restaurant_name,
        COALESCE(r.cuisine, 'Unknown')                 AS cuisine,
        o.zone_id,
        z.zone_name,
        r.platform_rating,
        COUNT(o.order_id)                              AS total_orders,
        ROUND(AVG(o.prep_minutes), 1)                  AS avg_prep_minutes,
        ROUND(AVG(o.delivery_minutes), 1)              AS avg_delivery_minutes,
        ROUND(AVG(o.total_minutes), 1)                 AS avg_total_minutes,
        ROUND(SUM(CASE WHEN o.status = 'Late'
              THEN 1.0 ELSE 0 END) /
              COUNT(o.order_id) * 100, 1)              AS late_pct,
        ROUND(MAX(o.prep_minutes), 0)                  AS max_prep_minutes,
        CASE
            WHEN AVG(o.prep_minutes) >= 35
                 THEN 'CRITICAL — Immediate review needed'
            WHEN AVG(o.prep_minutes) >= 28
                 THEN 'HIGH — Pickup window adjustment recommended'
            WHEN AVG(o.prep_minutes) >= 22
                 THEN 'MONITOR — Above platform average'
            ELSE 'NORMAL'
        END                                            AS restaurant_status
    FROM orders o
    LEFT JOIN restaurants r ON o.restaurant_id = r.restaurant_id
    JOIN zones z             ON o.zone_id       = z.zone_id
    GROUP BY o.restaurant_id, r.name, r.cuisine,
             o.zone_id, z.zone_name, r.platform_rating
    HAVING COUNT(o.order_id) >= 100
),
-- ────────────────────────────────────────────────────────────
-- CTE 5A: VENDOR PERFORMANCE (raw calculation)
-- ────────────────────────────────────────────────────────────
vendor_raw AS (
    SELECT
        r.vendor_id,
        r.vendor_name,
        v.primary_zone,
        z.zone_name,
        v.vendor_status,
        COUNT(r.rider_id)                              AS total_riders,
        SUM(CASE WHEN r.status='Active'
            THEN 1 ELSE 0 END)                         AS active_riders,
        SUM(CASE WHEN r.status='Inactive'
            THEN 1 ELSE 0 END)                         AS inactive_riders,
        SUM(CASE WHEN r.status='Suspended'
            THEN 1 ELSE 0 END)                         AS suspended_riders,
        ROUND(AVG(r.absence_rate) * 100, 1)            AS avg_absence_pct,
        ROUND(AVG(r.completion_rate) * 100, 1)         AS avg_completion_pct,
        ROUND(AVG(r.avg_rating), 2)                    AS avg_rider_rating,
        SUM(CASE WHEN r.overtime_flag=1
            THEN 1 ELSE 0 END)                         AS overtime_riders,
        ROUND(SUM(CASE WHEN r.overtime_flag=1
              THEN 1.0 ELSE 0 END) /
              COUNT(r.rider_id) * 100, 1)              AS overtime_pct,
        SUM(CASE WHEN r.is_zone_transfer=1
            THEN 1 ELSE 0 END)                         AS transferred_riders,
        ROUND(AVG(r.experience_days), 0)               AS avg_experience_days,
        ROUND(AVG(r.overtime_minutes), 0)              AS avg_overtime_minutes,
        ROUND(
            (AVG(r.completion_rate) * 40) +
            ((1.0 - AVG(r.absence_rate)) * 30) +
            ((AVG(r.avg_rating) - 1.0) / 4.0 * 20) +
            (CASE WHEN v.vendor_status = 'Active'
                  THEN 10.0 ELSE 0.0 END)
        , 1)                                           AS vendor_score
    FROM riders r
    JOIN vendors v ON r.vendor_id    = v.vendor_id
    JOIN zones z   ON v.primary_zone = z.zone_id
    GROUP BY r.vendor_id, r.vendor_name, v.primary_zone,
             z.zone_name, v.vendor_status
),

-- ────────────────────────────────────────────────────────────
-- CTE 5B: VENDOR PERFORMANCE (with tier)
-- ────────────────────────────────────────────────────────────
vendor_performance AS (
    SELECT
        *,
        CASE
            WHEN vendor_score >= 80 THEN 'TOP PERFORMER'
            WHEN vendor_score >= 60 THEN 'PERFORMING'
            WHEN vendor_score >= 40 THEN 'NEEDS IMPROVEMENT'
            ELSE 'CRITICAL'
        END                                            AS vendor_tier
    FROM vendor_raw
),

-- ────────────────────────────────────────────────────────────
-- CTE 6: SHIFT RECOMMENDATION
-- ────────────────────────────────────────────────────────────
shift_recommendation AS (
    SELECT
        g.zone_id,
        g.zone_name,
        g.avg_income,
        g.hour_of_day,
        g.time_slot,
        g.is_weekend,
        g.avg_daily_orders,
        g.riders_available,
        g.hourly_capacity,
        g.load_factor,
        g.order_gap,
        g.gap_type,
        g.zone_status,
        g.late_pct,
        g.overtime_riders,
        g.transferred_riders,
        g.avg_absence_pct,
        ROUND(
            MAX(0.0, g.avg_daily_orders - g.hourly_capacity) / 0.79
        , 0)                                           AS additional_riders_needed,
        CASE
            WHEN g.load_factor >= 2.0
                 THEN 'Add 3 overlapping shifts — stagger starts at this hour'
            WHEN g.load_factor >= 1.5
                 THEN 'Add 1 overlapping shift — blend with adjacent shift'
            WHEN g.load_factor >= 1.0
                 THEN 'Monitor — acceptable but watch late rate'
            WHEN g.order_gap < -50
                 THEN 'Redeploy excess riders to shortage zones'
            ELSE 'Current coverage adequate'
        END                                            AS shift_recommendation,
        ROUND(
            g.load_factor *
            CASE g.time_slot
                WHEN 'DINNER PEAK' THEN 3.0
                WHEN 'LUNCH PEAK'  THEN 2.5
                WHEN 'AFTERNOON'   THEN 1.5
                WHEN 'MORNING'     THEN 1.2
                ELSE 1.0
            END
        , 2)                                           AS action_priority_score
    FROM gap_analysis g
    WHERE g.time_slot IN ('LUNCH PEAK','DINNER PEAK','AFTERNOON')
)

-- ════════════════════════════════════════════════════════════
-- FINAL OUTPUT
-- Change FROM clause to query different CTEs:
--   FROM demand                → Q1 demand heatmap
--   FROM rider_coverage        → Q2 rider coverage by zone/hour
--   FROM gap_analysis          → Q3 full gap analysis
--   FROM slow_restaurants      → Q4 restaurant delay analysis
--   FROM vendor_performance    → Q5 vendor scorecard
--   FROM shift_recommendation  → Q6 priority recommendations
-- ════════════════════════════════════════════════════════════
SELECT *
FROM shift_recommendation
ORDER BY action_priority_score DESC
LIMIT 200;