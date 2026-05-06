# Last-Mile Delivery Intelligence System

A complete data pipeline that analyses food delivery operations across Dubai — 
from data collection through SQL analysis to Power BI dashboard.

## What This System Does

Identifies operational gaps in last-mile delivery — where rider shortages occur, 
when demand exceeds capacity, which restaurants cause delays, and which vendors 
are underperforming. Produces actionable shift redesign recommendations backed by data.

## The Pipeline

OpenStreetMap API → Python Simulator → SQLite Database → SQL Analysis → Excel Export → Power BI Dashboard

## Dataset

- **5.69 million** simulated orders across 90 days
- **5,500** delivery riders across 12 Dubai zones
- **168** vendors, **~500** restaurants
- Realistic demand patterns: peak hours, zone-level capacity gaps, vendor SLA variation

## Tech Stack

| Layer | Tool |
|---|---|
| Data Collection | Python, OpenStreetMap API |
| Data Simulation | Python (custom simulator) |
| Database | SQLite (6 tables, 11 indexes) |
| Analysis | SQL (CTEs, window functions) |
| Export | Python → Excel (6 tabs) |
| Dashboard | Power BI (3 pages) |

## Key Business Questions Answered

1. Which zones have the highest rider shortage relative to demand?
2. What time windows cause the most delivery delays?
3. Which vendors consistently underperform on SLA?
4. How does rider absence rate vary by zone and shift?
5. Which restaurants generate disproportionate delay?
6. Where should shift capacity be reallocated?

## Repository Structure

delivery-intelligence-system/
├── scripts/          # Python scripts — data collection, simulation, export
├── sql/              # SQL analysis queries
└── README.md

## Background

Built to demonstrate end-to-end data analytics capability for last-mile delivery operations —
combining domain knowledge from managing 1,500+ riders and 30+ vendors at Noon Food UAE
with a complete technical pipeline from raw data to dashboard.
