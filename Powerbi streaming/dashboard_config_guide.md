# CardShield Real-Time Ops Dashboard — Power BI Build Guide

Prerequisites: the `Transactions` and `SystemHealth` streaming tables from
`powerbi_streaming_schema.json` created in the Power BI Service, and rows
flowing into them via `powerbi_bridge.py`.

> Streaming (push) datasets only support live tiles by default. To use these
> fields in slicers, gauges with targets, or DAX measures the way this guide
> describes, enable **"Historic data analysis"** when you create the
> streaming dataset — this backs it with a queryable store so Power BI
> Desktop can build a report against it, not just Q&A tiles.

## 0. Apply the theme

Power BI Desktop → **View** → **Themes** → **Browse for themes** →
select `cardshield_theme.json`. Do this before placing visuals so
container/border/background styling applies automatically.

## 1. Real-Time Transaction Stream (table)

1. Insert a **Table** visual, add columns in this order: `Time`,
   `Transaction_ID`, `Card_Mask`, `Amount`, `Country`, `Status`, `Score`,
   `Rule`.
2. Sort by `Time` descending (column header dropdown → Sort descending),
   and cap displayed rows via **Top N** filter on `Transaction_ID` (e.g. Top 50)
   so the table stays fast as data accumulates.
3. Select the visual → **Format your visual** → **Cell elements**:
   - Field: `Status`.
   - Turn on **Background color**, click **fx**, set format style to
     "Rules": `Status = "Blocked"` → `#FF1744` (with white/near-white text
     via the Font color rule using the same condition); `Status =
     "Approved"` → `#00E676`.
   - Repeat for the `Status` field's **Font color** with the same rules
     inverted-contrast (white text on red/green).

## 2. Fraud Rate Gauge

1. Insert a **Gauge** visual.
2. **Value**: a measure `Fraud Rate % = DIVIDE(CALCULATE(COUNTROWS(Transactions), Transactions[isFraud]=1), COUNTROWS(Transactions))`.
3. **Minimum**: 0, **Maximum**: 0.01 (i.e. 1%, since the gauge is scaled
   as a percentage — set the measure's format to Percentage first).
4. **Target value**: add a second measure `Fraud Rate Target = 0.007`
   (0.7%) so the needle has a visible target line.
5. Format → **Color** tab: set the gauge's fill using the theme's
   `minimum`/`center`/`maximum` colors (already wired to red/orange/green
   in the theme) so low fraud reads green and high reads red.

## 3. Fraud Over Time & Transaction Throughput (tps)

1. Insert a **Line chart**.
2. **X-axis**: `Time`, set **Type** to Continuous; in the visual's
   **General** → **Edit interactions**/**X-Axis** settings, or via a
   relative-time slicer, constrain the range to the last hour (a slicer on
   `Time` with **Relative date filtering**: "is in the last 1 hours" works
   well and updates automatically).
3. **Y-axis**: two measures — `Total Transactions = COUNTROWS(Transactions)`
   and `Fraud Transactions = CALCULATE(COUNTROWS(Transactions), Transactions[isFraud]=1)`
   — plotted as two series (green/red, matching theme `dataColors`).
4. For throughput (tps), add a second Line chart with a measure:
   `TPS = DIVIDE(COUNTROWS(Transactions), DATEDIFF(MIN(Transactions[Time]), MAX(Transactions[Time]), SECOND))`
   evaluated over a small rolling window — easiest is to bucket `Time`
   by second/minute in Power Query and count rows per bucket, then plot
   that count directly rather than a windowed DAX ratio.
5. Turn on **Page refresh** (Format page → Page refresh → toggle on,
   interval a few seconds) so both charts visibly scroll in near
   real time.

## 4. KPI Cards

Create these measures against the `Transactions` table:

```
Total Transactions   = COUNTROWS(Transactions)
Fraud Transactions    = CALCULATE(COUNTROWS(Transactions), Transactions[isFraud] = 1)
Fraud Rate            = DIVIDE([Fraud Transactions], [Total Transactions])
Average Fraud Score   = AVERAGE(Transactions[Score])
Loss Prevented (USD)  = CALCULATE(SUM(Transactions[Amount]), Transactions[Status] = "Blocked")
```

1. Insert a **Card** (new) visual per measure, or one **Multi-row card**
   with all five fields for a compact strip.
2. Format → **Callout value**: white text, per the theme.
3. To get the "vs last hour" delta shown in the reference mockup, add a
   second hidden measure comparing the current value to the same measure
   filtered to `Time` between 1–2 hours ago, then a third measure for the
   percentage delta; display it via the card's **Reference labels**
   (Desktop's newer Card visual) or a small multi-row card underneath.

## 5. Fraud by Country (map) and Top Fraud Rules (bar chart)

- **Map**: use a **Filled map** or **Azure Map** visual, **Location**:
  `Country`, **Color saturation**: `Fraud Transactions` measure. The
  theme's `map.dataPoint.fill` is pre-set to the alert red for a
  monochrome heat look.
- **Top Fraud Rules Triggered**: a **Bar chart**, **Axis**: `Rule`,
  **Value**: `Fraud Transactions` (filtered to exclude `Rule = "None"`),
  sorted descending. Add a **% of total** column via a measure:
  `Rule Share % = DIVIDE([Fraud Transactions], CALCULATE([Fraud Transactions], ALL(Transactions[Rule])))`.

## 6. System Health strip

1. Bind a small set of icon-style cards or a **Table** to the
   `SystemHealth` table, one row per `System_Component`, showing
   `Component_Status`.
2. Conditional formatting on `Component_Status` (same technique as
   step 1): `Healthy` → green check styling, `Down` → red.
3. Since the bridge script pushes real probe results on an interval,
   set this visual's **Page refresh** to match `HEALTH_CHECK_INTERVAL_SEC`
   from the bridge config so it doesn't look stale between probes.

## 7. Alerts Overview

If you don't have a dedicated alerts pipeline yet, derive severity from
`Score` on the `Transactions` table as an interim proxy:
`Critical: Score >= 0.95`, `High: 0.85–0.95`, `Medium: 0.5–0.85`,
`Low: < 0.5` — four card visuals with `CALCULATE(COUNTROWS(...), ...)`
measures per band. Swap this for a real alerts table once one exists;
deriving severity from the fraud score is a placeholder, not a source
of truth.
