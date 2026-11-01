# Tableau Public dashboard: build guide

Three dashboards in one Tableau Public workbook:

1. **Price impact over time**: daily Kyle's lambda with its confidence band,
   next to a liquidity measure the viewer picks.
2. **High-volatility days**: lambda against volatility with volume held fixed;
   click a day to see its minute-by-minute price path and order flow.
3. **BTC vs ETH**: price impact level, co-movement and hour-of-day pattern
   across the two pairs.

Time: about one working day, plus publishing.

## 0. Data

From the repo root, with the virtual environment active and the Databricks
exports in `exports/`:

```powershell
python -m microstructure.dashboard --exports exports --out exports/tableau
```

| File | Rows | Grain | Used for |
|---|---|---|---|
| `dash_daily.csv` | 180 | pair x day | dashboards 1-3 |
| `dash_bars.csv` | 259,198 | pair x minute | drill-down in dashboard 2 |
| `dash_hourly.csv` | 48 | pair x UTC hour | dashboard 3 |

Install **Tableau Public** (the free desktop app, from public.tableau.com) and
sign in with your Tableau Public account.

## 1. Connect

Use three separate data sources, not a relationship: the files share column
names such as `Trades` and `Volume ($M)`, and separate sources keep those
from being mixed up.

1. **Connect → To a File → Text file** → `dash_daily.csv`.
2. On the Data Source page check the types: `Date` should be a date (calendar
   icon), `High-vol day` and `Pair` strings, everything else numbers.
3. **Data → New Data Source** for `dash_bars.csv`. Set `Bar start (UTC)` to
   **Date & Time** (click the type icon above the column).
4. **Data → New Data Source** for `dash_hourly.csv`. Then open a worksheet
   (the Data pane only appears there), select this source at the top of the
   Data pane, and right-click `Hour (UTC)` → **Convert to Dimension**. In
   current versions the pane has no "Dimensions" and "Measures" headings:
   dimensions sit above a thin grey line and measures below it, so the field
   moves above the line and its icon turns blue.
5. Rename the sources (right-click in the Data pane): `Daily`, `Bars`,
   `Hourly`.

## 2. Calculated fields and a parameter (Daily source)

**Analysis → Create Calculated Field**:

| Name | Formula |
|---|---|
| `CI width` | `[Lambda 95% CI high] - [Lambda 95% CI low]` |
| `Significance` | `IF ABS([Lambda t-stat (HC1)]) >= 1.96 THEN "Significant at 5%" ELSE "Not significant" END` |

Then a parameter so dashboard 1 can swap measures. Create the parameter
first, because the calculated field below refers to it.

1. Right-click an empty spot in the Data pane → **Create Parameter...**
   Name `Liquidity measure`, data type **String**, allowable values **List**.
   Type these five into the **Value** column, one per row, exactly as written:
   `Realized vol (%)`, `Volume ($M)`, `Roll spread (bps)`,
   `Amihud illiquidity`, `Order-flow imbalance`. Click **OK**; it appears
   under **Parameters** at the bottom of the Data pane.
2. **Analysis → Create Calculated Field...**, name it `Selected measure`, and
   paste this into the formula box (the `//` line is a comment and optional):

   ```
   // Selected measure
   CASE [Liquidity measure]
   WHEN "Realized vol (%)"      THEN [Realized vol (%)]
   WHEN "Volume ($M)"           THEN [Volume ($M)]
   WHEN "Roll spread (bps)"     THEN [Roll spread (bps)]
   WHEN "Amihud illiquidity"    THEN [Amihud illiquidity]
   WHEN "Order-flow imbalance"  THEN [Order-flow imbalance]
   END
   ```

   The box should say **The calculation is valid.** If not, check that each
   quoted name matches a parameter value exactly. Click **OK**.
3. Right-click the parameter → **Show Parameter**. A dropdown appears on the
   right of the sheet; it drives `Selected measure` in `Liquidity over time`.

## 3. Sheets

Name each sheet as shown (double-click the tab). Each line is one shelf or
setting.

### `Lambda over time` (Daily)

- Columns: `Date` → right-click the pill → **Exact Date**, then **Continuous**
  (pill turns green).
- Rows: `Pair`, then `Lambda 95% CI low`, then `Lambda (bps per $1M)`.
- First measure's Marks card: mark type **Gantt Bar**, drag `CI width` to
  **Size**, set the colour to a light grey. This draws the 95% band.
- Second measure: right-click its pill → **Dual Axis**, then right-click the
  right axis → **Synchronize Axis** and hide it (uncheck **Show Header**).
  Mark type **Line**.
- Keep the line one colour. Putting `High-vol day` on its **Color** would
  split it into two lines that join points of the same colour across days;
  high-volatility days are marked on the sheet below instead.
- Right-click the left axis → **Edit Axis → Independent axis ranges for each
  row or column**, because ETH's lambda is about 2.5 times BTC's.
- Axis title: `Lambda (bps per $1M of net buying)`.

### `Liquidity over time` (Daily)

- Columns: `Date` (exact, continuous). Rows: `Pair`, `Selected measure`.
- Mark type **Bar**; `High-vol day` on **Color** (same colours as above).
- Independent axis ranges per row, as before.
- Title: insert the parameter (**Insert → Parameters.Liquidity measure**) so
  the title follows the choice.

Note: `Roll spread (bps)` is empty on 50 of 180 days, where the price changes'
autocovariance is positive and the estimator is undefined. Say so in a
caption.

### `Impact vs volatility` (Daily)

The added-variable plot. Both axes are logs with each pair's dependence on
volume removed, so the slope of the trend line is the regression's
volatility elasticity.

- Columns: `Log vol, volume-adjusted`. Rows: `Log lambda, volume-adjusted`.
- `Pair` on **Color**, `High-vol day` on **Shape**, `Date` on **Detail**
  (right-click → **Exact Date**, keep it discrete/blue).
- **Analysis → Trend Lines → Show Trend Lines**, model **Linear**. Leave
  "Allow a trend line per color" on.
- Tooltip: add `Lambda (bps per $1M)`, `Realized vol (%)`, `Volume ($M)` to
  **Tooltip** so hover shows real units, not residuals.
- Axis titles: `Volatility, log, volume held fixed` and `Price impact, log,
  volume held fixed`.

**Check:** hover over each trend line. Slopes should be **1.594 (BTCUSDT)**
and **1.613 (ETHUSDT)**, the per-pair `logs` coefficients in
`python_results.csv`.

Why not the raw scatter: a straight log-log line of lambda on volatility gives
about 0.48 for BTC and 0.04 for ETH, because volatile days are also busy
days and extra depth pushes lambda down. Plotting raw values would hide the
result.

### `Intraday path` (Bars)

- Columns: `Bar start (UTC)` → **Exact Date**, **Continuous**.
- Rows: `Cumulative return (bps)`, mark type **Line**.
- Rows also `Pair` and `Date` (discrete) so the header names the day.
- Title: `Price path through the day (bps from first bar)`.

### `Flow vs return` (Bars)

- Columns: `Net flow ($M)`. Rows: `Return (bps)`.
- **Analysis → Aggregate Measures** off, so each minute bar is a point.
- Mark type **Circle**, small size, opacity about 30% (Color → Opacity).
- Trend line, Linear.

**Check:** filter to BTCUSDT, 1 Aug 2026 temporarily. The trend line slope
should be **1.852**, the same as that day's `Lambda (bps per $1M)` in
`dash_daily.csv`. Remove the filter afterwards. This is the dashboard's
own demonstration of what lambda is: the slope of returns on order flow.

### `Day card` (Daily)

- Rows: `Pair`, `Date` (exact, discrete).
- **Text** card: `Lambda (bps per $1M)`, `Lambda 95% CI low`,
  `Lambda 95% CI high`, `Realized vol (%)`, `Volume ($M)`,
  `Daily return (%)`, `Significance`. Edit the text so it reads as a short
  summary, e.g. `Lambda <AGG(Lambda)> bps per $1M (95% CI <low> to <high>)`.

### `Impact by pair` (Daily)

- Columns: `Pair`, `High-vol day`. Rows: `Lambda (bps per $1M)` → **Measure →
  Median**. Mark type **Bar**, `High-vol day` on **Color**.
- Labels on. Medians: BTCUSDT 3.21 vs 2.90 bps per $1M, ETHUSDT 8.90 vs
  7.53 (high-vol days vs other days).

### `Co-movement` (Daily)

- Columns: `Date` (exact, continuous). Rows: `Lambda vs pair median`.
- `Pair` on **Color**, mark type **Line**.
- **Analytics** pane → drag **Reference Line** onto the axis, value constant
  1, label `Pair median`.
- This puts both pairs on one scale. Volatility moves almost together
  across the pairs (correlation 0.92 across days) but price impact much less
  (0.41), which is worth a caption.

### `Hour of day` (Hourly)

- Columns: `Hour (UTC)` (discrete). Rows: `Lambda 95% CI low` (Gantt, size
  `CI width`, built as in `Lambda over time`; create `CI width` again in this
  source) and `Lambda (bps per $1M)` (line, dual axis, synchronized).
- `Pair` on **Rows** with independent axes, or on **Color** with one axis.

## 4. Dashboards

For each: **Dashboard → New Dashboard**, Size **Fixed**, 1200 x 850. Add a
title (Objects → Text) and one line of explanation under it.

### Dashboard 1, `Price impact over time`

- `Lambda over time` on top, `Liquidity over time` below.
- Show the `Liquidity measure` parameter control: select a sheet on the
  dashboard, click the ▾ at its top-right → **Parameters → Liquidity measure**.
- Add a `Pair` filter that controls both charts. A filter added on a single
  worksheet only affects that sheet, so:
  1. On the dashboard, select `Lambda over time`, click its ▾ →
     **Filters → Pair**. A filter card appears on the dashboard.
  2. On the filter card's own ▾ → **Apply to Worksheets → All Using This
     Data Source** (or **Selected Worksheets...** and tick both sheets).
  3. Test: untick ETHUSDT; the ETH row should disappear from both charts.
- Explainer: *Kyle's lambda: basis points the price moves per $1M of net
  buying, estimated from 1,440 one-minute bars per day. Grey band: 95%
  confidence interval with White (HC1) standard errors.*

### Dashboard 2, `High-volatility days`

- Left half: `Impact vs volatility`. Right half: `Day card` above `Intraday
  path` and `Flow vs return`.
- **Dashboard → Actions → Add Action → Filter**:
  - Source: `Impact vs volatility`, run on **Select**.
  - Targets: `Day card`, `Intraday path`, `Flow vs return`.
  - Clearing the selection will: **Exclude all values** (so the right side is
    empty until a day is picked, rather than drawing 259,000 points).
  - Target filters: **Selected fields**. Map `Pair` → `Pair` and
    `Date` → `Date` for each target; for the two Bars sheets, choose the Bars
    source's fields on the right-hand side.
- Text over the right side: *Click a day on the left.*
- Show a `High-vol day` filter on the scatter so a viewer can show only the
  top fifth of days by volatility.
- Explainer: *Each point is one pair-day. Both axes have volume taken out
  within each pair, so the line's slope (about 1.6) is the volatility
  elasticity of price impact from the regression. Same-day estimate: the
  README's split-sample and IV checks put the effect from cleaner designs at
  1.25 to 1.45.*

Try the pair's most volatile days: ETHUSDT 19 Aug (return +16%) and 22 Aug,
BTCUSDT 21 Aug and 19 Aug.

### Dashboard 3, `BTC vs ETH`

- `Impact by pair` and `Hour of day` side by side on top, `Co-movement`
  below.
- Explainer: *The same net order flow moves ETH about 2.5 times as far as
  BTC (median 7.7 vs 3.0 bps per $1M).*

Finally, a source note on each dashboard: *Data: Binance spot trades, 1 Aug to
29 Oct 2026, 480 million trades. Pipeline: PySpark on Databricks; code on
GitHub* with the repo link.

## 5. Publish

1. Name the workbook `Crypto market microstructure: price impact and
   liquidity`.
2. Hide the worksheets so only the dashboards show as tabs: right-click a
   dashboard tab → **Hide All Sheets**, then drag the three dashboard tabs
   into order. (To edit a hidden sheet, right-click it on a dashboard →
   **Go to Sheet**.)
3. **File → Save to Tableau Public As...** Tableau Public converts the data
   to an extract automatically; the 259,000-row bars file is well under the
   15 million row limit.
4. In the browser that opens, click the **Settings** gear on the viz and turn
   on **Show Sheets as Tabs**; without it the web version shows only the
   first dashboard. Optionally add **Navigation** buttons (Objects panel) on
   each dashboard as well.
5. **Edit Details** → add a description and the
   GitHub link; under **Settings**, decide whether to allow downloads (on is
   fine; the data is public).
6. Check the dashboard in the browser, not only in the app: open each tab,
   use the parameter, click a day on dashboard 2.
7. Copy the share link and add it to the README (`Dashboard` section) and to
   your Tableau Public profile.

## 6. Checks before calling it done

- [ ] `Impact vs volatility` trend slopes: 1.594 BTC, 1.613 ETH
- [ ] `Flow vs return` slope for BTCUSDT 1 Aug: 1.852, equal to that day's lambda
- [ ] Medians in `Impact by pair` match the numbers above
- [ ] Every dashboard has units on its axes and a one-line explainer
- [ ] Dashboard 2 is empty until a day is clicked, and filters both Bars sheets
- [ ] The published version shows the three dashboards as tabs, and no worksheets
- [ ] The published link opens in a private window (not signed in)
