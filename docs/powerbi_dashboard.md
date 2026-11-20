# Power BI dashboard: build guide

Written for **Power BI Desktop 2.159 (October 2026)**. Menus move between
monthly releases; if a command isn't where this says, type its name into the
**Search** box in the title bar (it finds ribbon commands), or into the search
box at the top of the **Format visual** pane (it finds formatting options).

The same three dashboards as the [Tableau Public version](tableau_dashboard.md),
built as three pages of one Power BI report from the same three CSVs:

1. **Price impact over time**: daily Kyle's lambda with its 95% band, next to
   a liquidity measure the viewer picks.
2. **High-volatility days**: lambda against volatility with volume held fixed;
   click a day to see its minute-by-minute price path and order flow.
3. **BTC vs ETH**: impact level, co-movement and hour-of-day pattern.

Where Tableau and Power BI differ, the Power BI way is used rather than an
imitation of Tableau (see the table at the end). The one real addition: the
regression checks run inside the report as DAX (an explicit OLS slope), so
the slopes are shown on the page instead of read off a trend-line tooltip.

## Finding your way around

The thin bar on the far left switches views, top to bottom:

| Icon | View | Used for |
|---|---|---|
| Bar chart | **Report** | Pages and visuals. The Filters, Visualizations and Data panes are on the right |
| Grid | **Table** | Looking at the loaded rows; row count in the bottom-left status bar |
| Linked boxes | **Model** | Relationships. Its **Home** tab has New measure, New table, New parameter, Manage relationships |
| DAX | **DAX query** | Not needed here |
| TMDL | **TMDL** | Not needed here |

In Report view the ribbon tabs are Home, Insert, Modeling, Design, View,
Optimize and Help, plus **Format** and **Data / Drill** when a visual is
selected. The **Visualizations** pane has three icons under *Build visual*:
**Build visual** (field wells), **Format visual** (paintbrush) and
**Analytics** (magnifying glass: trend lines, constant lines, error bars).

## 0. Data

The three files are already in `exports\tableau\` from the Tableau build. To
rebuild them, from the **repo root** (`spark-microstructure`, not the
`Apache Spark` folder above it, or the relative `exports` path fails):

```powershell
cd spark-microstructure
python -m microstructure.dashboard --exports exports --out exports/tableau
```

| File | Rows | Grain | Table name in Power BI |
|---|---|---|---|
| `dash_daily.csv` | 180 | pair x day | `Daily` |
| `dash_bars.csv` | 259,198 | pair x minute | `Bars` |
| `dash_hourly.csv` | 48 | pair x UTC hour | `Hourly` |

## 1. Load with Power Query

Loading through M rather than the Text/CSV wizard fixes the column types in
code, adds the join key, and lets the folder be changed in one place.

1. **Home → Transform data**. The Power Query Editor opens in its own window;
   steps 2 to 4 happen there.
2. **Home → Manage Parameters → New Parameter**: name `ExportFolder`, type
   **Text**, current value the full path to `exports\tableau\` *with* the
   trailing backslash:
   `C:\Users\Bradley\OneDrive\Documents\Projects\Apache Spark\spark-microstructure\exports\tableau\`
3. For each table: **Home → New Source → Blank Query**. With the new
   `Query1` selected in the list on the left, **Home → Advanced Editor**,
   delete everything in the box, paste the code, **Done**. Then right-click
   `Query1` → **Rename**.

   Paste only into the Advanced Editor. Pasted into **Enter data** or a cell,
   the code becomes a one-cell table of text with every quote doubled.

`Daily`:

```m
let
    Source   = Csv.Document(File.Contents(ExportFolder & "dash_daily.csv"),
                   [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    TextCols = {"Pair", "Date", "High-vol day"},
    NumCols  = List.Difference(Table.ColumnNames(Promoted), TextCols),
    Typed    = Table.TransformColumnTypes(Promoted,
                   {{"Date", type date}, {"Pair", type text}, {"High-vol day", type text}}
                   & List.Transform(NumCols, each {_, type number}), "en-US"),
    Keyed    = Table.AddColumn(Typed, "DayKey",
                   each [Pair] & "|" & Date.ToText([Date], "yyyy-MM-dd"), type text)
in
    Keyed
```

`Bars`:

```m
let
    Source   = Csv.Document(File.Contents(ExportFolder & "dash_bars.csv"),
                   [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed    = Table.TransformColumnTypes(Promoted, {
                   {"Pair", type text}, {"Date", type date},
                   {"Bar start (UTC)", type datetime}, {"Hour (UTC)", Int64.Type},
                   {"Return (bps)", type number}, {"Cumulative return (bps)", type number},
                   {"Net flow ($M)", type number}, {"Volume ($M)", type number},
                   {"Trades", Int64.Type}}, "en-US"),
    Keyed    = Table.AddColumn(Typed, "DayKey",
                   each [Pair] & "|" & Date.ToText([Date], "yyyy-MM-dd"), type text)
in
    Keyed
```

`Hourly`:

```m
let
    Source   = Csv.Document(File.Contents(ExportFolder & "dash_hourly.csv"),
                   [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    NumCols  = List.Difference(Table.ColumnNames(Promoted), {"Pair", "Hour (UTC)"}),
    Typed    = Table.TransformColumnTypes(Promoted,
                   {{"Pair", type text}, {"Hour (UTC)", Int64.Type}}
                   & List.Transform(NumCols, each {_, type number}), "en-US")
in
    Typed
```

4. Check for errors: **View → Column quality**, then click *Column profiling
   based on top 1000 rows* at the bottom-left and switch it to **entire data
   set**. Every column should show 0% Error. `Roll spread (bps)` in `Daily`
   shows about 28% Empty (50 of 180 days): expected.
5. **Home → Close & Apply**.

Check the row counts in **Table view**: click a table in the Data pane and
read the count in the status bar at the bottom-left. `Daily` 180, `Bars`
259,198, `Hourly` 48. (The Power Query Editor only previews rows, so its
counts are not reliable.)

## 2. Model

Do this section in **Model view**, whose **Home** tab has New table, New
measure and New parameter.

### Dimension tables

**Home → New table**, one at a time:

```dax
Pair = DISTINCT ( UNION ( VALUES ( Daily[Pair] ), VALUES ( Hourly[Pair] ) ) )
```

```dax
Dates = CALENDAR ( MIN ( Daily[Date] ), MAX ( Daily[Date] ) )
```

Mark `Dates` as the date table: select `Dates` in the Data pane, then
**Mark as date table** (search for it in the title-bar Search if it isn't on
the ribbon), column `Date`.

### Relationships

Power BI may have guessed some relationships on load. Delete any that are not
in this list (click the line → Delete), and create the missing ones by
dragging column onto column (all single direction, one-to-many):

| From (one) | To (many) |
|---|---|
| `Pair[Pair]` | `Daily[Pair]` |
| `Pair[Pair]` | `Hourly[Pair]` |
| `Dates[Date]` | `Daily[Date]` |
| `Daily[DayKey]` | `Bars[DayKey]` |

**Home → Manage relationships** lists them all, which is the easiest way to
check there are exactly four.

There is deliberately no direct link from `Pair` or `Dates` to `Bars`: filters
reach `Bars` through `Daily`, which gives one unambiguous path and is what
makes the click-a-day drill-down on page 2 work.

Then hide the fact-table copies of the keys so visuals use the dimensions
(right-click the field → **Hide in report view**): `Daily[Pair]`,
`Hourly[Pair]`, `Bars[Pair]`, `Bars[Date]`, `Bars[DayKey]`.

### Measures

A new measure goes on whichever table is selected in the Data pane, so click
the table first, then **Home → New measure**, paste one measure into the
formula bar, and press Enter. Each block below is **one measure**; create
them one at a time.

A measure can't share a name with a column in its table, which is why the
median-relative measure is called `Lambda rel. median`, not
`Lambda vs pair median` (that's a column).

On `Daily`:

```dax
Lambda = AVERAGE ( Daily[Lambda (bps per $1M)] )
```

```dax
Lambda CI low = AVERAGE ( Daily[Lambda 95% CI low] )
```

```dax
Lambda CI high = AVERAGE ( Daily[Lambda 95% CI high] )
```

```dax
Median lambda = MEDIAN ( Daily[Lambda (bps per $1M)] )
```

```dax
Lambda rel. median = AVERAGE ( Daily[Lambda vs pair median] )
```

```dax
Volatility elasticity =
-- OLS slope of the added-variable plot = per-pair logs coefficient (Frisch-Waugh-Lovell)
VAR mx = AVERAGE ( Daily[Log vol, volume-adjusted] )
VAR my = AVERAGE ( Daily[Log lambda, volume-adjusted] )
VAR sxy = SUMX ( Daily, ( Daily[Log vol, volume-adjusted] - mx ) * ( Daily[Log lambda, volume-adjusted] - my ) )
VAR sxx = SUMX ( Daily, ( Daily[Log vol, volume-adjusted] - mx ) ^ 2 )
RETURN
    IF ( HASONEVALUE ( Pair[Pair] ), DIVIDE ( sxy, sxx ) )
```

```dax
Day selected = HASONEVALUE ( Daily[DayKey] )
```

```dax
Selected day =
IF (
    [Day selected],
    SELECTEDVALUE ( Daily[Pair] ) & ", " & FORMAT ( SELECTEDVALUE ( Daily[Date] ), "d mmm yyyy" ),
    "Click a day on the left"
)
```

The day card's fields. Each is blank until one day is selected, and each is
short enough to fit a card tile (one long sentence gets cut off):

```dax
Day lambda = IF ( [Day selected], [Lambda] )
```

```dax
Day 95% CI =
IF ( [Day selected], FORMAT ( [Lambda CI low], "0.00" ) & " to " & FORMAT ( [Lambda CI high], "0.00" ) )
```

```dax
Day volatility (%) = IF ( [Day selected], AVERAGE ( Daily[Realized vol (%)] ) )
```

```dax
Day volume ($M) = IF ( [Day selected], AVERAGE ( Daily[Volume ($M)] ) )
```

```dax
Day return (%) = IF ( [Day selected], AVERAGE ( Daily[Daily return (%)] ) )
```

```dax
Day significance =
IF (
    [Day selected],
    IF ( ABS ( AVERAGE ( Daily[Lambda t-stat (HC1)] ) ) >= 1.96, "Significant at 5%", "Not significant" )
)
```

On `Bars`. Each returns blank unless exactly one day is selected, so the
drill-down visuals stay empty until a day is clicked instead of drawing
259,000 points:

```dax
Price path (bps) = IF ( [Day selected], SUM ( Bars[Cumulative return (bps)] ) )
```

```dax
Bar return (bps) = IF ( [Day selected], SUM ( Bars[Return (bps)] ) )
```

```dax
Bar net flow ($M) = IF ( [Day selected], SUM ( Bars[Net flow ($M)] ) )
```

```dax
Lambda from bars =
-- Re-estimates the selected day's lambda from its 1,440 minute bars: OLS slope of return on net flow
VAR mx = AVERAGE ( Bars[Net flow ($M)] )
VAR my = AVERAGE ( Bars[Return (bps)] )
VAR sxy = SUMX ( Bars, ( Bars[Net flow ($M)] - mx ) * ( Bars[Return (bps)] - my ) )
VAR sxx = SUMX ( Bars, ( Bars[Net flow ($M)] - mx ) ^ 2 )
RETURN
    IF ( [Day selected], DIVIDE ( sxy, sxx ) )
```

On `Hourly`:

```dax
Hourly lambda = AVERAGE ( Hourly[Lambda (bps per $1M)] )
```

```dax
Hourly CI low = AVERAGE ( Hourly[Lambda 95% CI low] )
```

```dax
Hourly CI high = AVERAGE ( Hourly[Lambda 95% CI high] )
```

Number formats: select a measure, and set the format in the measure tools
tab or the **Properties** pane (`0.00` for the lambda measures, `0.000` for
`Volatility elasticity` and `Lambda from bars`).

A measure that lands on the wrong table can be moved: select it and change
its **Home table**.

Check: in Model view each table's card lists its measures (calculator icon)
alongside the columns.

## 3. Field parameter (the measure picker)

Power BI's equivalent of Tableau's parameter plus `CASE` field.

First give the parameter five explicit measures to point at. Fed raw numeric
columns, a field parameter can come out blank in a chart's value well, so
use measures (on `Daily`, one at a time):

```dax
Realized vol avg = AVERAGE ( Daily[Realized vol (%)] )
```

```dax
Volume avg = AVERAGE ( Daily[Volume ($M)] )
```

```dax
Roll spread avg = AVERAGE ( Daily[Roll spread (bps)] )
```

```dax
Amihud avg = AVERAGE ( Daily[Amihud illiquidity] )
```

```dax
OFI avg = AVERAGE ( Daily[Order-flow imbalance] )
```

(Each chart cell is one pair-day, so the average is just that day's value.)

1. **Home → New parameter → Fields** (Model view; in Report view it's under
   **Modeling**). Name `Liquidity measure`.
2. Add the five measures above, in that order. **Create**.
3. A `Liquidity measure` table appears in the Data pane. Select it: its
   definition shows in the formula bar. Change the display names (the first
   item in each row) so the dropdown and title read nicely:

   ```dax
   Liquidity measure = {
       ("Realized vol (%)", NAMEOF('Daily'[Realized vol avg]), 0),
       ("Volume ($M)", NAMEOF('Daily'[Volume avg]), 1),
       ("Roll spread (bps)", NAMEOF('Daily'[Roll spread avg]), 2),
       ("Amihud illiquidity", NAMEOF('Daily'[Amihud avg]), 3),
       ("Order-flow imbalance", NAMEOF('Daily'[OFI avg]), 4)
   }
   ```

   The table has a visible `Liquidity measure` field and two hidden ones
   (`... Fields`, `... Order`).

This version doesn't offer *Add slicer to this page* in the dialog, so add
the picker by hand, in Report view (after step 4 below has set up the
pages):

1. Click an empty spot on the page, then the **Slicer** icon in
   Visualizations (or the list slicer).
2. Drag the visible `Liquidity measure` field into the slicer's **Field**
   well. The five measures appear as tick boxes.
3. **Format visual** (paintbrush) → **Visual** → **Slicer settings**:
   **Options → Style: Dropdown**, and **Selection → Single select** on.
4. Pick **Realized vol (%)** so the chart starts with a measure selected.

Then create the title measure on `Daily` (it refers to the parameter, so it
has to come after it):

```dax
Liquidity title =
-- A field parameter's name column is grouped with its hidden Fields column,
-- so read both together rather than SELECTEDVALUE on the name alone
VAR picked =
    SUMMARIZE (
        'Liquidity measure',
        'Liquidity measure'[Liquidity measure],
        'Liquidity measure'[Liquidity measure Fields]
    )
RETURN
    "Liquidity: "
        & IF (
            COUNTROWS ( picked ) = 1,
            MAXX ( picked, 'Liquidity measure'[Liquidity measure] ),
            "pick a measure"
        )
```

## 4. Theme, pages and canvas

**Theme**: search for **Browse for themes** in the title-bar Search (it's in
the Themes gallery, on the **Design** or **View** tab), and pick
`powerbi/theme.json` from this repo.

**Pages**: the tabs along the bottom. Double-click `Page 1` to rename it;
**+** adds a page. Names: `Price impact over time`, `High-volatility days`,
`BTC vs ETH`.

**Canvas size**: click an empty part of the page so nothing is selected; the
paintbrush pane becomes **Format page**. Under **Canvas settings**, type
**Custom**, 1200 x 850 (the Tableau dashboards' size). Repeat per page.

## 5. Page 1: Price impact over time

**Title** (**Insert → Text box**): *Price impact over time*, and under it:
*Kyle's lambda: basis points the price moves per $1M of net buying,
estimated from 1,440 one-minute bars per day. Shaded band: 95% confidence
interval with White (HC1) standard errors.*

**Lambda over time**: Line chart.
- **Build visual**: X-axis `Dates[Date]` (click the ▾ on the field in the
  well → **Date**, not *Date Hierarchy*). Y-axis `Lambda`. Small multiples
  `Pair[Pair]`.
- **Format visual**: Small multiples → Layout, 1 column, 2 rows. Y-axis →
  turn **Shared y-axis** off (ETH's lambda is about 2.5 times BTC's; search
  *shared* if it isn't under Y-axis). Y-axis title
  `Lambda (bps per $1M of net buying)`.
- **Analytics** (magnifying glass) → **Error bars**: series `Lambda`, on;
  Upper bound `Lambda CI high`, Lower bound `Lambda CI low`, relationship
  **Absolute**. Turn the error **Bar** off and the **Error band** on, light
  grey, about 70% transparency.
- One line colour, as in Tableau; high-volatility days are shown on the
  chart below.

If error bars won't combine with small multiples, use two line charts
instead, each with a filter on one pair (drag `Pair[Pair]` into *Filters on
this visual* and tick one value).

**Liquidity over time**: Clustered column chart.
- **Build visual**: X-axis `Dates[Date]` (Date, not hierarchy). Y-axis the
  `Liquidity measure` parameter field. Legend `Daily[High-vol day]`. Small
  multiples `Pair[Pair]`; format as above (1 column, shared y-axis off).
- Title: **Format visual → General → Title → Text**, click **fx** → Format
  style **Field value** → `Liquidity title`. The title now follows the picker.
- Caption (text box under it): *Roll spread is undefined on 50 of 180 days,
  where price changes have positive autocovariance; those days are blank.*

**Measure picker**: the dropdown slicer from step 3.

No Pair slicer on this page. Small multiples draw a fixed grid, so filtering
to one pair would leave an empty slot rather than drop the row as Tableau
does; both pairs are always shown side by side instead.

Test: switch the picker through all five measures and watch the title change.

## 6. Page 2: High-volatility days

**Title** *High-volatility days*, and: *Each point is one pair-day. Both axes
have volume taken out within each pair, so the line's slope (about 1.6) is
the volatility elasticity of price impact from the regression. Same-day
estimate: the README's split-sample and IV checks put the effect from cleaner
designs at 1.25 to 1.45.*

Left half:

**Impact vs volatility**: Scatter chart.
- **Build visual**: Values `Daily[DayKey]` (one point per pair-day). X-axis
  `Log vol, volume-adjusted`, Y-axis `Log lambda, volume-adjusted` (Sum is
  fine: one row per point). Legend `Pair[Pair]`.
- Tooltips well: `Lambda`, `Daily[Realized vol (%)]`, `Daily[Volume ($M)]`,
  `Daily[High-vol day]`, so hover shows real units, not residuals.
- **Analytics → Trend line** on, **Combine series** off (one line per pair).
- Axis titles: *Volatility, log, volume held fixed* and *Price impact, log,
  volume held fixed*.
- Tableau put high-vol days on Shape; Power BI scatters can't vary marker
  shape by a field, so use the slicer below and the tooltip instead.

**Slope check**: Table with `Pair[Pair]` and `Volatility elasticity`, small,
under the scatter, titled *Trend line slope (OLS in DAX)*. It should read
**1.594** and **1.613**.

**High-vol slicer**: Slicer on `Daily[High-vol day]`.

Right half:

**Day card**: one Card visual with, in order: `Selected day`, `Day lambda`,
`Day 95% CI`, `Lambda from bars`, `Day volatility (%)`, `Day volume ($M)`,
`Day return (%)`, `Day significance`. In **Format visual → Layout**, arrange
the tiles in a grid (2 rows of 4) and shrink the callout font (about 16-18
pt) so nothing is cut off. Rename the tiles in the field well (double-click):
*Lambda (bps per $1M)*, *95% CI*, *Lambda re-estimated from minute bars*,
*Volatility (%)*, *Volume ($M)*, *Return (%)*. Number formats `0.00`;
volume `#,0`. If your card takes only one field, use a Multi-row card.

**Intraday path**: Line chart. X-axis `Bars[Bar start (UTC)]` (not
hierarchy; Format visual → X-axis → Type **Continuous**). Y-axis
`Price path (bps)`. Title *Price path through the day (bps from first bar)*.

**Flow vs return**: Scatter chart. Values `Bars[Bar start (UTC)]` (one point
per minute), X-axis `Bar net flow ($M)`, Y-axis `Bar return (bps)`.
Markers small, about 70% transparency. **Analytics → Trend line** on.

**Interactions** (the drill-down). Select the `Impact vs volatility` scatter,
then **Format** tab (top ribbon) → **Edit interactions**. Small icons appear
over the other visuals:
- Day card, Intraday path, Flow vs return: **Filter** (funnel icon).
- Slope-check table: **None** (it should always show both pairs; with Filter
  it shows one pair and a meaningless slope through a single point).

On the slope-check table also turn **Totals** off (Format visual → Totals).

Click **Edit interactions** again to turn it off.

Clicking a point filters `Daily` to that day, which flows to `Bars` through
the `DayKey` relationship. Clicking empty space clears it, and the three
right-hand visuals go blank because their measures need exactly one day.

Try ETHUSDT 19 Aug (return +16%) and 22 Aug, BTCUSDT 21 Aug and 19 Aug.

## 7. Page 3: BTC vs ETH

**Title** *BTC vs ETH*, and: *The same net order flow moves ETH about 2.5
times as far as BTC (median 7.7 vs 3.0 bps per $1M).*

**Impact by pair**: Clustered column chart. X-axis `Pair[Pair]`, Y-axis
`Median lambda`, Legend `Daily[High-vol day]`. Data labels on, `0.00`.
Expect BTCUSDT 3.21 vs 2.90, ETHUSDT 8.90 vs 7.53.

**Hour of day**: Line chart. X-axis `Hourly[Hour (UTC)]` (Format visual →
X-axis → Type **Categorical**), Y-axis `Hourly lambda`, Small multiples
`Pair[Pair]` with shared y-axis off. Error bars as on page 1 with
`Hourly CI high` / `Hourly CI low`, band on.

**Co-movement**: Line chart. X-axis `Dates[Date]`, Y-axis
`Lambda rel. median`, Legend `Pair[Pair]`. **Analytics → Constant line**
(Y-axis), value 1, label *Pair median*. Caption: *Volatility moves almost
together across the pairs (correlation 0.92 across days); price impact much
less (0.41).*

Layout: `Impact by pair` and `Hour of day` side by side on top,
`Co-movement` below.

On every page, a footer text box: *Data: Binance spot trades, 1 Aug to 29 Oct
2026, 480 million trades. Pipeline: PySpark on Databricks; code on GitHub*
with the repo link (select the text → insert link in the text box toolbar).

## 8. Save and publish

Save as `powerbi/market_microstructure.pbix`. The file holds the imported
data (public Binance data, so fine to commit); it is not covered by the
`exports/` ignore rule.

Optionally also **File → Save as → Power BI project (.pbip)**, which writes
the model and report as text files (TMDL/JSON) that diff cleanly in git.

**Public link.** Power BI's equivalent of Tableau Public is *Publish to web*,
and it has two hurdles Tableau Public doesn't:

1. The Power BI service needs a **work or school** account; personal Gmail
   or Outlook addresses are refused. A UBC address may work.
2. *Publish to web* has to be allowed by that organization's Power BI
   admin. Universities often switch it off.

Try it:

1. **Sign in** (top right) with the UBC account, then **Home → Publish** →
   *My workspace*.
2. In the browser (app.powerbi.com), open the report → **File → Embed report
   → Publish to web (public)** → **Create embed code**. If it's missing or
   greyed out, the tenant has it disabled.
3. Open the link in a private window to check it works signed out.

If it is blocked, publish the static versions instead:

- **File → Export → Export to PDF** → `docs/powerbi_dashboard.pdf`.
- A screenshot of each page (`docs/img/powerbi_page1.png` etc.) in the
  README, and a short GIF of the page-2 click-through.
- The `.pbix` in the repo, so a reviewer with Power BI Desktop can open the
  working report.

## 9. Checks before calling it done

- [ ] `Daily` 180 rows, `Bars` 259,198, `Hourly` 48; no errors in Column quality
- [ ] **Manage relationships** lists exactly the four in step 2
- [ ] Slope-check table: 1.594 BTCUSDT, 1.613 ETHUSDT
- [ ] Click BTCUSDT 1 Aug: `Lambda from bars` reads **1.852**, the same as
      the day's lambda, and the Flow vs return trend line has that slope
- [ ] Page 2 right side is blank until a day is clicked, and blank again on
      Ctrl-clicking a second day
- [ ] `Impact by pair` labels match the medians on page 3
- [ ] The liquidity picker changes both the chart and its title
- [ ] Every chart has units on its axes and each page a one-line explainer

## Tableau → Power BI, what changed

| Tableau | Power BI | Note |
|---|---|---|
| Three separate data sources | One model: `Pair`, `Dates`, `Daily` → `Bars` | Relationships replace blending; filters flow one way |
| Parameter + `CASE` calculated field | Field parameter + dropdown slicer | Built in; the slicer also renames the axis |
| Gantt bar sized by `CI width` on a dual axis | Error bars with **Error band** | Native, no dual-axis trick |
| Independent axis ranges per row | Small multiples, shared y-axis off | Fixed grid: no Pair filter on page 1, since a filtered-out pair leaves an empty slot |
| Filter action on Select, "exclude all values" | Edit interactions → Filter, plus measures that blank without one day | Same behaviour, done in DAX |
| Trend-line slope read from tooltip | OLS-slope measure shown on the page | The check is part of the report |
| `High-vol day` on Shape | Slicer + tooltip | Scatter marker shape can't vary by field |
| Tableau Public link | Publish to web (needs an org account and admin setting) or PDF + `.pbix` | |
