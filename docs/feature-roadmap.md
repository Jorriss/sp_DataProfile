# sp_DataProfile — Feature-Completeness Roadmap

This document expands the **"What to add for a genuinely complete profiling tool"** section of [analysis.md](analysis.md) (lines 57–84). Each candidate feature is fleshed out — what it is, why it matters, and how it would land inside this specific single-file dynamic-SQL proc — and graded on **importance** and **difficulty**. It is a planning aid, not a commitment; grades are judgment calls and are meant to start a conversation, not end one.

For *correctness and performance* work (the single-pass rewrite, the `NVARCHAR(MAX)` fixes, etc.) see [fix-plan.md](fix-plan.md); this doc is strictly about new profiling *capabilities*.

## Grading key

- **Importance** — user value: how often this is the actual reason someone reaches for a profiler, and how much it closes the gap between "column detail dump" and "real profiler."
  - *High* — most users want it; its absence is felt on the first run.
  - *Medium* — valuable to a meaningful subset, or valuable to everyone but not urgent.
  - *Low* — niche or advanced.
- **Difficulty** — implementation effort *within this proc's constraints*: dynamic SQL built into `NVARCHAR(MAX)` and run via `sp_executesql`, `QUOTENAME` on every identifier, cross-DB `QUOTENAME(@DatabaseName)` qualification, the SQL Server 2012 compatibility floor, and per-scan cost on wide/large tables.
  - *Low* — rides existing machinery (e.g. an extra aggregate on the Mode 1 single-pass `#agg` scan, or a `CASE` at reshape time).
  - *Medium* — a new query/mode following an existing pattern, or a version/compat gate.
  - *High* — new algorithmic surface (pattern matching, combinatorial search) or a structural change to how results are produced.

## Summary matrix

Ordered roughly by value-for-effort (best first); the **#** column is the canonical id used by the detailed sections and build order below.

| # | Feature | Bucket | Importance | Difficulty |
|:-:|---------|--------|:----------:|:----------:|
| 1 | Blank/empty/whitespace + zero/negative counts ✅ **done** | Per-column | High | Low |
| 2 | Cardinality classification ✅ **done** | Per-column | High | Low |
| 3 | Min/max string *values* ✅ **done** | Per-column | Medium | Low |
| 9 | Table size / partitions / last-stats-update ✅ **done** | Structural | Medium | Low |
| 4 | Percentiles + SUM + coefficient of variation | Per-column | Medium | Low–Med |
| 8 | PK / defaults / check / computed-column defs | Structural | Medium | Low–Med |
| 15 | Test harness (tSQLt / golden output) ✅ **done** | Operational | High | Medium |
| 6 | Mode value + frequency; top-N / bottom-N | Per-column | High | Medium |
| 7 | Data-type-mismatch detection | Per-column | Medium | Medium |
| 10 | Orphan / referential-integrity check | Relational | Medium | Medium |
| 12 | Sensitive-data (PII) flagging | Governance | Medium | Medium |
| 13 | Persist results to a history table | Operational | Medium | Medium |
| 14 | Batch mode (whole schema / DB / wildcard) | Operational | Medium | Medium |
| 5 | Pattern & format profiling | Per-column | High | High |
| 11 | Functional-dependency / composite-key discovery | Relational | Low–Med | High |

---

## Per-column depth (the biggest gap)

### 1. Blank/empty/whitespace + zero/negative counts ✅ **Delivered**
**Importance: High · Difficulty: Low**

*Shipped in Mode 1: `num_blank`/`num_whitespace` (string columns), `num_zero`/`num_negative` (numeric columns), each with a computed ratio; all ride the single-pass `#agg` scan.*

Today `num_nulls` counts `IS NULL` only, so an all-`''` or all-space column looks fully populated. Profiling reality is that empty strings, whitespace-only strings, zeros, and negatives are "soft nulls" that matter as much as real NULLs — they're the first thing a data steward looks for. Concretely: `SUM(CASE WHEN col = '' THEN 1 ELSE 0 END)`, `SUM(CASE WHEN LTRIM(RTRIM(col)) = '' AND col <> '' THEN 1 ELSE 0 END)`, and for numerics `SUM(CASE WHEN col = 0 ...)` / `SUM(CASE WHEN col < 0 ...)`. These are pure additive aggregates, so they ride the existing Mode 1 single-pass `#agg` scan for free — new columns in `#table_column_profile`, no extra table read.

### 2. Cardinality classification ✅ **Delivered**
**Importance: High · Difficulty: Low**

*Shipped in Mode 1 as the `cardinality` column (Constant / Binary / Unique / Categorical / High-cardinality), derived at output time from `num_unique_values`/`num_rows`; the Categorical boundary is tunable via the new `@CategoricalMaxDistinct` (default 50) parameter.*

Auto-label each column as *constant* / *binary* / *categorical* / *high-cardinality* / *unique-key-candidate*. This turns raw numbers into an at-a-glance verdict — the single most "profiler-feeling" cheap win. It's a pure derivation from the `unique_ratio` and distinct count the proc **already computes**, so it's a `CASE` expression applied at reshape time (e.g. distinct = 1 → constant; distinct = 2 → binary; unique_ratio = 1 → key candidate; distinct/rows < threshold → categorical; else high-card). No scan, no version gate — it's a new computed column on `#table_column_profile`.

### 3. Min/max *values* ✅ **Delivered**
**Importance: Medium · Difficulty: Low**

*Shipped in Mode 1: `min_value`/`max_value` now carry the alphabetical extremes for string columns (truncated to 100 chars) and the numeric extremes for number columns (cast to `NVARCHAR(100)`, mirroring Mode 2), reusing the existing columns and riding the single-pass scan. String coverage excludes `(max)` types, where `MIN`/`MAX` aggregates are invalid.*

Alphabetical first/last actual values, not just min/max *length* (which Mode 1 already gives). Seeing the literal extremes ("`' '`" vs "`'ZZZ test'`") surfaces stray leading spaces, sentinel values, and encoding junk instantly. Plain `MIN(col)` / `MAX(col)` on string types ride the single-pass scan alongside the existing `MIN/MAX(LEN(col))`. The only wrinkle is result width — store truncated (e.g. `LEFT(MIN(col), 100)`) so a wide value doesn't bloat `#table_column_profile`.

### 4. Percentiles + SUM + coefficient of variation
**Importance: Medium · Difficulty: Low–Medium**

Extend the median to a full spread: P25/P75/P90/P95/P99, plus `SUM` and coefficient of variation (`stddev / mean`) for numerics. Percentiles are what tell you whether "average order = $80" hides a long tail. The proc already has the `PERCENTILE_DISC` machinery for median in Mode 2, so this is more of the same expression, gated on the **same compatibility level 110+** check that already guards median (and degrades gracefully below it). `SUM` and coefficient of variation are trivial aggregates that ride the Mode 2 stats scan. Difficulty nudges above "Low" only because percentiles use window functions that can't share the scalar-aggregate scan (same constraint the median already lives with).

### 5. Pattern & format profiling
**Importance: High · Difficulty: High**

Detect emails, phone numbers, all-numeric-in-`varchar`, date-in-`varchar`, casing patterns (all-upper/all-lower/mixed), and a length histogram per column. **This is the feature that separates a "column detail" tool from a real profiler** — it answers "is this column clean?" rather than "how many rows?". It's High difficulty because SQL Server has no native regex before the 2025 engine, so patterns must be approximated with layered `LIKE` masks (`'%_@_%._%'` for email, etc.) — each pattern is effectively its own per-column scan or a bundle of `CASE`-count aggregates, and coverage is heuristic, not exact. It also underpins feature #11 (PII flagging). Scope it: ship a handful of high-value patterns first rather than a general engine.

### 6. Mode value + frequency; top-N / bottom-N
**Importance: High · Difficulty: Medium**

The most-common value and its frequency, plus the top-N / bottom-N frequent values, per column. Users constantly ask "what's the dominant value here and how skewed is it?" — Mode 4 answers this for **one column at a time** today; generalizing it to every column is the ask. Difficulty is Medium because, unlike #1–#3, this **cannot ride the single-pass `#agg` scan** — each column needs its own `GROUP BY col ORDER BY COUNT(*)`, so it's inherently a scan-per-column (or a cursor emitting a `TOP N` per column into a new temp table). This is the expensive per-column feature; pair it with the `@ApproxDistinct`-style opt-in so it's off by default on wide tables.

### 7. Data-type-mismatch detection
**Importance: Medium · Difficulty: Medium**

Count rows where a `varchar` column can't `TRY_CONVERT` to its *apparent* type — e.g. a "quantity" column stored as text with `"N/A"` sprinkled in. This is the classic "why did my ETL blow up?" finding. `TRY_CONVERT` and `TRY_CAST` need SQL Server 2012, which is **already the floor**, so no new gating. The catch is knowing the apparent type: either take it as a parameter, or infer it (all rows numeric → try `INT`/`DECIMAL`; all rows parseable as dates → try `DATE`). The count itself is an additive `SUM(CASE WHEN TRY_CONVERT(...) IS NULL AND col IS NOT NULL THEN 1 ...)` that can share a scan.

## Schema / structural

### 8. PK / defaults / check constraints / computed-column definitions
**Importance: Medium · Difficulty: Low–Medium**

Surface primary key, default constraints, check constraints, and computed-column definitions in the overview (Mode 0) — the proc already does FKs and indexes into `#table_relationship` / `#table_indexes`, so this rounds out the structural picture. Difficulty is Low–Medium: it's the **same catalog-view pattern already in use** (`sys.key_constraints`, `sys.default_constraints`, `sys.check_constraints`, `sys.computed_columns`), each qualified with `QUOTENAME(@DatabaseName)` per the cross-DB convention. Mostly rote query-writing plus a temp table (or extra columns) to hold the output.

### 9. Table size / partitions / last-stats-update ✅ **Delivered**
**Importance: Medium · Difficulty: Low**

*Shipped in Mode 0: the overview header now carries `size_mb` (total reserved size, data + all indexes), `partition_count`, `data_compression` (`NONE`/`ROW`/`PAGE`/`COLUMNSTORE`…, or `Mixed` across partitions), and `last_stats_update`; all are populated from one cross-DB metadata query gated to Mode 0. When `@ShowIndexes = 1`, each index row also carries `size_mb`. One implementation note: last-stats-update uses the built-in `STATS_DATE()` rather than the roadmap's suggested `sys.dm_db_stats_properties`, because the latter needs SQL 2012 **SP1** and would risk the RTM-2012 compatibility floor — `STATS_DATE()` returns the same date and is available on every supported version.*

Table size in MB, row count, data compression setting, partition count, and last-statistics-update date in the overview. These are the operational vitals a DBA wants before touching a table, and they're **cheap DMV reads** — `sys.dm_db_partition_stats` (size/rows), `sys.partitions` (compression/partition count), `sys.dm_db_stats_properties` (last update) — no base-table scan at all. The metadata row-count work in [fix-plan.md](fix-plan.md) already touches `sys.dm_db_partition_stats`, so the plumbing is partly there.

## Cross-column / relational

### 10. Orphan / referential-integrity check
**Importance: Medium · Difficulty: Medium**

For FK columns, count child values with no matching parent row — real orphans that a trusted/disabled FK or a soft relationship let slip in. High signal for anyone auditing data quality after migrations. The FK metadata is **already collected in `#table_relationship`**, so the machinery is a per-FK anti-join (`WHERE NOT EXISTS (SELECT 1 FROM parent ...)`) built as dynamic SQL with full `QUOTENAME` + cross-DB qualification. Medium because it's one scan per FK relationship and needs careful join-key construction for composite FKs.

### 11. Functional-dependency / composite-key discovery
**Importance: Low–Medium · Difficulty: High**

Generalize the Mode 3 candidate-key check to *suggest* keys automatically — search column combinations for ones that are unique (candidate composite keys) or that functionally determine another column. Genuinely useful for reverse-engineering an undocumented schema, but niche and expensive: the search space is combinatorial, so it needs pruning heuristics (start from high-cardinality single columns, grow greedily) and is a batch of `COUNT` vs `COUNT(DISTINCT)` scans. Highest difficulty, lowest-in-bucket importance — a "someday" item.

## PII / governance

### 12. Sensitive-data (PII) flagging
**Importance: Medium (trending High) · Difficulty: Medium**

Heuristically flag columns likely to hold PII, combining **column-name matching** (a dictionary: `%ssn%`, `%email%`, `%phone%`, `%dob%`, `%credit%`…) with **value-pattern matching** (reuse feature #5's masks). This is increasingly the *reason* people run a profiler at all, so importance is rising. Difficulty is Medium and mostly rides on #5 being done first — without value patterns it's just name-dictionary matching (still useful, and Low on its own). Output a per-column flag + reason (name-hit / pattern-hit / both) into `#table_column_profile`.

## Operational

### 13. Persist results to a history table
**Importance: Medium · Difficulty: Medium**

Parameterize an output table (`@OutputTable`) so a profile run is captured as rows, enabling trending over time and diffing before/after ETL loads — "did this load change the null rate?". The hard part is committing to a **stable output schema** (the temp tables' shapes become a contract) and an insert path with a run timestamp / run id; once that exists, it's mechanical. This is a prerequisite for #14.

### 14. Batch mode
**Importance: Medium · Difficulty: Medium**

Profile every table in a schema/DB, or a name wildcard, in one call — looping over `sys.tables` and writing each result to the history table from #13. Turns the proc from "one table, interactive" into "profile the whole warehouse overnight." Difficulty is Medium: a cursor over `sys.tables` (cross-DB qualified) calling the existing per-table logic, plus guardrails (skip huge tables, honor sampling). **Depends on #13** for somewhere to put the output.

### 15. Test harness (tSQLt or scripted golden-output)
**Importance: High · Difficulty: Medium**

The `tests/` folder holds one ad-hoc script (`Checking Dates in Mode 3.sql`); there's no automated coverage. A [tSQLt](https://tsqlt.org/) suite (or even scripted golden-output comparisons) covering each mode, cross-DB, sampling, and edge-case column names (`[Order Date]`, reserved words) is what makes **every other change in this doc safe to land** — hence High importance despite being invisible to end users. Difficulty is Medium: choosing the framework, seeding a small deterministic fixture DB, and writing assertions for stable outputs. It's listed under [fix-plan.md](fix-plan.md)'s verification section as a manual process today; this would automate it.

Fleshed out in a dedicated design doc: [test-harness-design.md](test-harness-design.md) — framework choice (tSQLt vs scripted golden-output), micro-fixture strategy, test taxonomy per mode, and a build order for the harness itself.

---

## Suggested build order

The existing "Suggested priority order" in [analysis.md](analysis.md) ends at *"Feature depth"* and *"Operational"* as broad buckets. This refines that tail into a concrete sequence, front-loading the High-importance / Low-difficulty wins that ride the single-pass scan:

1. ~~**Free riders on the single-pass scan** — #1 blank/zero counts, #2 cardinality classification, #3 min/max string values. High/Medium value, near-zero marginal cost once the Phase 3 rewrite has landed.~~ ✅ **Delivered** — all three ride the Mode 1 single-pass `#agg` scan: soft-null counts (`num_blank`/`num_whitespace`/`num_zero`/`num_negative`) + ratios, a `cardinality` label tuned by `@CategoricalMaxDistinct`, and `min_value`/`max_value` extremes (alphabetical for strings, numeric for number columns).
2. **Cheap structural adds** — ~~#9 table size/partition DMVs~~ ✅ **Delivered** (Mode 0 `size_mb`/`partition_count`/`data_compression`/`last_stats_update` + per-index `size_mb`), then #8 constraints in overview. Rote catalog queries, no scan.
3. **Statistical depth** — #4 percentiles/SUM/CV, reusing the median machinery.
4. **The test harness (#15)** — do this before the harder features so the risky ones land safely.
5. **Per-column heavy hitters** — #6 top-N values (opt-in, scan-per-column), #7 type-mismatch, then #5 pattern profiling (the big differentiator) and #12 PII flagging built on top of it.
6. **Relational & operational** — #10 orphan checks, then #13 history table → #14 batch mode as a pair.
7. **Someday** — #11 functional-dependency discovery, when there's appetite for the combinatorial cost.
