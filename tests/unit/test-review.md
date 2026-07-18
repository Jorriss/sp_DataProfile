# Mode0 test review — evaluation + plan to strengthen assertions

## Context

Evaluation of the `Mode0.sql` unit tests: are they complete, do the assertions actually check the
state under test, and where counts are asserted, is a count the best way to prove the behavior?
Decisions taken: produce a **report + a plan to fix**, move count/existence checks to **full value
assertions** where feasible, and **scan the rest of `tests/unit/` and flag where the same
weaknesses recur**.

Reference points:
- Mode 0 emits two result sets — set 1 = overview row (`object_id, schema_name, table_name,
  row_count, is_sample`) at [sp_DataProfile.sql:967-971](../../sp_DataProfile.sql#L967-L971);
  set 2 = column schema at [sp_DataProfile.sql:973-987](../../sp_DataProfile.sql#L973-L987).
  Optional FK set (built at [sp_DataProfile.sql:382-457](../../sp_DataProfile.sql#L382-L457)) and
  index set (built at [sp_DataProfile.sql:464-539](../../sp_DataProfile.sql#L464-L539)) follow when
  the flags are on.
- Fixtures: `AllTypes` (all NOT NULL), `Nullable`, `Cardinality`, `Parent`/`Child` (real FK +
  PK), etc. in [tests/install/03_create_fixture_db.sql](../install/03_create_fixture_db.sql).
- `tSQLt.AssertEqualsTable` matches columns by name and rows by content, and it *does* detect
  row-count and duplicate differences — strictly stronger than a `COUNT(*)` check and also
  implicitly asserts the row count.

---

## Part A — Evaluation of `Mode0.sql` (the four tests)

### Test 1 — `test_Mode0_AllTypes_ReturnsExpectedRowShapeValues` ([Mode0.sql:12-33](Mode0.sql#L12-L33))
Uses `AssertEqualsTable` (good, content-based). But it captures the full 9-column schema set and
then **projects away 6 of the 9 columns**, asserting only `name, system_type, is_nullable`.
Consequences:
- **`is_nullable` is effectively a constant here.** Every `AllTypes` column is `NOT NULL`, so all
  13 rows have `is_nullable = 0`. The `is_nullable = 1` path is never exercised — the assertion
  can't distinguish "reports nullability correctly" from "always returns 0".
- **`length`, `precision`, `scale`, `user_type`, `collation` get zero value coverage** in Mode 0.
  Notably the `length` CASE at [sp_DataProfile.sql:977-982](../../sp_DataProfile.sql#L977-L982)
  (`-1 & xml → NULL`, `-1 → 'max'`, else cast) is never asserted — and no fixture column is
  `xml` or `(max)`, so those branches aren't even reachable by the current fixtures.
- **The overview row (result set 1) is not asserted at all** despite the test name saying
  "…RowShapeValues" and this being *Table Overview* mode. `row_count` (should be 3),
  `is_sample` (`'False'`), `schema_name` (`'dbo'`), `table_name` are unchecked here and,
  as far as I can find, **nowhere else in the suite** (Guards/StackOverflowSmoke only assert the
  metadata row *exists*, never its values). `is_sample` is a `CASE`-formatted `'True'/'False'`
  string that could silently regress.

### Test 2 — `test_Mode0_ShowForeignKeysOnChild_EmitsForeignKeyRows` ([Mode0.sql:35-50](Mode0.sql#L35-L50))
Asserts only `COUNT(*) WHERE fk_name = 'FK_Child_Parent' = 1`. This proves *a* row with that name
came back, but **the entire point of the FK result set — which table references which, on which
columns, in which direction — is unasserted** (`relationship_type`, `parent_table`,
`parent_column_name`, `referrenced_table`, `referrenced_column_name`, the column ids). A count
cannot catch a wrong parent/referenced swap or a mislabeled direction.
- **Flag (possible real bug the stronger assertion would surface):** the UNION branches label
  `relationship_type` `'Outgoing'` for `WHERE tr.name = @TableName` (our table is the *referenced*
  side) and `'Incoming'` for `WHERE tp.name = @TableName` (our table *holds* the FK) —
  [sp_DataProfile.sql:383/405](../../sp_DataProfile.sql#L383). That reads backwards
  (holding the FK is normally the *outgoing* direction). A value assertion on `relationship_type`
  forces us to write down the intended semantics and will pin/expose this.

### Test 3 — `test_Mode0_ShowIndexesOnChild_EmitsIndexRows` ([Mode0.sql:52-68](Mode0.sql#L52-L68))
Asserts only `COUNT(*) WHERE name='PK_Child' AND is_primary_key=1 = 1`. The **most bug-prone part
of the index set — `index_columns`, built by the nested `STUFF`/`FOR XML` concatenation at
[sp_DataProfile.sql:473-484](../../sp_DataProfile.sql#L473-L484) — is never asserted** (expected
`'child_id ASC'`), nor is `type_desc` (`'CLUSTERED'`), `is_unique`, `is_unique_constraint`,
`included_columns`, or `filter_definition`. A count proves the row exists; it proves nothing about
the column list, which is exactly what an index report is for.

### Test 4 — `test_Mode0_BothFlagsOff_OmitsFkAndIndexResultSets` ([Mode0.sql:70-80](Mode0.sql#L70-L80))
`ExpectException @Message = NULL` (any error passes) then requests result set 3. This is the
**weakest assertion in the file**: "any error is acceptable" means an unrelated failure (wrong
loopback server, provider, permissions, a typo) produces a **false green**. It's really testing
`ResultSetFilter`'s out-of-range behavior, not the proc. The comment itself concedes the
fragility. It also asserts nothing positive about what set 2 *is* when the flags are off.

### Completeness gaps specific to Mode 0
- No test for **both flags on together** (`@ShowForeignKeys=1 AND @ShowIndexes=1`) — the header
  comment documents FK=set 3, index=set 4 ordering, but nothing locks that ordering.
- No **overview-row value** test (set 1), as noted.
- No **`is_nullable = 1`** coverage (needs a nullable-bearing table like `Nullable`, profiled in
  Mode 0).

---

## Part B — Recommended changes to `Mode0.sql`

Principle: capture the same sets, but **assert content with `AssertEqualsTable` against
hand-computed expected rows**, and stop projecting away the columns under test. Keep captures on
`@SampleValue=100` (already the helper default) for determinism. `object_id` is DB-dependent, so
assert `object_id IS NOT NULL` / `= OBJECT_ID(...)` rather than a literal.

1. **Overview row (new or folded into Test 1).** Capture set 1 for `AllTypes`; `AssertEqualsTable`
   on `schema_name='dbo'`, `table_name='AllTypes'`, `row_count=3`, `is_sample='False'` (project
   out `object_id`, or assert it non-null separately). This is the missing headline assertion for
   Overview mode.

2. **Column schema values (Test 1).** Expand the expected table to also pin the deterministic
   schema columns for representative rows — e.g. `c_decimal` → `precision=9, scale=2`;
   `c_varchar`/`c_nvarchar` → `length` values and non-null `collation`; and keep `system_type`.
   Add an `is_nullable=1` case by **also profiling `Nullable` in Mode 0** (its `id` is NOT NULL →
   0, `s`/`soft` nullable → 1) so both nullability states are asserted.

3. **FK set (Test 2).** Replace the count with `AssertEqualsTable` against the expected single row:
   `relationship_type` (write down the intended value — see the flagged label concern),
   `fk_name='FK_Child_Parent'`, `parent_table='Child'`, `parent_column_name='parent_id'`,
   `referrenced_table='Parent'`, `referrenced_column_name='parent_id'`. This also implicitly
   asserts exactly one row.

4. **Index set (Test 3).** Replace the count with `AssertEqualsTable` against the expected PK row:
   `name='PK_Child'`, `is_primary_key=1`, `is_unique=1`, `type_desc='CLUSTERED'`,
   `index_columns='child_id ASC'`, `filter_definition` NULL. Locks the concatenation logic.

5. **Both flags off (Test 4).** Keep a negative check but make it not-any-error: strengthen to a
   **positive** assertion instead — capture set 2 with both flags off and `AssertEqualsTable` that
   it is still the column-schema set (proves set 2 is the last meaningful set). If the "set 3 must
   error" behavior is still wanted, keep it as a *secondary* check but narrow `@Message` toward the
   specific `ResultSetFilter` out-of-range text (accepting it's tSQLt-version sensitive) rather
   than `NULL`.

6. **New — both flags on.** Add one test that sets both flags and asserts FK lands as set 3 and
   index as set 4 (two captures), locking the documented result-set ordering.

*Fixture note:* items 1–6 need **no fixture change** (`AllTypes`, `Nullable`, `Parent`/`Child`
already suffice). Only the `length='max'` / `xml→NULL` branch would require a new fixture column
(`varchar(max)` + `xml`); that is called out below as a shared, optional gap, consistent with the
inventory's already-acknowledged fixture gaps.

---

## Part C — Same weaknesses elsewhere in `tests/unit/` (flagged for a follow-up pass)

Prioritized; the first three are the same class of issue found in Mode 0.

1. **Overview/metadata set (set 1) values under-asserted everywhere.** Mode 1 & Mode 2 never
   assert their overview row; Mode 4 asserts `distinct_row_count` but not `row_count`/`is_sample`/
   `column_name`. `is_sample` string formatting is asserted nowhere. Same gap as Mode 0.

2. **`is_nullable`, `unique_ratio`, `nulls_ratio` never value-asserted.** Mode 1 captures all
   three in every test ([Mode1.sql](Mode1.sql)) but projects them away. e.g. `Nullable.s` →
   `nulls_ratio` = 2/6; `Cardinality.uniq_col` → `unique_ratio` = 1. Derived-ratio math has zero
   coverage. `is_nullable` is capturable against `Nullable`/`[Odd Names]`.

3. **`length`/`precision`/`scale`/`user_type`/`collation` never value-asserted** in any mode, and
   the Mode 0/1 `length` CASE (`max`, `xml→NULL`) has **no fixture reaching it**. Shared fixture
   gap (add a `varchar(max)`+`xml` column) — optional, matches the inventory's documented gaps.

4. **Count-only assertions that a value assertion would improve (lower severity):**
   - [Mode4.sql:14-23](Mode4.sql#L14-L23) `ReturnsDistinctValueCount` asserts only `COUNT=3`, and
     the very next test does a full `AssertEqualsTable` on the same capture — the count test is
     largely redundant; fold or drop.
   - [Mode1.sql:87-104](Mode1.sql#L87-L104) `ColumnListSpecified` — `COUNT=1` + name; could
     `AssertEqualsTable` the single expected row.
   - [Mode3.sql:29-42](Mode3.sql#L29-L42) and the `[Odd Names]` Mode 3 test use `COUNT` + scalar
     reads; an `AssertEqualsTable` on `(row_count, k1, v)` is cleaner and the
     `view_data_sql LIKE 'SELECT * FROM %'` check ([Mode3.sql:56](Mode3.sql#L56)) is loose.

5. **Acceptable-by-design count/existence checks — leave as-is (documented constraints):**
   `Sampling.sql` (bounds only — page-sampling nondeterminism), `StackOverflowSmoke.sql`
   (no value asserts — snapshot drifts), the `Smoke.sql` and `Mode3` loopback plumbing proofs,
   and `Guards.sql` `AssertEmptyTable` (early-return has genuinely no result set). Mode 2's
   below-110 "median dropped" test proves the drop via capture *shape* — implicit but legitimate.

---

## Verification

No build system; verification is running the tSQLt suite in SSMS (the chosen path). After editing
`tests/unit/Mode0.sql` (and any follow-up files):
1. Ensure the proc and fixtures are installed (see [tests/README.md](../README.md) prereqs).
2. In SSMS: **Query ▸ SQLCMD Mode**, open [tests/run_all.sql](../run_all.sql) from `tests/`, F5 —
   or `sqlcmd -b -S (local) -i tests/run_all.sql`.
3. Iterate on just this class with `EXEC tSQLt.Run 'Mode0';` (run `EXEC tSQLt.Run 'Smoke';` first
   to confirm the capture/loopback plumbing is green before trusting value failures).
4. A **new value assertion going red is the signal to inspect**, not auto-"fix" — specifically the
   FK `relationship_type` label (Part A, Test 2) may reveal an actual direction-labeling bug in the
   proc; confirm intended semantics before pinning the expected value.

## Open items
- Whether to also **add the `varchar(max)`+`xml` fixture column** now (unlocks the `length` CASE
  branches) or leave it as a documented gap.
- Whether Part C fixes should be **in scope for this change** or tracked as a follow-up issue
  (per the repo's GitHub-issue convention).

## Resolved
- **FK `relationship_type` labeling (was backwards): fixed.** The two branches had their labels
  swapped — `WHERE tr.name = @TableName` (our table is the *referenced* side) now correctly reports
  `'Incoming'` and `WHERE tp.name = @TableName` (our table *holds* the FK) reports `'Outgoing'`
  (sp_DataProfile.sql:383/405). `Mode0` Test 2 expected value updated to `'Outgoing'`.
- **`index_columns` leading-space off-by-one: fixed.** The `STUFF(..., 1, 1, '')` stripped only the
  comma from the `', '` separator; changed to length 2 so it strips both (sp_DataProfile.sql:484,496).
  `Mode0` Test 3 expected value updated from `' child_id ASC'` to `'child_id ASC'`.
