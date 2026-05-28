# Introduction-to-Big-Data-Technologies-Project

## Spark Catalyst Optimizer: A Performance Analysis

**An experimental investigation of Apache Spark's Catalyst optimizer — when it optimizes, when it fails, and when optimization itself costs more than it saves.**

## Overview

This project systematically analyzes Apache Spark's Catalyst optimizer, the query optimization engine that transforms logical query plans into efficient physical execution plans. Through six controlled experiments, we investigate:

- How different query interfaces (DataFrame API, SQL, RDD) affect execution plans
- When predicate pushdown succeeds and what defeats it
- Whether join reordering normalizes all logically equivalent queries
- How catalyst rewrites complex queries across four planning stages
- Whether algebraic equivalence is recognized (e.g., `total > 100` vs `subtotal + tax + shipping > 100`)
- Which optimization rules are most critical for performance — and which ones add overhead

## Key Findings

### 1. High-Level APIs Converge; RDD Bypasses Catalyst
DataFrame API, Spark SQL, and mixed SQL+DataFrame approaches produced **byte-identical optimized plans**. RDD transformations bypass Catalyst entirely, showing only lineage/DAG information.

### 2. Predicate Pushdown Is Easily Defeated
| Filter Type | Pushed Down? | Relative Performance |
|-------------|--------------|---------------------|
| `total > 100` (deterministic) | ✅ Yes | Baseline |
| Python UDF | ❌ No | ~4.4x slower |
| `rand() > 0.5` (non-deterministic) | ❌ No | ~0.5x faster (different result set) |
| Complex `CASE WHEN` | ❌ No (simplified but not pushed) | ~0.8x baseline |

### 3. Join Reordering Is Not Invariant Without CBO
Six logically equivalent three-way join orderings collapsed into **3 distinct optimized plans**, not 1. Runtime gap between fastest and slowest families: **~36%**. Without cost-based optimization enabled, Catalyst's `ReorderJoin` rule only ensures connectivity — it does not search for the optimal join order.

### 4. ColumnPruning Is the Most Critical Rule
Disabling each of 7 Catalyst rules one at a time produced unexpected results:

| Rule Disabled | Slowdown vs Baseline |
|---------------|----------------------|
| **ColumnPruning** | **+127%** (much slower) |
| FilterPushdown | -5% (faster) |
| PushPredicateThroughJoin | -9% (faster) |
| ConstantFolding | -20% (faster) |
| ReorderJoin | -20% (faster) |
| CombineFilters | -26% (faster) |
| CollapseProject | -27% (faster) |

**Takeaway:** ColumnPruning is essential (reduces I/O by reading only needed columns). Other rules can add measurable overhead — for simple queries, disabling them actually improves performance.

### 5. Algebraic Equivalence Is Not Recognized
`total > 100` and `subtotal + tax + shipping > 100` are mathematically equivalent (sanity check confirmed 0 mismatched rows). Catalyst treated them as completely different expressions — the algebraic form was not pushed down and required reading 3 columns instead of 1.

### 6. Catalyst Rewrites Queries Across Four Planning Stages
A complex query (3 joins, 3 filters, aggregation, sorting) was traced through:
- **Parsed logical plan** → unresolved relations
- **Analyzed logical plan** → resolved table/column IDs
- **Optimized logical plan** → column pruning, filter pushdown, join reordering
- **Physical plan** → BroadcastHashJoin + SortMergeJoin + two-stage HashAggregate


## Technologies Used

- **Apache Spark (PySpark)** — distributed query execution
- **Google Colab** — reproducible 2-core local environment
- **Python** — data manipulation and analysis
- **Matplotlib / Pandas** — results visualization

## How to Reproduce

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the notebook in Google Colab or locally with Spark installed
4. Note: The dataset is ~350MB (orders, customers, order_items, products). Download links are in the notebook.

## Key Experimental Controls

- AQE disabled (`spark.sql.adaptive.enabled=false`) to ensure `explain()` matches execution
- Cache cleared before each timing run
- Median of 5 runs after warmup
- Fixed seed (Team Seed = 2) for reproducible data subsets
- 2-core local cluster (`local[2]`) pinned for consistency

## Lessons Learned

1. **Catalyst is form-agnostic at the API layer** (DataFrame/SQL converge) but **expression-shape sensitive**
2. **Without CBO, join order normalization is incomplete** — user-written FROM clause order leaks into physical plans
3. **Optimization has overhead** — disabling most rules made simple queries faster
4. **ColumnPruning is uniquely critical** — it operates at the I/O boundary
5. **Catalyst does not infer semantic equivalences** between columns (e.g., `total = subtotal+tax+shipping`)
