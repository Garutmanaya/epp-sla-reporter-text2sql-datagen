
# Text-to-SQL Data Generator (Step 1: Robotic NL)

## Purpose
This module is designed to generate high-quality, balanced datasets for training Text-to-SQL models. It specifically targets **Step 1: Robotic Natural Language**, focusing on standardized, predictable phrasing that maps directly to SQL structures. This ensures the model learns structural mapping (joins, filters, aggregates) before moving to more complex, human-like linguistic variations.

---

## 🏗️ Architecture Overview

The generator is built on a modular pipeline where data, structure, and logic are strictly decoupled.

### 1. Configuration (`config.py`)
This is the "Source of Truth" for the database schema.
- **COLUMN_VALUES**: Realistic data samples used to populate `WHERE` clauses.
- **METRICS**: Mapping of robotic NL phrases (e.g., "total volume") to SQL aggregations (`SUM(volume)`).
- **TIME_FILTERS**: Standardized temporal constraints for SQLite.

### 2. Utility Layer (`config_utils.py`)
The `ConfigUtils` class provides an interface to access the configuration in two distinct modes:
- **Random Mode**: Stochastic selection for general diversity.
- **Serial Mode**: Round-robin selection ensuring every metric, time filter, and column value is used exactly once before repeating, resulting in a perfectly balanced dataset.

### 3. Structural Templates (`templates.py`)
Templates define the core SQL structure and the distribution logic.
- **SQL Structure**: Defines table relationships and `FROM` clauses (including complex `BETWEEN` joins for releases).
- **Filter Modes**: Every template defines a weight and a list of `filter_modes`. This allows precise control over what percentage of queries are simple, involve joins, or include time/value filters.

### 4. Filter Pipeline (`filters.py`)
The `FilterProcessor` handles the logic of "decorating" a base query. It uses a pipeline approach where a query state is passed through several "hooks":
- **Value/Time Filters**: Adds `WHERE` clauses.
- **Time Series**: Modifies `SELECT` and `GROUP BY` to create trends.
- **Group/Order/Having**: Adds dimensionality, limits (Top N), and aggregate thresholds.

### 5. Execution Engine (`generator.py`)
The `QueryEngine` acts as the glue. It:
1. Picks a template based on defined weights.
2. Selects a filter mode for that template.
3. Orchestrates the `FilterProcessor` to build the SQL and NL simultaneously.
4. Performs robotic cleanup (regex) to ensure grammatical consistency.

---

## 🚀 Usage

### Command Line Arguments
- `--count`: Number of total records to generate.
- `--mode`: Selection strategy (`random` or `serial`).

```bash
# Generate 2500 balanced records
python generator.py --count 2500 --mode serial
```

