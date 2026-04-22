import random
import re
from config import TIME_FILTERS, COLUMN_VALUES, METRICS

# ==============================================================================
# 1. TIME SERIES MODES
# ==============================================================================
# Defines the specific columns used for temporal grouping in trend queries.
TS_MODES = [
    {"nl": "daily trend", "cols": ["epp_sla.date"]},
    {"nl": "hourly trend", "cols": ["epp_sla.date", "epp_sla.hour"]}
]

# ==============================================================================
# 2. FILTER PROCESSOR
# ==============================================================================

class FilterProcessor:
    """
    Handles sequential application of filters. Each method checks the 
    active_filters list before modifying the SQL or NL state.
    """

    def __init__(self, utils):
        """
        Initializes the processor with a ConfigUtils instance to handle 
        data selection (Serial/Random).
        """
        self.utils = utils

    def apply_group_filter(self, active_filters, template, sql_state, nl_state):
        """
        LOGIC: 
        1. Checks if 'group' is in active_filters and template has group_cols.
        2. Picks a dimension column (e.g., tld) from the template.
        3. Prepends column to SELECT so it appears before the metric.
        4. Adds column to GROUP BY to ensure valid SQL aggregation.
        5. Updates nl_state['group'] for phrasing like "...grouped by tld".
        """
        if "group" in active_filters and "group_cols" in template:
            g = random.choice(template["group_cols"])
            sql_state["select"].insert(0, g["sql"])
            sql_state["group"].append(g["sql"])
            nl_state["group"] = g["nl"]
        return sql_state, nl_state

    def apply_value_filter(self, active_filters, template, sql_state, nl_state):
        """
        LOGIC:
        1. Checks for 'val' in active_filters.
        2. Uses ConfigUtils to pick a column and valid value from the primary table.
        3. Appends a standard equality constraint (col = val) to the WHERE list.
        4. Formats the NL filter string (e.g., "command ADD-DOMAIN") for Step 1.
        """
        if "val" in active_filters:
            table = template["tables"][0]
            col, val = self.utils.get_column_value(table)
            if col and val:
                sql_state["where"].append(f"{table}.{col} = {val}")
                nl_col = col.replace('_', ' ')
                nl_val = val.strip("'")
                nl_state["filters"].append(f"{nl_col} {nl_val}")
        return sql_state, nl_state

    def apply_time_filter(self, active_filters, sql_state, nl_state):
        """
        LOGIC:
        1. Checks for 'time' in active_filters.
        2. Fetches a temporal constraint from ConfigUtils (e.g., date >= '...').
        3. Appends the condition to the WHERE list.
        4. Updates nl_state['time'] with the robotic variation (e.g., "today").
        """
        if "time" in active_filters:
            nl_variants, condition = self.utils.get_time_filter()
            sql_state["where"].append(condition)
            nl_state["time"] = nl_variants[0]
        return sql_state, nl_state

    def apply_ts_filter(self, active_filters, sql_state, nl_state):
        """
        LOGIC:
        1. Checks for 'ts' in active_filters.
        2. Randomly selects a trend mode (Daily or Hourly).
        3. Prepends Date/Hour columns to SELECT and GROUP BY to shift the 
           structure into a time-series format.
        4. Adds chronological ORDER BY to the SQL state.
        5. Sets a prefix in NL (e.g., "daily trend of").
        """
        if "ts" in active_filters:
            ts = random.choice(TS_MODES)
            for col in reversed(ts["cols"]):
                sql_state["select"].insert(0, col)
                sql_state["group"].insert(0, col)
            sql_state["order"].extend([f"{c} ASC" for c in ts["cols"]])
            nl_state["ts_prefix"] = ts["nl"]
        return sql_state, nl_state

    def apply_having_filter(self, active_filters, metric_sql, sql_state, nl_state):
        """
        LOGIC:
        1. Checks for 'having' in active_filters.
        2. Takes the current metric SQL (e.g., SUM(volume)) and applies a 
           threshold comparison.
        3. Stores the fragment in sql_state['having'] for later assembly.
        4. Appends "greater than 100" to the NL filter list.
        """
        if "having" in active_filters:
            threshold = 100
            sql_state["having"] = f"HAVING {metric_sql} > {threshold}"
            nl_state["filters"].append(f"greater than {threshold}")
        return sql_state, nl_state


    def apply_pattern_filter(self, active_filters, template, sql_state, nl_state):
        """
        LOGIC:
        1. Checks for 'pattern' in active_filters.
        2. Applies a LIKE operator to the chosen group column.
        3. Returns the pattern string used so the NL can be specific.
        """
        pattern_val = "ADD" # Robotic placeholder
        if "pattern" in active_filters:
            # Use the group column defined in the template for the LIKE clause
            g_col = template["group_cols"][0]["sql"]
            sql_state["where"].append(f"{g_col} LIKE '{pattern_val}%'")
            # We store the pattern value in nl_state to use in formatting
            nl_state["pattern_val"] = pattern_val
            
        return sql_state, nl_state 
    

    def apply_order_filter(self, active_filters, template, metric_sql, sql_state, nl_state):
        """
        LOGIC:
        1. Checks for 'order' in active_filters (typically for Top-N).
        2. Retrieves limit options from template (e.g., [3, 5, 10]) or defaults to [3, 5, 10].
        3. Appends the metric to the ORDER BY list in descending order.
        4. Sets the SQL LIMIT based on a random pick from available options.
        5. Updates nl_state['order_prefix'] for phrasing like "top 5...".
        """
        if "order" in active_filters:
            # Check if template defines a specific list of limit options
            limit_options = template.get("limit", [3, 5, 10])
            limit = random.choice(limit_options)
            
            # Update SQL state
            sql_state["order"].append(f"{metric_sql} DESC")
            sql_state["limit"] = f"LIMIT {limit}"
            
            # Update NL state for the robotic prefix
            nl_state["order_prefix"] = f"top {limit}"
            
        return sql_state, nl_state
    
# ==============================================================================
# 3. MAIN TEST BLOCK
# ==============================================================================
if __name__ == "__main__":
    from config_utils import ConfigUtils
    
    print("--- Testing FilterProcessor Functions with Descriptions ---")
    utils = ConfigUtils(mode="serial")
    processor = FilterProcessor(utils)
    
    # Setup initial mock state
    mock_template = {
        "tables": ["epp_sla"],
        "group_cols": [{"nl": "command", "sql": "epp_sla.command"}]
    }
    sql_s = {"select": ["SUM(volume)"], "where": [], "group": [], "order": []}
    nl_s = {"metric": "total volume", "filters": [], "time": "", "group": "", "ts_prefix": ""}
    
    # Run a pipeline simulation
    active = ["val", "ts"]
    print(f"Executing pipeline for: {active}")
    
    sql_s, nl_s = processor.apply_value_filter(active, mock_template, sql_s, nl_s)
    sql_s, nl_s = processor.apply_ts_filter(active, sql_s, nl_s)
    
    print(f"\nSQL RESULTS:")
    print(f"   SELECT: {sql_s['select']}")
    print(f"   WHERE:  {sql_s['where']}")
    print(f"   ORDER:  {sql_s['order']}")
    
    print(f"\nNL RESULTS:")
    print(f"   Prefix: {nl_s.get('ts_prefix')}")
    print(f"   Filters: {nl_s['filters']}")
    
    print("\n--- Validation Complete ---")