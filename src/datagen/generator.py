import json
import re
import os
import argparse
import sqlite3
from datagen.config_utils import ConfigUtils
from datagen.templates import SQL_TEMPLATES, get_from_clause
from datagen.filters import FilterProcessor
from common.config_manager import ConfigManager 

# ==============================================================================
# QUERY ENGINE (The Glue)
# ==============================================================================

class QueryEngine:
    def __init__(self, mode="random"):
        """
        Initializes the engine with the utility and processor.
        :param mode: 'random' or 'serial' for data selection.
        """
        self.utils = ConfigUtils(mode=mode)
        self.processor = FilterProcessor(self.utils)
        self.cfg = ConfigManager()


    # ==============================================================================
    # SCHEMA DEFINITION (tables.json)
    # ==============================================================================
    def save_tables_json(self,output_path):
        """Generates the mandatory tables.json for Spider-style training."""
        tables_data = [{
            "db_id": "epp_registry",
            "table_names_original": ["epp_sla", "epp_client", "epp_release"],
            "table_names": ["epp sla", "epp client", "epp release"],
            "column_names_original": [
                [-1, "*"], [0, "date"], [0, "hour"], [0, "command"], [0, "tld"], [0, "response_time"],
                [0, "result"], [0, "volume"], [0, "client_name"], [0, "failed_reason"],
                [1, "client_name"], [1, "client_ip_version"], [1, "client_group"], [1, "client_location"],
                [2, "release_name"], [2, "release_start"], [2, "release_end"], [2, "release_location"]
            ],
            "column_names": [
                [-1, "all"], [0, "date"], [0, "hour"], [0, "command"], [0, "tld"], [0, "response time"],
                [0, "result"], [0, "volume"], [0, "client name"], [0, "failed reason"],
                [1, "client name"], [1, "client ip version"], [1, "client group"], [1, "client location"],
                [2, "release name"], [2, "release start"], [2, "release end"], [2, "release location"]
            ],
            "column_types": ["text", "text", "number", "text", "text", "number", "text", "number", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text"],
            "primary_keys": [1, 10, 14],
            "foreign_keys": [[8, 10]]
        }]
        with open(output_path, "w") as f:
            json.dump(tables_data, f, indent=2)
        print(f"Schema File saved to: {output_path}")

    def validate_sql(self, sql):
        """
        Validates SQL syntax against an in-memory SQLite schema.
        Uses EXPLAIN QUERY PLAN to check validity without executing.
        """
        try:
            conn = sqlite3.connect(":memory:")
            cursor = conn.cursor()
            # Initialize the EPP Schema for validation
            cursor.executescript("""
            CREATE TABLE epp_sla (
                date TEXT, hour INTEGER, command TEXT, tld TEXT,
                response_time REAL, result TEXT, volume INTEGER,
                client_name TEXT, failed_reason TEXT
            );
            CREATE TABLE epp_client (
                client_name TEXT PRIMARY KEY, client_ip_version TEXT,
                client_group TEXT, client_location TEXT
            );
            CREATE TABLE epp_release (
                release_name TEXT PRIMARY KEY, release_start TEXT,
                release_end TEXT, release_location TEXT
            );
            """)
            cursor.execute("EXPLAIN QUERY PLAN " + sql)
            return True
        except Exception:
            return False


    def generate_sample(self, template, active_filters):
        """
        Orchestrates the creation of a single SQL/NL pair.
        """
        # 1. Get Base Metric
        metric = self.utils.get_metric()

        # --- PATTERN SEARCH FIX START ---
        # If the template is a pattern search, we want raw records or specific logic 
        # rather than random numeric aggregates like AVG() or SUM().
        is_pattern = template["id"] == "pattern_search"
        base_metric_sql = "*" if is_pattern else metric["sql"]
        base_metric_nl = "records" if is_pattern else metric["nl"]
        # --- PATTERN SEARCH FIX END ---
        
        # 2. Initialize States as per requirement
        sql_parts = {
            "select": [base_metric_sql],
            "where": [],
            "group": [],
            "order": [],
            "having": "",
            "limit": ""
        }
        nl_parts = {
            "metric": base_metric_nl,
            "filters": [],
            "time": "",
            "group": "",
            "ts_prefix": "",
            "order_prefix": "",
            "pattern_val": "" # Track pattern string (e.g., 'ADD') for NL
        }

        # 3. Apply Filter Pipeline (Order matters for SQL logic)
        # We pass the states through each filter function. 
        # Functions internally check if they should run based on 'active_filters'.
        sql_parts, nl_parts = self.processor.apply_group_filter(active_filters, template, sql_parts, nl_parts)
        sql_parts, nl_parts = self.processor.apply_value_filter(active_filters, template, sql_parts, nl_parts)
        sql_parts, nl_parts = self.processor.apply_time_filter(active_filters, sql_parts, nl_parts)
        sql_parts, nl_parts = self.processor.apply_ts_filter(active_filters, sql_parts, nl_parts)
        sql_parts, nl_parts = self.processor.apply_pattern_filter(active_filters, template, sql_parts, nl_parts)
        
        # Apply metric-dependent filters (Order and Having)
        sql_parts, nl_parts = self.processor.apply_having_filter(active_filters, metric["sql"], sql_parts, nl_parts)
        # Added template argument to handle dynamic limits per template
        sql_parts, nl_parts = self.processor.apply_order_filter(active_filters, template, metric["sql"], sql_parts, nl_parts)

        # 4. Assemble SQL
        from_clause = get_from_clause(template)
        sql = f"SELECT {', '.join(sql_parts['select'])} FROM {from_clause}"
        
        if sql_parts["where"]:
            sql += " WHERE " + " AND ".join(sql_parts["where"])
        if sql_parts["group"]:
            sql += " GROUP BY " + ", ".join(sql_parts["group"])
        if sql_parts["having"]:
            sql += " " + sql_parts["having"]
        if sql_parts["order"]:
            sql += " ORDER BY " + ", ".join(sql_parts["order"])
        # Uncommented to ensure LIMIT is actually appended to the final SQL
        if sql_parts["limit"]:
            sql += " " + sql_parts["limit"]

        # 1. Extract limit if it exists in the SQL state to avoid KeyError
        # We parse the number out of the "LIMIT 5" string for NL formatting
        limit_val = ""
        if sql_parts["limit"]:
            limit_val = sql_parts["limit"].replace("LIMIT ", "").strip()

        # 2. Assemble NL (Step 1 Robotic Phrasing)
        # Handle fallback for group_col in pattern search to ensure NL is meaningful
        g_col_nl = nl_parts["group"] if nl_parts["group"] else (template["group_cols"][0]["nl"] if "group_cols" in template else "")

        # We provide m_nl, group_col, filters, limit, and pattern_val to satisfy all potential template placeholders
        try:
            # Prepare the filter string once
            filter_str = ", ".join(nl_parts["filters"]) if nl_parts["filters"] else ""
            
            main_nl = template["nl"][0].format(
                m_nl=nl_parts["metric"],
                group_col=g_col_nl,
                filters=filter_str,
                limit=limit_val,
                pattern_val=nl_parts.get("pattern_val", "ADD") # Fix for pattern_search
            )
        except KeyError as e:
            # Fallback if other unexpected keys appear in templates
            print(f"Warning: Template {template['id']} missing key in format: {e}")
            main_nl = template["nl"][0]

        # 5. Final NL Decoration (Step 1 Robotic Phrasing)
        # Format: [Order Prefix] [TS Prefix] [Core Metric/Group/Filters] [Time]
        full_nl = main_nl
        if nl_parts["ts_prefix"]:
            full_nl = f"{nl_parts['ts_prefix']} of {full_nl}"
        if nl_parts["order_prefix"]:
            # Note: Depending on template, 'top N' might already be in main_nl or needs prefixing
            if "top" not in full_nl.lower():
                full_nl = f"{nl_parts['order_prefix']} {full_nl}"
        if nl_parts["time"]:
            full_nl = f"{full_nl} during {nl_parts['time']}"


        # Clean up double spaces and grammatical artifacts
        full_nl = re.sub(r"\s+", " ", full_nl).strip()
        full_nl = full_nl.replace("for during", "during").replace("of for", "for").rstrip("for").strip()

        return full_nl, sql

# ==============================================================================
# EXECUTION & SAVE LOGIC
# ==============================================================================

def run_generation_v1(record_count, mode):
    engine = QueryEngine(mode=mode)
    dataset = []
    
    # Calculate weights
    enabled_templates = [t for t in SQL_TEMPLATES if t.get("enabled", True)]
    total_template_weight = sum(t["weight"] for t in enabled_templates)

    print(f"Starting generation: {record_count} records in {mode} mode...")

    for template in enabled_templates:
        # Determine how many records for this specific template
        template_quota = int((template["weight"] / total_template_weight) * record_count)
        
        for mode_cfg in template["filter_modes"]:
            # Determine how many records for this specific filter combination
            mode_quota = int((mode_cfg["weight"] / 100) * template_quota)
            
            for _ in range(mode_quota):
                nl, sql = engine.generate_sample(template, mode_cfg["filters"])
                dataset.append({
                    "db_id": "epp_registry",
                    "template_id": template["id"],
                    "question": nl,
                    "query": sql
                })

    # Save to file
    output_dir = engine.cfg.get_versioned_data_path()
    #output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "train.json")
    
    with open(file_path, "w") as f:
        json.dump(dataset, f, indent=2)
    
    print(f"Success! Generated {len(dataset)} records.")
    print(f"File saved to: {file_path}")


def run_generation_v2(record_count, mode):
    engine = QueryEngine(mode=mode)
    dataset = []
    failed_log = []  # To track validation failures
    
    
    # Calculate weights
    enabled_templates = [t for t in SQL_TEMPLATES if t.get("enabled", True)]
    total_template_weight = sum(t["weight"] for t in enabled_templates)

    print(f"Starting generation: {record_count} records in {mode} mode...")

    for template in enabled_templates:
        # Determine how many records for this specific template
        template_quota = int((template["weight"] / total_template_weight) * record_count)
        
        for mode_cfg in template["filter_modes"]:
            # Determine how many records for this specific filter combination
            mode_quota = int((mode_cfg["weight"] / 100) * template_quota)
            
            generated = 0
            attempts = 0
            max_attempts = mode_quota * 3  # Retry limit for validation
            
            # Use a while loop to ensure we meet quota with valid SQL
            while generated < mode_quota and attempts < max_attempts:
                attempts += 1
                nl, sql = engine.generate_sample(template, mode_cfg["filters"])
                
                # Validation Logic
                if engine.validate_sql(sql):
                    dataset.append({
                        "db_id": "epp_registry",
                        "template_id": template["id"],
                        "question": nl,
                        "query": sql
                    })
                    generated += 1
                else:
                    failed_log.append({
                        "template_id": template["id"],
                        "filters": mode_cfg["filters"],
                        "sql": sql,
                        "error": "Validation failed"
                    })

            # Force Fill Fallback: If we couldn't generate enough valid SQL
            while generated < mode_quota:
                nl, sql = engine.generate_sample(template, mode_cfg["filters"])
                dataset.append({
                    "db_id": "epp_registry",
                    "template_id": template["id"],
                    "question": nl,
                    "query": sql
                })
                failed_log.append({
                    "template_id": template["id"],
                    "sql": sql,
                    "forced": True
                })
                generated += 1

    # Define paths
    output_dir = engine.cfg.get_versioned_data_path()
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, "train.json")
    failed_file_path = os.path.join(output_dir, "train_failed.json")
    tables_json_file_path = os.path.join(output_dir, "tables.json")

    # Save tables.json file 
    engine.save_tables_json(tables_json_file_path)

    # Save successful dataset
    with open(file_path, "w") as f:
        json.dump(dataset, f, indent=2)
        
    # Save failed logs
    if failed_log:
        with open(failed_file_path, "w") as f:
            json.dump(failed_log, f, indent=2)
    
    print(f"Success! Generated {len(dataset)} records.")
    print(f"Failed! {len(failed_log)} records failed in validation.")
    print(f"File saved to: {file_path}")
    if failed_log:
        print(f"Failures logged to: {failed_file_path}")


def run_generation(record_count, mode):
    engine = QueryEngine(mode=mode)
    dataset = []
    failed_log = []
    
    enabled_templates = [t for t in SQL_TEMPLATES if t.get("enabled", True)]
    total_template_weight = sum(t["weight"] for t in enabled_templates)

    print(f"Starting generation: {record_count} records in {mode} mode...")

    for t_idx, template in enumerate(enabled_templates):
        # 1. Calculate Template Quota
        # If it's the last template, take all remaining records to hit record_count exactly
        if t_idx == len(enabled_templates) - 1:
            template_quota = record_count - len(dataset)
        else:
            template_quota = int((template["weight"] / total_template_weight) * record_count)
        
        current_template_generated = 0
        for m_idx, mode_cfg in enumerate(template["filter_modes"]):
            # 2. Calculate Mode Quota
            # If it's the last mode in this template, take the remainder of the template_quota
            if m_idx == len(template["filter_modes"]) - 1:
                mode_quota = template_quota - current_template_generated
            else:
                mode_quota = int((mode_cfg["weight"] / 100) * template_quota)
            
            generated_for_mode = 0
            attempts = 0
            max_attempts = mode_quota * 5 
            
            # 3. Validation Loop
            while generated_for_mode < mode_quota and attempts < max_attempts:
                attempts += 1
                nl, sql = engine.generate_sample(template, mode_cfg["filters"])
                
                if engine.validate_sql(sql):
                    dataset.append({
                        "db_id": "epp_registry",
                        "template_id": template["id"],
                        "question": nl,
                        "query": sql
                    })
                    generated_for_mode += 1
                    current_template_generated += 1
                else:
                    failed_log.append({"template_id": template["id"], "sql": sql})

            # 4. Force Fill (If validation failed too many times)
            while generated_for_mode < mode_quota:
                nl, sql = engine.generate_sample(template, mode_cfg["filters"])
                dataset.append({
                    "db_id": "epp_registry",
                    "template_id": template["id"],
                    "question": nl,
                    "query": sql,
                    "validation_skipped": True
                })
                generated_for_mode += 1
                current_template_generated += 1

    # Save logic
    output_dir = engine.cfg.get_versioned_data_path()
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, "train.json")
    failed_file_path = os.path.join(output_dir, "train_failed.json")
    tables_json_file_path = os.path.join(output_dir, "tables.json")
    print(tables_json_file_path)
    # Save tables.json file 
    engine.save_tables_json(tables_json_file_path)

    with open(file_path, "w") as f:
        json.dump(dataset, f, indent=2)
    
    if failed_log:
        with open(failed_file_path, "w") as f:
            json.dump(failed_log, f, indent=2)
    
    print(f"Success! Generated {len(dataset)} records.")
    print(f"Failed! {len(failed_log)} records failed in validation.")
    print(f"File saved to: {file_path}")
    if failed_log:
        print(f"Failures logged to: {failed_file_path}")

# ==============================================================================
# MAIN TEST BLOCK
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL/NL Dataset")
    parser.add_argument("--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--mode", type=str, default="random", choices=["random", "serial"], help="Data selection mode")
    
    args = parser.parse_args()
    
    # Fix for argparse passing string choices back to the generator
    run_generation(args.count, str(args.mode))