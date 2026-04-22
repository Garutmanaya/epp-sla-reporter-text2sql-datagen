import random
import json
import re
import os

# ==============================================================================
# 1. DATABASE CONFIGURATION & SCHEMA VALUES
# ==============================================================================
# These values are used to populate WHERE clauses with realistic data.
DB_ID = "epp_registry"

COLUMN_VALUES = {
    "epp_sla": {
        "command": ["'ADD-DOMAIN'", "'CHECK-DOMAIN'", "'MOD-DOMAIN'", "'RENEW-DOMAIN'", "'TRANSFER-DOMAIN'"],
        "tld": ["'com'", "'net'", "'io'", "'org'", "'info'", "'biz'"],
        "result": ["'SUCCESS'", "'FAILURE'", "'TIMEOUT'", "'ERROR'"],
        "failed_reason": ["'CONNECTION_TIMEOUT'", "'AUTH_FAILED'", "'INVALID_TLD'", "'QUOTA_EXCEEDED'"]
    },
    "epp_client": {
        "client_location": ["'USA'", "'EU'", "'ASIA'", "'AUSTRALIA'", "'LATAM'", "'AFRICA'"],
        "client_group": ["'Gold'", "'Silver'", "'Internal'", "'VIP'", "'Reseller'"],
        "client_ip_version": ["'IPv4'", "'IPv6'"]
    },
    "epp_release": {
        "release_name": ["'v1.0'", "'v2.1'", "'Emergency_Patch'", "'Spring_Update'", "'Q4_Rollout'"],
        "release_location": ["'Global'", "'Regional'", "'Staging'"]
    }
}

# ==============================================================================
# 2. METRICS (The "What" of the Query)
# ==============================================================================
# Maps Natural Language phrases to SQL Aggregate functions.
METRICS = [
    {"id": "volume", "sql": "SUM(epp_sla.volume)", "nl": "total volume"},
    {"id": "latency", "sql": "AVG(epp_sla.response_time)", "nl": "average latency"},
    {"id": "distinct_command", "sql": "COUNT(DISTINCT epp_sla.command)", "nl": "unique command count"},
    {"id": "row_count", "sql": "COUNT(*)", "nl": "record count"},
    {"id": "max_latency", "sql": "MAX(epp_sla.response_time)", "nl": "maximum response time"},
    {"id": "min_latency", "sql": "MIN(epp_sla.response_time)", "nl": "minimum response time"}
]

# ==============================================================================
# 3. TIME FILTERS (The "When" of the Query)
# ==============================================================================
# Expanded list of SQLite-compatible date filters for diverse temporal training.
TIME_FILTERS = [
    # Fixed Points
    (["today"], "epp_sla.date = DATE('now')"),
    (["yesterday"], "epp_sla.date = DATE('now', '-1 day')"),
    (["two days ago"], "epp_sla.date = DATE('now', '-2 days')"),
    
    # Relative Ranges
    (["past 7 days"], "epp_sla.date >= DATE('now', '-7 days')"),
    (["past 30 days"], "epp_sla.date >= DATE('now', '-30 days')"),
    (["past 90 days"], "epp_sla.date >= DATE('now', '-90 days')"),
    
    # Calendar Blocks
    (["this month"], "epp_sla.date >= DATE('now', 'start of month')"),
    (["previous month"], "epp_sla.date BETWEEN DATE('now','start of month','-1 month') AND DATE('now','start of month','-1 day')"),
    (["this quarter"], "epp_sla.date >= DATE('now', 'start of month', '-3 months')"),
    (["this year"], "epp_sla.date >= DATE('now', 'start of year')"),
    (["previous year"], "epp_sla.date BETWEEN DATE('now','start of year','-1 year') AND DATE('now','start of year','-1 day')")
]
