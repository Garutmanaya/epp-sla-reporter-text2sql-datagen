import random

# ==============================================================================
# UTILITY FUNCTIONS FOR TEMPLATES
# ==============================================================================

def get_from_clause(template):
    """
    Constructs the FROM ... JOIN ... string based on the template definition.
    Supports both standard FK joins and range-based (release) joins.
    """
    tables = template.get("tables", [])
    joins = template.get("joins", [])
    
    if not joins:
        return tables[0]
    
    from_sql = tables[0]
    for j in joins:
        if "right_table" in j:
            # Explicit join table and condition (e.g., Release joins)
            from_sql += f" JOIN {j['right_table']} ON {j['on']}"
        else:
            # Standard FK join based on left/right columns
            right_table = j["right"].split(".")[0]
            from_sql += f" JOIN {right_table} ON {j['left']} = {j['right']}"
    return from_sql

def select_filter_mode(template):
    """
    Randomly selects a filter mode based on the weights defined in the template.
    """
    modes = template.get("filter_modes", [])
    if not modes:
        return []
    
    weights = [m.get("weight", 0) for m in modes]
    selected_mode = random.choices(modes, weights=weights, k=1)[0]
    return selected_mode.get("filters", [])

# ==============================================================================
# CLEANED SQL TEMPLATES
# ==============================================================================

SQL_TEMPLATES = [
    # ---------------------------------------------------------
    # 1. BASIC METRICS (Single Table)
    # ---------------------------------------------------------
    {
        "id": "metrics_all",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} for {filters}"],
        "sql": "SELECT {m_sql} FROM epp_sla",
        "tables": ["epp_sla"],
        "apply": ["val", "time", "ts"],
        "filter_modes": [
            {"filters": [], "weight": 40},
            {"filters": ["val"], "weight": 20},
            {"filters": ["time"], "weight": 20},
            {"filters": ["val", "time"], "weight": 10},
            {"filters": ["val", "ts"], "weight": 10}
        ]
    },

    # ---------------------------------------------------------
    # 2. GROUP BY (Internal Column)
    # ---------------------------------------------------------
    {
        "id": "group_by",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} by {group_col} {filters}"],
        "sql": "SELECT {group_col_sql}, {m_sql} FROM epp_sla",
        "tables": ["epp_sla"],
        "group_cols": [
            {"nl": "command", "sql": "epp_sla.command"},
            {"nl": "tld", "sql": "epp_sla.tld"},
            {"nl": "result", "sql": "epp_sla.result"}
        ],
        "apply": ["val", "time", "group", "ts"],
        "filter_modes": [
            {"filters": ["group"], "weight": 40},
            {"filters": ["group", "val"], "weight": 30},
            {"filters": ["group", "time"], "weight": 30}
        ]
    },

    # ---------------------------------------------------------
    # 3. JOIN: SLA + CLIENT
    # ---------------------------------------------------------
    {
        "id": "metrics_client",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} across clients {group_clause} for {filters}"],
        "sql": "SELECT {m_sql} FROM {from_clause}",
        "tables": ["epp_sla", "epp_client"],
        "joins": [{"left": "epp_sla.client_name", "right": "epp_client.client_name"}],
        "group_cols": [
            {"nl": "client", "sql": "epp_client.client_name"},
            {"nl": "client location", "sql": "epp_client.client_location"},
            {"nl": "client group", "sql": "epp_client.client_group"}
        ],
        "apply": ["val", "time", "ts", "group"],   # 👈 add group
        "filter_modes": [
            {"filters": ["group"], "weight": 20},
            {"filters": ["val", "group"], "weight": 20},
            {"filters": ["val", "time", "group"], "weight": 30},
            {"filters": ["val", "ts", "group"], "weight": 30}
        ]
    },
    {
        "id": "metrics_client",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} across clients for {filters}"],
        "sql": "SELECT {m_sql} FROM {from_clause}",
        "tables": ["epp_sla", "epp_client"],
        "joins": [{"left": "epp_sla.client_name", "right": "epp_client.client_name"}],
        "apply": ["val", "time", "ts"],
        "filter_modes": [
            {"filters": [], "weight": 30},
            {"filters": ["val"], "weight": 20},
            {"filters": ["val", "time"], "weight": 30},
            {"filters": ["val", "ts"], "weight": 20}
        ]
    },
    # ---------------------------------------------------------
    # 4. JOIN: SLA + RELEASE (Date Range Join)
    # ---------------------------------------------------------
    {
        "id": "metrics_release",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} during releases {group_clause} for {filters}"],
        "sql": "SELECT {m_sql} FROM {from_clause}",
        "tables": ["epp_sla", "epp_release"],
        "joins": [
            {
                "right_table": "epp_release",
                "on": "epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end"
            }
        ],
        "group_cols": [
            {"nl": "release", "sql": "epp_release.release_name"},
            {"nl": "release location", "sql": "epp_release.release_location"}
        ],
        "apply": ["val", "time", "ts", "group"],   # 👈 add group
        "filter_modes": [
            {"filters": ["group"], "weight": 20},
            {"filters": ["val", "time", "group"], "weight": 40},
            {"filters": ["val", "ts", "group"], "weight": 40}
        ]
    },
    {
        "id": "metrics_release_old",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} during releases for {filters}"],
        "sql": "SELECT {m_sql} FROM {from_clause}",
        "tables": ["epp_sla", "epp_release"],
        "joins": [
            {
                "right_table": "epp_release",
                "on": "epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end"
            }
        ],
        "apply": ["val", "time", "ts"],
        "filter_modes": [
            {"filters": [], "weight": 40},
            {"filters": ["val", "time"], "weight": 40},
            {"filters": ["val", "ts"], "weight": 20}
        ]
    },
    # ---------------------------------------------------------
    # 5. TOP N (Ordering and Limits)
    # ---------------------------------------------------------
    {
        "id": "top_n",
        "enabled": False,
        "weight": 80,
        "nl": ["top {limit} {group_col} by {m_nl} {filters}"],
        "sql": "SELECT {group_col_sql}, {m_sql} FROM epp_sla",
        "tables": ["epp_sla"],
        "group_cols": [
            {"nl": "command", "sql": "epp_sla.command"},
            {"nl": "tld", "sql": "epp_sla.tld"}
        ],
        "limit": [3, 5, 10, 25 ],
        "apply": ["time", "group", "order"],
        "filter_modes": [
            {"filters": ["group", "time", "order"], "weight": 100}
        ]
    },

    # ---------------------------------------------------------
    # 6. THRESHOLD (HAVING Clause)
    # ---------------------------------------------------------
    {
        "id": "having_threshold",
        "enabled": True,
        "weight": 80,
        #"nl": ["{group_col} where {m_nl} is greater than threshold {filters}"],
        "nl": ["{group_col} where {m_nl} is greater than {threshold} {filters}"],
        "sql": "SELECT {group_col_sql}, {m_sql} FROM epp_sla",
        "tables": ["epp_sla"],
        "group_cols": [
            {"nl": "command", "sql": "epp_sla.command"},
            {"nl": "tld", "sql": "epp_sla.tld"}
        ],
        "apply": ["group", "having", "time", "ts"],
        "filter_modes": [
            {"filters": ["having", "group", "time"], "weight": 50},
            {"filters": ["having", "group", "ts"], "weight": 30}
          
        ]
    },

    # ---------------------------------------------------------
    # 7. TRIPLE JOIN (SLA + CLIENT + RELEASE)
    # ---------------------------------------------------------
    {
        "id": "metrics_client_release_old",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} across clients and releases for {filters}"],
        "sql": "SELECT {m_sql} FROM {from_clause}",
        "tables": ["epp_sla", "epp_client", "epp_release"],
        "joins": [
            {"left": "epp_sla.client_name", "right": "epp_client.client_name"},
            {
                "right_table": "epp_release",
                "on": "epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end"
            }
        ],
        "apply": ["val", "time", "ts"],
        "filter_modes": [
            {"filters": ["time"], "weight": 50},
            {"filters": ["val", "ts"], "weight": 50}
        ]
    },
    {
        "id": "metrics_client_release",
        "enabled": False,
        "weight": 100,
        "nl": ["{m_nl} across clients and releases {group_clause} for {filters}"],
        "sql": "SELECT {m_sql} FROM {from_clause}",
        "tables": ["epp_sla", "epp_client", "epp_release"],
        "joins": [
            {"left": "epp_sla.client_name", "right": "epp_client.client_name"},
            {
                "right_table": "epp_release",
                "on": "epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end"
            }
        ],
        "group_cols": [
            {"nl": "client", "sql": "epp_client.client_name"},
            {"nl": "client location", "sql": "epp_client.client_location"},
            {"nl": "release", "sql": "epp_release.release_name"},
            {"nl": "release location", "sql": "epp_release.release_location"}
        ],
        "apply": ["val", "time", "ts", "group"],   # 👈 key addition
        "filter_modes": [
            {"filters": ["time", "group"], "weight": 40},
            {"filters": ["val", "ts", "group"], "weight": 60}
        ]
    },
    # ---------------------------------------------------------
    # 8. PATTERN SEARCH (LIKE Clause)
    # ---------------------------------------------------------
    {
        "id": "pattern_search",
        "enabled": False,
        "weight": 60,
        "nl": ["records where {group_col} starts with {pattern_val} {filters}"],
        "sql": "SELECT * FROM epp_sla", # Pattern search usually implies raw records
        "tables": ["epp_sla"],
        "group_cols": [
            {"nl": "command", "sql": "epp_sla.command"},
            {"nl": "tld", "sql": "epp_sla.tld"},
            {"nl": "result", "sql": "epp_sla.result"},
            {"nl": "failed reason", "sql": "epp_sla.failed_reason"}
        ],   
        "apply": ["pattern", "time"],
        "filter_modes": [
            {"filters": ["pattern"], "weight": 50},
            {"filters": ["pattern", "time"], "weight": 50}
        ]
    },
]


# ==============================================================================
# MAIN TEST BLOCK
# ==============================================================================
if __name__ == "__main__":
    print("--- Testing templates.py Utilities ---")
    
    # 1. Test FROM Clause Generation
    print("\n1. Testing FROM Clause Construction:")
    for temp in SQL_TEMPLATES:
        if temp["enabled"]:
            from_clause = get_from_clause(temp)
            print(f"   ID: {temp['id']:<25} | FROM: {from_clause}")

    # 2. Test Filter Mode Selection Distribution
    print("\n2. Testing Filter Mode Selection (Distribution Check):")
    test_temp = SQL_TEMPLATES[0] # metrics_all
    stats = {}
    
    test_runs = 1000
    for _ in range(test_runs):
        filters = tuple(select_filter_mode(test_temp))
        stats[filters] = stats.get(filters, 0) + 1
    
    print(f"   Template: {test_temp['id']} | Runs: {test_runs}")
    for filter_set, count in stats.items():
        percentage = (count / test_runs) * 100
        print(f"   Filters: {str(filter_set):<25} | Actual: {percentage:>4.1f}%")

    print("\n--- Validation Complete ---")
