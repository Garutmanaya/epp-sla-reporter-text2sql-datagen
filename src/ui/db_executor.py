import sqlite3
import pandas as pd
from common.config_manager import ConfigManager

def run_query(sql: str):
    """Executes SQL against the local sqlite file and returns a DataFrame."""
    cfg = ConfigManager()
    db_path = cfg.get_versioned_db_path()
    
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        return f"SQL Error: {str(e)}"
