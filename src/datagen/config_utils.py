import random
# Importing from your existing config.py
from config import METRICS, TIME_FILTERS, COLUMN_VALUES

class ConfigUtils:
    def __init__(self, mode="random"):
        """
        Initializes the utility with a selection mode.
        :param mode: "random" for stochastic picking, "serial" for round-robin.
        """
        self.mode = mode.lower()
        # Internal counters for serial tracking
        self._metric_idx = 0
        self._time_idx = 0
        # Tracks the last used column index for each table to ensure serial coverage
        self._col_indices = {table: 0 for table in COLUMN_VALUES}

    def _get_next_index(self, current_idx, collection):
        """Helper to increment index and wrap around the collection size."""
        idx = current_idx % len(collection)
        return idx, current_idx + 1

    def get_metric(self):
        """Returns a metric dictionary based on the initialized mode."""
        if self.mode == "serial":
            idx, self._metric_idx = self._get_next_index(self._metric_idx, METRICS)
            return METRICS[idx]
        return random.choice(METRICS)

    def get_time_filter(self):
        """Returns a time filter tuple (nl_variants, sql_condition) based on the mode."""
        if self.mode == "serial":
            idx, self._time_idx = self._get_next_index(self._time_idx, TIME_FILTERS)
            return TIME_FILTERS[idx]
        return random.choice(TIME_FILTERS)

    def get_column_value(self, table_name):
        """
        Returns (column_name, value) for a specific table.
        In serial mode, it rotates through columns.
        """
        if table_name not in COLUMN_VALUES:
            return None, None
        
        cols = list(COLUMN_VALUES[table_name].keys())
        
        if self.mode == "serial":
            c_idx, self._col_indices[table_name] = self._get_next_index(self._col_indices[table_name], cols)
            col_name = cols[c_idx]
            # Picking a random value for the sequentially chosen column
            val = random.choice(COLUMN_VALUES[table_name][col_name])
            return col_name, val
            
        col_name = random.choice(cols)
        val = random.choice(COLUMN_VALUES[table_name][col_name])
        return col_name, val

# ==============================================================================
# MAIN TEST BLOCK
# ==============================================================================
if __name__ == "__main__":
    print("--- Testing ConfigUtils: SERIAL MODE ---")
    serial_utils = ConfigUtils(mode="serial")
    
    # Test Metric Rotation
    print("\n1. Metric Rotation (Serial):")
    for i in range(len(METRICS) + 1):
        m = serial_utils.get_metric()
        print(f"   Call {i+1}: {m['nl']}")

    # Test Time Filter Rotation
    print("\n2. Time Filter Rotation (Serial):")
    for i in range(3):
        t = serial_utils.get_time_filter()
        print(f"   Call {i+1}: {t[0][0]}")

    # Test Column Rotation for epp_sla
    print("\n3. Column Rotation for 'epp_sla' (Serial):")
    for i in range(5):
        col, val = serial_utils.get_column_value("epp_sla")
        print(f"   Call {i+1}: Column='{col}', Value={val}")

    print("\n" + "="*40 + "\n")

    print("--- Testing ConfigUtils: RANDOM MODE ---")
    random_utils = ConfigUtils(mode="random")
    
    print("\n1. Random Metric Samples:")
    for i in range(3):
        m = random_utils.get_metric()
        print(f"   Sample {i+1}: {m['nl']}")