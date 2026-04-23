from __future__ import annotations
import random
import sqlite3
import os
import logging
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from pathlib import Path
from src.common.config_manager import ConfigManager

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DBGenerator")

class EPPDatabaseGenerator:
    def __init__(self):
        self.config_manager = ConfigManager()
        self.config = self.config_manager.get_config()
        
        # Resolve path: hub/databases/v1/
        self.version = self.config.get("active_version", "v1")
        self.db_dir = os.path.join(self.config["paths"]["db"], self.version)
        self.db_name = self.config.get("datbase_name", "epp_registry.db") # Using key from your config
        self.db_path = Path(os.path.join(self.db_dir, self.db_name))
        
        self.sla_min_records = 10_000

    def create_tables(self, conn: sqlite3.Connection) -> None:
        """Creates lowercase tables as per EPP standards."""
        cursor = conn.cursor()
        
        # epp_sla table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS epp_sla (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, hour INTEGER, command TEXT, tld TEXT,
                response_time REAL, result TEXT, volume INTEGER,
                client_name TEXT, failed_reason TEXT
            );
        """)
        
        # epp_client table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS epp_client (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT UNIQUE, client_ip_version TEXT,
                client_group TEXT, client_location TEXT
            );
        """)
        
        # epp_release table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS epp_release (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                release_name TEXT, release_start TEXT,
                release_end TEXT, release_location TEXT
            );
        """)
        conn.commit()
        logger.info("Tables created successfully (lowercase).")

    def drop_tables(self, conn: sqlite3.Connection) -> None:
        """Drops all existing tables for a fresh reset."""
        cursor = conn.cursor()
        for table in ["epp_sla", "epp_client", "epp_release"]:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()

    def _table_row_count(self, conn: sqlite3.Connection, table_name: str) -> int:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return int(cursor.fetchone()[0])

    def seed_clients(self, conn: sqlite3.Connection) -> None:
        """Seeds the client registry."""
        rows = [
            ("AtlasRegistrar", "IPv4", "Premium", "Toronto"),
            ("NorthStarDomains", "IPv6", "Standard", "New York"),
            ("ZenithNames", "DualStack", "Enterprise", "Chicago"),
            ("BlueHarborRegistry", "IPv4", "Standard", "Dallas"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO epp_client (client_name, client_ip_version, client_group, client_location) VALUES (?, ?, ?, ?)",
            rows,
        )
        conn.commit()

    def seed_releases(self, conn: sqlite3.Connection) -> None:
        """Seeds maintenance/release windows for the last 3 months."""
        release_locations = ["Primary-DC", "Secondary-DC", "DR-DC"]
        rows = []
        # Date range logic
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90)
        
        for idx in range(1, 5):
            start_ordinal = random.randint(start_date.toordinal(), (end_date - timedelta(days=15)).toordinal())
            start_dt = datetime.combine(date.fromordinal(start_ordinal), datetime.min.time()).replace(hour=random.randint(20, 23))
            end_dt = start_dt + timedelta(days=random.randint(1, 3)) # Releases usually last a few days
            
            rows.append((
                f"Release-{start_dt.strftime('%Y%m')}-{idx:02d}",
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(release_locations)
            ))
        
        conn.executemany(
            "INSERT INTO epp_release (release_name, release_start, release_end, release_location) VALUES (?, ?, ?, ?)",
            rows,
        )
        conn.commit()

    def seed_sla(self, conn: sqlite3.Connection) -> None:
        """Generates bulk SLA performance data."""
        commands = ["create", "renew", "transfer", "update", "delete", "check", "info"]
        tlds = ["com", "net", "org", "ai", "co"]
        clients = ["AtlasRegistrar", "NorthStarDomains", "ZenithNames", "BlueHarborRegistry"]
        reasons = ["Timeout", "RateLimit", "ValidationError", "ConnectionReset"]

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90)
        
        rows = []
        for _ in range(self.sla_min_records):
            day_offset = random.randint(0, 90)
            dt = datetime.combine(start_date + timedelta(days=day_offset), datetime.min.time()) + timedelta(hours=random.randint(0, 23))
            
            result = "Failure" if random.random() < 0.15 else "Success"
            rows.append((
                dt.strftime("%Y-%m-%d"),
                dt.hour,
                random.choice(commands),
                random.choice(tlds),
                round(random.uniform(0.05, 2.5), 3),
                result,
                random.randint(1, 1000),
                random.choice(clients),
                random.choice(reasons) if result == "Failure" else None
            ))

        conn.executemany(
            "INSERT INTO epp_sla (date, hour, command, tld, response_time, result, volume, client_name, failed_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()

    def initialize(self, reset: bool = False):
        """Main entry point to build the database file."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            if reset:
                logger.info(f"Resetting database at {self.db_path}")
                self.drop_tables(conn)
            
            self.create_tables(conn)
            
            if self._table_row_count(conn, "epp_client") == 0:
                self.seed_clients(conn)
            if self._table_row_count(conn, "epp_release") == 0:
                self.seed_releases(conn)
            if self._table_row_count(conn, "epp_sla") == 0:
                logger.info(f"Seeding {self.sla_min_records} records...")
                self.seed_sla(conn)
        
        logger.info(f"Database initialized successfully at: {self.db_path}")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Drop and recreate tables")
    args = parser.parse_args()
    
    gen = EPPDatabaseGenerator()
    gen.initialize(reset=args.reset)
