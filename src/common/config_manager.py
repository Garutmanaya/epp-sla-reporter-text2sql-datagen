import json
import os
import sys
from pathlib import Path

class ConfigManager:
    """
    Utility class to manage global configurations for the EPP SLA Reporter.
    Automatically detects project root and manages versioned paths.
    """
    
    def __init__(self, config_rel_path="config/main.config.json"):
        # 1. Identify Root Directory dynamically
        self.root_dir = self._find_project_root()
        
        # 2. Set Config Path (Absolute)
        # Defaults to [ROOT]/config/main.config.json
        self.config_path = self.root_dir / config_rel_path
        
        # 3. Load actual data
        self.config = self._load_config()

    def _find_project_root(self, marker="pyproject.toml"):
        """
        Traverses upwards from the current file location to find the project root 
        based on the existence of a marker file (e.g., pyproject.toml or .git).
        """
        current_path = Path(__file__).resolve()
        
        # Iterate through parents until marker file is found
        for parent in [current_path] + list(current_path.parents):
            if (parent / marker).exists():
                return parent
            
        # Fallback to the current working directory if marker not found
        return Path.cwd()

    def _load_config(self):
        """Reads the JSON config file from disk with error handling."""
        if not self.config_path.exists():
            print(f"Warning: Configuration file not found at {self.config_path}")
            # Return a default structure to prevent total crash
            return {"active_version": "v1", "paths": {"data": "data/", "model": "models/"}}
            
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse config JSON: {e}")
            return {}

    @property
    def version(self):
        """Returns the active version string."""
        return self.config.get("active_version", "v1")


    @property
    def database_name(self):
        """Returns the active version string."""
        return self.config.get("database_name", "epp_registry.db")

    @property
    def s3_enabled(self):
        """Returns boolean status of S3 integration."""
        return self.config.get("s3", {}).get("enabled", False)

    @property
    def pipeline_params(self):
        """Returns the entire pipeline configuration dictionary."""
        return self.config.get("pipeline", {})

    @property
    def training_mode(self):
        """Returns lora or full as defined in config."""
        return self.pipeline_params.get("training_mode", "lora")

    @property
    def model_size(self):
        """Returns base or small as defined in config."""
        return self.pipeline_params.get("google_flan_t5_model_size", "base")

    @property
    def datagen_count(self):
        """Returns datagen record count from pipleline config."""
        return self.pipeline_params.get("datagen_count", "1000")

    @property
    def datagen_mode(self):
        """Returns datagen mode either serial or random as defined in config."""
        # Fixed: now looks for datagen_mode
        return self.pipeline_params.get("datagen_mode", "random")
    
    @property
    def datagen_augment_mode(self):
        """Returns datagen augment mode (Natural, Short, Complex, All) as defined in config."""
        return self.pipeline_params.get("datagen_augment_mode", "Natural")

    def get_path(self, path_key):
        """
        Retrieves a local path from config and makes it absolute relative to project root.
        Creates the directory if it doesn't exist.
        """
        relative_path = self.config.get("paths", {}).get(path_key, f"{path_key}/")
        full_path = self.root_dir / relative_path
        
        # Ensure the directory exists
        full_path.mkdir(parents=True, exist_ok=True)
        return full_path

    def get_versioned_data_path(self):
        """Returns the absolute path to [ROOT]/data/[VERSION]/."""
        base_data = self.get_path("data")
        versioned_path = base_data / self.version
        versioned_path.mkdir(parents=True, exist_ok=True)
        return versioned_path


    def get_versioned_model_path(self):
        """
        Returns the absolute path to [ROOT]/models/[VERSION]/.
        Useful for saving specific model checkpoints per dataset version.
        """
        base_model = self.get_path("model")
        versioned_path = base_model / self.version
        versioned_path.mkdir(parents=True, exist_ok=True)
        return versioned_path 
    
    def get_versioned_db_path(self):
        """Returns absolute path to [ROOT]/hub/databases/[VERSION]/[DB_NAME]"""
        # 1. Get the base path 'hub/databases/' (absolute)
        base_db_dir = self.get_path("db")
        
        # 2. Append version 'v1'
        versioned_dir = base_db_dir / self.version
        versioned_dir.mkdir(parents=True, exist_ok=True)
        
        # 3. Get filename from property
        db_name = self.database_name
        
        # 4. Return the full absolute Path object
        return (versioned_dir / db_name).resolve() 
     
    def get_versioned_db_path_v1(self):
        """Returns absolute path to [ROOT]/hub/databases/[VERSION]/[DB_NAME]"""
        base_db = self.get_path("db")
        versioned_dir = base_db / self.version
        versioned_dir.mkdir(parents=True, exist_ok=True)
        
        db_name = self.config.get("database_name", "epp_registry.db")
        return versioned_dir / db_name 
    
    def __repr__(self):
        return f"<ConfigManager(root={self.root_dir}, version={self.version})>"


# --- Main Block for Testing/Stand-alone Utility ---

if __name__ == "__main__":
    print("--- ConfigManager Debug Info ---")
    
    # Initialize Manager
    try:
        manager = ConfigManager()
        
        print(f"Project Root:    {manager.root_dir}")
        print(f"Config Path:     {manager.config_path}")
        print(f"Active Version:  {manager.version}")
        print(f"S3 Enabled:      {manager.s3_enabled}")
        
        # Test Path Resolution
        data_path = manager.get_versioned_data_path()
        model_path = manager.get_versioned_model_path()
        
        print(f"Data Directory:  {data_path}")
        print(f"Model Directory: {model_path}")
        
        print("\n✅ Configuration loaded successfully.")
        
    except Exception as e:
        print(f"\n❌ Error during config initialization: {e}")
        sys.exit(1)
