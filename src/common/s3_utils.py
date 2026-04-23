import boto3
import os
import logging
import argparse
from pathlib import Path
from common.config_manager import ConfigManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("S3Utils")

class S3Manager:
    """
    Manages versioned uploads and downloads of project assets (data, models, databases)
    to and from Amazon S3 based on main.config.json.
    """
    def __init__(self):
        self.config_manager = ConfigManager()
        self.config = self.config_manager.get_config()
        
        # S3 specific configuration
        self.s3_cfg = self.config.get("s3", {})
        self.enabled = self.s3_cfg.get("enabled", False)
        self.version = self.config.get("active_version", "v1")
        
        # Path mapping for local directories (e.g., hub/data/, hub/artifacts/, hub/databases/)
        self.path_map = self.config.get("paths", {})
        
        if self.enabled:
            # Initialize boto3 client using region from config
            self.s3 = boto3.client('s3', region_name=self.s3_cfg.get('region', 'us-east-1'))
            self.bucket = self.s3_cfg.get('bucket')
            self.prefix = self.s3_cfg.get('prefix', 'data')
        else:
            logger.warning("S3 is disabled in main.config.json. Operations will be skipped.")

    def _get_local_path(self, category: str) -> str:
        """
        Resolves local path based on category and active version.
        Example: category 'db' -> 'hub/databases/v1'
        """
        base = self.path_map.get(category, f"{category}/")
        return os.path.join(base, self.version)

    def _get_s3_key(self, relative_path: str, category: str) -> str:
        """
        Constructs S3 key following the pattern: prefix/category/version/relative_path
        """
        return f"{self.prefix}/{category}/{self.version}/{relative_path}"

    def upload_assets(self, category: str):
        """
        Uploads all files for a specific category (data, model, or db) to S3.
        Maintains sub-directory structures (essential for model checkpoints).
        """
        if not self.enabled:
            logger.error(f"Cannot upload {category}: S3 is disabled.")
            return
        
        local_dir = self._get_local_path(category)
        path = Path(local_dir)
        
        if not path.exists():
            logger.error(f"Local directory {local_dir} not found. Nothing to upload.")
            return

        logger.info(f"Starting upload for {category} (Version: {self.version})")
        for file in path.rglob('*'):
            if file.is_file():
                # Get the path relative to the versioned root (e.g., 'epp_registry.db')
                rel_path = file.relative_to(path)
                s3_key = self._get_s3_key(str(rel_path), category)
                
                try:
                    self.s3.upload_file(str(file), self.bucket, s3_key)
                    logger.info(f"Successfully uploaded: {rel_path} -> s3://{self.bucket}/{s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {file}: {e}")

    def download_assets(self, category: str):
        """
        Downloads all files for a specific category from S3 to the local versioned directory.
        Recreates local sub-directory structures as found in S3.
        """
        if not self.enabled:
            logger.error(f"Cannot download {category}: S3 is disabled.")
            return

        local_dir = self._get_local_path(category)
        s3_prefix = f"{self.prefix}/{category}/{self.version}/"
        
        logger.info(f"Fetching {category} assets from s3://{self.bucket}/{s3_prefix}")
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket, Prefix=s3_prefix):
                if 'Contents' not in page:
                    logger.info(f"No S3 assets found for {category} version {self.version}")
                    continue

                for obj in page['Contents']:
                    s3_key = obj['Key']
                    
                    # Extract file path relative to the version folder in S3
                    rel_s3_path = s3_key.replace(s3_prefix, "").lstrip('/')
                    if not rel_s3_path: 
                        continue # Skip directory markers
                    
                    local_file_path = os.path.join(local_dir, rel_s3_path)
                    
                    # Create local directories if they don't exist
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    
                    self.s3.download_file(self.bucket, s3_key, local_file_path)
                    logger.info(f"Downloaded: {s3_key} -> {local_file_path}")
        except Exception as e:
            logger.error(f"Error during download for {category}: {e}")

    def sync_all_to_s3(self):
        """Helper to upload data, models, AND databases for the active version."""
        logger.info("Syncing all assets (data, model, db) to S3...")
        self.upload_assets("data")
        self.upload_assets("model")
        self.upload_assets("db")

    def sync_all_from_s3(self):
        """Helper to download data, models, AND databases for the active version."""
        logger.info("Syncing all assets (data, model, db) from S3...")
        self.download_assets("data")
        self.download_assets("model")
        self.download_assets("db")
        
def main():
    """
    CLI Entry point for S3 operations.
    Usage:
        python -m src.common.s3_utils upload all
        python -m src.common.s3_utils download db
    """
    parser = argparse.ArgumentParser(description="S3 Asset Manager for Text2SQL Project")
    parser.add_argument("action", choices=["upload", "download"], help="Action to perform")
    parser.add_argument("category", choices=["data", "model", "db", "all"], help="Category of assets")
    
    args = parser.parse_args()
    manager = S3Manager()

    # Determine which categories to process
    if args.category == "all":
        categories = ["data", "model", "db"]
    else:
        categories = [args.category]

    for cat in categories:
        if args.action == "upload":
            manager.upload_assets(cat)
        else:
            manager.download_assets(cat)

if __name__ == "__main__":
    main()
