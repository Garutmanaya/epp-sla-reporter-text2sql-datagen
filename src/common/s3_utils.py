import boto3
import os
import logging
import argparse
from pathlib import Path
from src.common.config_manager import ConfigManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("S3Utils")

class S3Manager:
    def __init__(self):
        self.config_manager = ConfigManager()
        self.config = self.config_manager.get_config()
        
        self.s3_cfg = self.config.get("s3", {})
        self.enabled = self.s3_cfg.get("enabled", False)
        self.version = self.config.get("active_version", "v1")
        
        # Paths from config: e.g., {"model": "models/", "data": "data/"}
        self.path_map = self.config.get("paths", {})
        
        if self.enabled:
            self.s3 = boto3.client('s3', region_name=self.s3_cfg.get('region', 'us-east-1'))
            self.bucket = self.s3_cfg.get('bucket')
            self.prefix = self.s3_cfg.get('prefix', 'data')
        else:
            logger.warning("S3 is disabled in main.config.json. Operations will be skipped.")

    def _get_local_path(self, category: str) -> str:
        """Resolves local path based on config and version (e.g., data/v1 or models/v1)."""
        base = self.path_map.get(category, f"{category}/")
        return os.path.join(base, self.version)

    def _get_s3_key(self, relative_path: str, category: str) -> str:
        """Constructs S3 key: prefix/category/version/relative_path"""
        return f"{self.prefix}/{category}/{self.version}/{relative_path}"

    def upload_assets(self, category: str):
        """Uploads all files for a specific category (data or model) to S3."""
        if not self.enabled:
            logger.error("Cannot upload: S3 is disabled.")
            return
        
        local_dir = self._get_local_path(category)
        path = Path(local_dir)
        
        if not path.exists():
            logger.error(f"Local directory {local_dir} not found. Nothing to upload.")
            return

        logger.info(f"Starting upload for {category} (Version: {self.version})")
        for file in path.rglob('*'):
            if file.is_file():
                # Keep sub-directory structure (important for models/checkpoints)
                rel_path = file.relative_to(path)
                s3_key = self._get_s3_key(str(rel_path), category)
                
                try:
                    self.s3.upload_file(str(file), self.bucket, s3_key)
                    logger.info(f"Successfully uploaded: {rel_path} -> s3://{self.bucket}/{s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {file}: {e}")

    def download_assets(self, category: str):
        """Downloads all files for a specific category from S3 to local disk."""
        if not self.enabled:
            logger.error("Cannot download: S3 is disabled.")
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
                    # Extract path relative to the version folder
                    rel_s3_path = s3_key.replace(s3_prefix, "").lstrip('/')
                    if not rel_s3_path: continue # Skip the directory object itself
                    
                    local_file_path = os.path.join(local_dir, rel_s3_path)
                    
                    # Create sub-directories if they exist in S3
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    
                    self.s3.download_file(self.bucket, s3_key, local_file_path)
                    logger.info(f"Downloaded: {s3_key} -> {local_file_path}")
        except Exception as e:
            logger.error(f"Error during download: {e}")

def main():
    parser = argparse.ArgumentParser(description="S3 Asset Manager for Text2SQL Datagen/Model")
    parser.add_argument("action", choices=["upload", "download"], help="Action to perform")
    parser.add_argument("category", choices=["data", "model", "all"], help="Category of assets")
    
    args = parser.parse_args()
    manager = S3Manager()

    categories = ["data", "model"] if args.category == "all" else [args.category]

    for cat in categories:
        if args.action == "upload":
            manager.upload_assets(cat)
        else:
            manager.download_assets(cat)

if __name__ == "__main__":
    main()
