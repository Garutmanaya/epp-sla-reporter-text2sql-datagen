import boto3
import os
import streamlit as st
from common.config_manager import ConfigManager

def download_db_from_s3():
    """Downloads the database from S3 to the local versioned path."""
    cfg = ConfigManager()
    s3_cfg = cfg.config.get("s3", {})
    
    if not s3_cfg.get("enabled"):
        st.warning("S3 is disabled in config. Using local database if available.")
        return

    # Resolve local path where DB should live
    local_db_path = cfg.get_versioned_db_path()
    bucket = s3_cfg.get("bucket")
    # Construct S3 Key: e.g., hub/v1/databases/epp_registry.db
    s3_key = f"{s3_cfg.get('prefix')}/{cfg.version}/databases/{cfg.database_name}"

    if not os.path.exists(local_db_path):
        try:
            with st.spinner(f"Downloading database from S3: {s3_key}..."):
                s3 = boto3.client('s3', region_name=s3_cfg.get('region'))
                s3.download_file(bucket, s3_key, str(local_db_path))
                st.success("Database synced successfully!")
        except Exception as e:
            st.error(f"Failed to download DB from S3: {e}")
