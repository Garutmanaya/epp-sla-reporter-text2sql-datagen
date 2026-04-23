# =========================================
# MODULE: pipeline_run
# PURPOSE: End-to-End Orchestrator for Text2SQL
# =========================================

import argparse
import sys
import logging
import traceback
from common.config_manager import ConfigManager
from common.s3_utils import S3Manager
from database.db_generator import EPPDatabaseGenerator
from datagen.generator import DataGenerator 
from datagen.augmentor import DataAugmentor 
from model.trainer import Text2SQLTrainer
from model.inference import Text2SQLInference

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("PipelineRunner")

# =========================================
# PIPELINE STEPS
# =========================================

def step_s3_sync_down():
    """Download existing versioned assets from S3."""
    print("\n=== STEP 0: S3 SYNC (DOWNLOAD) ===")
    s3 = S3Manager()
    if s3.enabled:
        s3.sync_all_from_s3()
    else:
        print("S3 disabled in config. Skipping download.")

def step_db_generation(reset=False):
    """Generate the SQLite database for testing/agent use."""
    print("\n=== STEP 1: DATABASE GENERATION ===")
    gen = EPPDatabaseGenerator()
    gen.initialize(reset=reset)

def step_datagen():
    """Generate initial training data from schemas and templates."""
    print("\n=== STEP 2: RAW DATA GENERATION ===")
    config = ConfigManager()
    params = config.pipeline_params
    
    # Dynamically pull from config
    hardcoded_args = [
        "--count", str(config.datagen_count), 
        "--mode", str(config.datagen_mode)
    ]
    
    DataGenerator(argv=hardcoded_args)


def step_augmentation():
    """Augment the generated data for better model robustness."""
    print("\n=== STEP 3: DATA AUGMENTATION ===")
    cfg = ConfigManager()
    
    # Get the value from your new pipeline config block
    mode = cfg.pipeline_params.get("datagen_augment_mode", "natural") 
    
    # Call directly with the config value
    DataAugmentor(style_choice=mode) 
    

def step_training(mode="lora", size="base"):
    """Run model training (Full or LoRA)."""
    print(f"\n=== STEP 4: TRAINING ({mode.upper()} - {size}) ===")
    trainer = Text2SQLTrainer(model_size=size)
    trainer.train(mode=mode)

def step_s3_sync_up():
    """Upload newly created assets (DB, Data, Models) to S3."""
    print("\n=== STEP 5: S3 SYNC (UPLOAD) ===")
    s3 = S3Manager()
    if s3.enabled:
        s3.sync_all_to_s3()
    else:
        print("S3 disabled in config. Skipping upload.")

def step_inference_test(mode="lora", size="base"):
    """Run a quick sanity check inference."""
    print("\n=== STEP 6: INFERENCE SANITY CHECK ===")
    try:
        infer = Text2SQLInference(mode=mode, model_size=size)
        sample_q = "Show total volume for AtlasRegistrar yesterday"
        result = infer.predict(sample_q)
        print(f"Question: {result['question']}")
        print(f"SQL: {result['sql']}")
    except Exception as e:
        print(f"Inference check failed: {e}")

# =========================================
# STEP ROUTER
# =========================================

def run_pipeline(args):
    config = ConfigManager()
    version = config.version

    print("\n" + "="*50)
    print(f"TEXT2SQL PIPELINE | VERSION: {version}")
    print(f"TARGET STEP: {args.step}")
    print("="*50)

    # Sequence logic
    if args.step in ["sync_down", "all"]:
        step_s3_sync_down()

    if args.step in ["db", "all"]:
        step_db_generation(reset=(args.step == "all"))

    if args.step in ["datagen", "all"]:
        step_datagen()

    if args.step in ["augment", "all"]:
        step_augmentation()

    if args.step in ["train", "all"]:
        step_training(mode=args.mode, size=args.size)

    if args.step in ["sync_up", "all"]:
        step_s3_sync_up()

    if args.step in ["inference", "all"]:
        step_inference_test(mode=args.mode, size=args.size)

# =========================================
# MAIN
# =========================================

def main():
    parser = argparse.ArgumentParser(description="Full EPP Text2SQL Workflow")

    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "sync_down", "db", "datagen", "augment", "train", "sync_up", "inference"],
        help="Pipeline step to run"
    )

    parser.add_argument(
        "--mode", type=str, default="lora", choices=["lora", "full"],
        help="Training mode"
    )

    parser.add_argument(
        "--size", type=str, default="base", choices=["small", "base"],
        help="Model size"
    )

    args = parser.parse_args()
    
    try:
        run_pipeline(args)
        print("\nPipeline completed successfully.")

    except Exception as e:
        # This replaces the simple logger.error
        print("\n" + "!"*50)
        print("PIPELINE EXECUTION FAILED")
        print("!"*50)
        
        # This prints the full error path (file, line number, function)
        traceback.print_exc() 
        
        print("!"*50)
        sys.exit(1)

if __name__ == "__main__":
    main()

#========================================================#
#python -m pipeline.pipeline_run --step all --mode lora
#python -m pipeline.pipeline_run --step datagen
#python -m pipeline.pipeline_run --step augment
#python -m pipeline.pipeline_run --step train --mode full --size small
#
#=========================================================#