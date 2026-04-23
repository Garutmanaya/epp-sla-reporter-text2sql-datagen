# =========================================
# MODULE: run_pipeline
# PURPOSE: CLI pipeline runner
# =========================================

# =========================================
# IMPORTS
# =========================================
import argparse

from common.config import load_config
from common.config_loader import (
    get_active_version,
)

from common.data_generator import main as run_data_generation
from xgboost_ad.train import main as run_training
from xgboost_ad.threshold_generator import main as run_thresholds
from xgboost_ad.inference import main as run_inference
from xgboost_ad.validator import main as run_validation

# =========================================
# PIPELINE STEPS
# =========================================
def step_data_generation():
    print("\n=== STEP: DATA GENERATION ===")
    run_data_generation()

def step_training():
    print("\n=== STEP: TRAINING ===")
    run_training()

def step_thresholds():
    print("\n=== STEP: THRESHOLDS ===")
    run_thresholds()

def step_inference():
    print("\n=== STEP: INFERENCE ===")
    run_inference()

def step_validation():
    print("\n=== STEP: VALIDATION ===")
    run_validation()

# =========================================
# STEP ROUTER
# =========================================
def run_step(step):

    if step == "data":
        step_data_generation()

    elif step == "train":
        step_training()

    elif step == "thresholds":
        step_thresholds()

    elif step == "inference":
        step_inference()
    
    elif step == "validate":
        step_validation()

    elif step == "all":
        step_data_generation()
        step_training()
        step_thresholds()
        step_inference()
        step_validation()

    else:
        raise ValueError(f"Invalid step: {step}")


# =========================================
# MAIN
# =========================================
def main():

    parser = argparse.ArgumentParser(description="Run anomaly detection pipeline")

    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "data", "train", "thresholds", "inference", "validate"],
        help="Pipeline step to run"
    )

    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="Override active version (optional)"
    )

    args = parser.parse_args()

    # Resolve version
    version = args.version if args.version else get_active_version()

    cfg = load_config(version)

    print("\n=====================================")
    print(f"Pipeline Start | Version: {cfg.version}")
    print(f"Step: {args.step}")
    print("=====================================")

    run_step(args.step)

    print("\nPipeline completed successfully")


# =========================================
# RUN
# =========================================
if __name__ == "__main__":
    main() 


#========================================================
# CLI EXAMPLES 
## Run full pipeline
# python -m pipeline.run_pipeline_xgboost
#
# Run specific steps
# python -m pipeline.run_pipeline_xgboost --step data
# python -m pipeline.run_pipeline_xgboost --step train
# python -m pipeline.run_pipeline_xgboost --step thresholds
# python -m pipeline.run_pipeline_xgboost --step inference
# python -m pipeline.run_pipeline_xgboost --step validate
# python -m pipeline.run_pipeline_xgboost --step all

# Override version
#python -m pipeline.run_pipeline_xgboost --version v2
#===================================================================
