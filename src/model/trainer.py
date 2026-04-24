import os
import json
import torch
import argparse
import logging
from datasets import Dataset
from transformers import (
    T5Tokenizer,
    T5ForConditionalGeneration,
    Trainer,
    TrainingArguments,
    DataCollatorForSeq2Seq
)
from peft import LoraConfig, get_peft_model, TaskType

from common.config_manager import ConfigManager
from common.s3_utils import S3Manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Trainer")

class Text2SQLTrainer:
    
    def __init__(self, model_size="base"):
        # 1. Initialize the ConfigManager
        self.config_manager = ConfigManager()
        
        # 2. Fix: Access .config attribute directly (not .get_config())
        self.config = self.config_manager.config
        
        self.s3_manager = S3Manager()
        
        # 3. Model Selection logic
        self.model_name = f"google/flan-t5-{model_size}"
        
        # 4. Use ConfigManager properties for better reliability
        self.active_version = self.config_manager.version
        
        # 5. Optimized Path Resolution
        # Instead of manual os.path.join, use the helpers we built in ConfigManager
        self.data_dir = self.config_manager.get_versioned_data_path()
        self.model_dir = self.config_manager.get_versioned_model_path()
        
        # Define the input file specifically
        self.input_file = os.path.join(self.data_dir, "train_augmented.json")
        
        # Ensure model directory exists (ConfigManager helpers already do this, but safe to keep)
        os.makedirs(self.model_dir, exist_ok=True)

    def preprocess_function(self, examples, tokenizer):
        """Original preprocess logic preserved."""
        inputs = [
            f"Using only the schema provided, generate a SQL query for the question.\n"
            f"Schema: {s}\nQuestion: {q}\nSQL: "
            for s, q in zip(examples['prompt_schema'], examples['question'])
        ]
        model_inputs = tokenizer(inputs, max_length=512, truncation=True)
        labels = tokenizer(text_target=examples['query'], max_length=128, truncation=True)
        model_inputs["labels"] = labels["input_ids"]
        return model_inputs

    def train(self, mode="lora"):
        # Ensure data is present
        if not os.path.exists(self.input_file):
            logger.info("Data not found. Syncing from S3...")
            self.s3_manager.download_assets("data")

        tokenizer = T5Tokenizer.from_pretrained(self.model_name, legacy=False)
        
        # Load model in FP32 (Critical for T5 on T4 as per your original code)
        model = T5ForConditionalGeneration.from_pretrained(
            self.model_name, 
            torch_dtype=torch.float32
        )

        if mode == "lora":
            logger.info("Configuring LoRA...")
            lora_config = LoraConfig(
                r=16,
                lora_alpha=32,
                target_modules=["q", "v"],
                lora_dropout=0.05,
                bias="none",
                task_type=TaskType.SEQ_2_SEQ_LM
            )
            model = get_peft_model(model, lora_config)
        
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device)
        model.print_trainable_parameters()

        # Load Dataset
        with open(self.input_file, 'r') as f:
            raw_data = json.load(f)
        
        dataset = Dataset.from_list(raw_data).shuffle(seed=42)
        processed_dataset = dataset.map(
            lambda x: self.preprocess_function(x, tokenizer),
            batched=True,
            remove_columns=dataset.column_names
        )
        dataset_split = processed_dataset.train_test_split(test_size=0.1)

        # Training Args (Original T4-stable settings)
        training_args = TrainingArguments(
            output_dir=os.path.join(self.model_dir, "checkpoints"),
            learning_rate=2e-4 if mode == "lora" else 5e-5,
            per_device_train_batch_size=4,
            gradient_accumulation_steps=4,
            num_train_epochs=5,
            fp16=False, # Keeping FP16 False for stability
            logging_steps=10,
            eval_strategy="steps",
            eval_steps=50,
            save_total_limit=2,
            load_best_model_at_end=True,
            report_to="none"
        )

        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=dataset_split["train"],
            eval_dataset=dataset_split["test"],
            data_collator=DataCollatorForSeq2Seq(tokenizer, model=model, padding=True)
        )

        logger.info(f"Training {mode} version {self.active_version}...")
        trainer.train()

        # Save Final Output
        final_path = os.path.join(self.model_dir, "final_model")
        model.save_pretrained(final_path)
        tokenizer.save_pretrained(final_path)
        
        # Sync to S3
        self.s3_manager.upload_assets("model")
        logger.info(f"Model saved and uploaded to S3: {final_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["lora", "full"], default="lora")
    parser.add_argument("--size", choices=["small", "base"], default="base")
    args = parser.parse_args()

    trainer = Text2SQLTrainer(model_size=args.size)
    trainer.train(mode=args.mode)
