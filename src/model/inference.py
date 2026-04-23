import torch
import json
import os
import logging
from transformers import T5Tokenizer, T5ForConditionalGeneration
from peft import PeftModel
from src.common.config_manager import ConfigManager

logger = logging.getLogger("Inference")

class InferenceSchemaManager:
    """Original Fuzzy Table Detection Logic preserved."""
    def __init__(self, db_schemas):
        self.db_schemas = db_schemas
        self.trigger_map = {
            1: ["client", "customer", "location", "region", "asia", "eu", "usa", "australia"],
            2: ["release", "maintenance", "deployment", "patch", "v1", "v2", "window"]
        }

    def detect_indices(self, db_id, question):
        q_low = question.lower()
        relevant_indices = {0} # Always include epp_sla
        for idx, phrases in self.trigger_map.items():
            if any(p in q_low for p in phrases):
                relevant_indices.add(idx)
        
        table_names = self.db_schemas.get(db_id, {}).get('table_names_original', [])
        for i, name in enumerate(table_names):
            if name.lower() in q_low:
                relevant_indices.add(i)
        return relevant_indices

    def get_serialized_prompt(self, db_id, question):
        schema = self.db_schemas.get(db_id, {})
        if not schema: return ""
        indices = self.detect_indices(db_id, question)
        
        table_names = schema['table_names_original']
        column_names = schema['column_names_original']
        foreign_keys = schema.get('foreign_keys', [])

        serialized_tables = []
        for i in sorted(indices):
            t_name = table_names[i]
            cols = [c[1] for c in column_names if c[0] == i]
            serialized_tables.append(f"Table {t_name}({', '.join(cols)})")

        schema_str = " | ".join(serialized_tables)
        
        rel_set = set()
        for src_idx, dest_idx in foreign_keys:
            if column_names[src_idx][0] in indices and column_names[dest_idx][0] in indices:
                rel = f"{table_names[column_names[src_idx][0]]}.{column_names[src_idx][1]} = {table_names[column_names[dest_idx][0]]}.{column_names[dest_idx][1]}"
                rel_set.add(rel)

        if 0 in indices:
            if 1 in indices: rel_set.add("epp_sla.client_name = epp_client.client_name")
            if 2 in indices: rel_set.add("epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end")

        if rel_set:
            schema_str += " | Relationships: " + ", ".join(sorted(list(rel_set)))
        return schema_str

class Text2SQLInference:
    def __init__(self, mode="lora", model_size="base"):
        self.config = ConfigManager().get_config()
        self.version = self.config.get("active_version", "v1")
        self.mode = mode
        self.base_model_id = f"google/flan-t5-{model_size}"
        
        # Path Setup
        model_path = os.path.join(self.config["paths"]["model"], self.version, "final_model")
        tables_path = os.path.join(self.config["paths"]["data"], self.version, "tables.json")

        # Load Schema Manager
        with open(tables_path, 'r') as f:
            tables = json.load(f)
        self.schema_manager = InferenceSchemaManager({db['db_id']: db for db in tables})

        # Load Model & Tokenizer
        self.tokenizer = T5Tokenizer.from_pretrained(model_path, legacy=False)
        
        logger.info(f"Loading {mode} model from {model_path}...")
        if mode == "lora":
            base_model = T5ForConditionalGeneration.from_pretrained(
                self.base_model_id, torch_dtype=torch.float32, device_map="auto"
            )
            self.model = PeftModel.from_pretrained(base_model, model_path)
        else:
            self.model = T5ForConditionalGeneration.from_pretrained(
                model_path, torch_dtype=torch.float32, device_map="auto"
            )
        self.model.eval()

    def predict(self, question: str, db_id: str = "epp_registry"):
        """Callable function for FastAPI."""
        schema_str = self.schema_manager.get_serialized_prompt(db_id, question)
        
        input_text = (
            f"Using only the schema provided, generate a SQL query for the question.\n"
            f"Schema: {schema_str}\nQuestion: {question}\nSQL: "
        )

        inputs = self.tokenizer(input_text, return_tensors="pt", max_length=512, truncation=True).to(self.model.device)

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_length=512,
                num_beams=5,
                repetition_penalty=1.2
            )

        return {
            "question": question,
            "sql": self.tokenizer.decode(outputs[0], skip_special_tokens=True),
            "detected_schema": schema_str
        }

if __name__ == "__main__":
    # Test script
    infer = Text2SQLInference(mode="lora")
    print(infer.predict("Show average latency in ASIA yesterday"))
