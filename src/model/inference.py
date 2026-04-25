import torch
import json
import os
import logging
from transformers import T5Tokenizer, T5ForConditionalGeneration
from peft import PeftModel
from common.config_manager import ConfigManager

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


    def format_sql_for_execution(self, generated_sql):
        """
        Converts 'table__column' back to 'table.column' so the
        SQL can actually run against the database.
        """
        # Simply swap the double underscore back to a dot
        return generated_sql.replace("__", ".")

    def format_prompt_for_model(self, schema_str):
        """
        Converts 'table.column' to 'table__column' in the prompt schema
        to match the new training format.
        """
        tables = ["epp_sla", "epp_client", "epp_release"]
        patched_str = schema_str

        for table in tables:
            # Replaces 'epp_sla.' with 'epp_sla__'
            patched_str = patched_str.replace(f"{table}.", f"{table}__")

        return patched_str

    def get_serialized_prompt(self, db_id, question):
        schema = self.db_schemas.get(db_id, {})
        if not schema: return ""

        indices = self.detect_indices(db_id, question)
        table_names = schema['table_names_original']
        column_names = schema['column_names_original']
        foreign_keys = schema.get('foreign_keys', [])

        # Serialize Tables with Prefixed Columns
        serialized_tables = []
        for i in sorted(indices):
            t_name = table_names[i]
            # UPDATED: Mapping every column to 'tablename.columnname'
            cols = [f"{t_name}.{c[1]}" for c in column_names if c[0] == i]
            # UPDATED: Using 'Table name: col1, col2' format
            serialized_tables.append(f"Table {t_name}: {', '.join(cols)}")

        schema_str = " | ".join(serialized_tables)

        # Relationships
        rel_set = set()
        for src_idx, dest_idx in foreign_keys:
            s_tab_idx = column_names[src_idx][0]
            d_tab_idx = column_names[dest_idx][0]
            if s_tab_idx in indices and d_tab_idx in indices:
                rel = f"{table_names[s_tab_idx]}.{column_names[src_idx][1]} = {table_names[d_tab_idx]}.{column_names[dest_idx][1]}"
                rel_set.add(rel)

        # Implicit Bridges (Business Logic)
        if 0 in indices:
            if 1 in indices:
                rel_set.add("epp_sla.client_name = epp_client.client_name")
            if 2 in indices:
                rel_set.add("epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end")

        if rel_set:
            # UPDATED: Added spacing for consistent tokenization
            schema_str += " | Relationships: " + ", ".join(sorted(list(rel_set)))

        return self.format_prompt_for_model(schema_str)
    
    def get_serialized_prompt_v1(self, db_id, question):
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
        # 1. Initialize ConfigManager
        self.config_manager = ConfigManager()
        
        # 2. Fix: Access .config attribute directly
        self.config = self.config_manager.config
        
        # 3. Use ConfigManager properties for version and mode
        self.version = self.config_manager.version
        self.mode = mode
        self.base_model_id = f"google/flan-t5-{model_size}"
        
        # 4. Path Setup using ConfigManager helpers
        # This replaces the manual os.path.join logic
        versioned_model_root = self.config_manager.get_versioned_model_path()
        versioned_data_root = self.config_manager.get_versioned_data_path()
        
        # Define specific file paths
        model_path = os.path.join(versioned_model_root, "final_model")
        tables_path = os.path.join(versioned_data_root, "tables.json")

        print(f"model_path ==> {model_path}")
        print(f"tables_path ==> {tables_path}")

        # Load Schema Manager
        if not os.path.exists(tables_path):
            logger.error(f"Tables file not found at {tables_path}")
            raise FileNotFoundError(f"Missing tables.json in {versioned_data_root}")

        with open(tables_path, 'r') as f:
            tables = json.load(f)
        
        self.schema_manager = InferenceSchemaManager({db['db_id']: db for db in tables})

        # Load Model & Tokenizer
        self.tokenizer = T5Tokenizer.from_pretrained(model_path, legacy=False)
        
        logger.info(f"Loading {mode} model from {model_path}...")
        
        if mode == "lora":
            base_model = T5ForConditionalGeneration.from_pretrained(
                self.base_model_id, 
                torch_dtype=torch.float32, 
                device_map="auto"
            )
            self.model = PeftModel.from_pretrained(base_model, model_path)
        else:
            self.model = T5ForConditionalGeneration.from_pretrained(
                model_path, 
                torch_dtype=torch.float32, 
                device_map="auto"
            )
        
        self.model.eval()

    def predict(self, question: str, db_id: str = "epp_registry"):
        """Callable function for FastAPI."""
        schema_str = self.schema_manager.get_serialized_prompt(db_id, question)
        
        input_text = (
            f"Using only the schema provided, generate a SQL query for the question.\n"
            f"Schema: {schema_str}\nQuestion: {question}\nSQL: "
        )

        print(input_text)

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
            "sql": self.schema_manager.format_sql_for_execution(self.tokenizer.decode(outputs[0], skip_special_tokens=True)),
            "detected_schema": schema_str
        }

if __name__ == "__main__":
    # Test script
    infer = Text2SQLInference(mode="lora")
    print(infer.predict("Show average latency in ASIA yesterday"))
