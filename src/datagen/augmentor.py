import json
import random
import re
import os
from datetime import datetime
from common.config_manager import ConfigManager 

class EPPAugmentor:
    """
    Handles linguistic augmentation for EPP SLA Text-to-SQL datasets.
    Transforms robotic templates into diverse natural language variations.
    """

    def __init__(self, log_enabled=True, log_path="output/augmentation_debug.jsonl"):
        self.log_enabled = log_enabled
        self.log_path = log_path

        # 1. Trigger Map: Defines which keywords indicate a specific table is needed
        # 0: epp_sla (default), 1: epp_client, 2: epp_release
        self.trigger_map = {
            1: ["client", "customer", "account", "location", "at the client level"],
            2: ["release", "deployment", "maintenance", "window", "rollout"]
        }

        # 2. Synonym Map: Replaces robotic technical terms with natural alternatives
        self.variations = {
            "total number of requests": ["total traffic", "request count", "volume", "throughput"],
            "across clients": ["per customer", "by client", "at the account level", "for all users"],
            "across releases": ["during rollout periods", "per release", "by maintenance window"],
            "average response time": ["avg latency", "mean speed", "response delay"],
            "count of distinct commands": ["number of unique actions", "unique command count", "different commands"],
            "hourly trend": ["hour-by-hour breakdown", "hourly stats", "history per hour"]
        }

        # 3. Conversational Wrappers for "Verbose" mode
        self.prefixes = ["Please show me", "I need to see", "Can you fetch", "Give me a report on", "I want the"]
        self.suffixes = ["for my report.", "broken down by hour.", "as soon as possible.", "clearly."]

        # Initialize log file
        if self.log_enabled:
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            with open(self.log_path, "w") as f: pass 

    def detect_relevant_indices(self, question, table_names_original):
        """
        Identifies which tables (by index) are referenced in the question string.
        Ensures the prompt_schema contains only necessary context.
        """
        q_low = question.lower()
        indices = {0} # Always include the core fact table (epp_sla)

        # Check triggers for JOIN tables
        for idx, phrases in self.trigger_map.items():
            if any(p in q_low for p in phrases):
                indices.add(idx)

        # Safety check against actual table names
        for i, name in enumerate(table_names_original):
            if name.lower() in q_low:
                indices.add(i)
        return indices

    def _apply_linguistic_style(self, text, style):
        """
        Applies a specific persona to the query string.
        - short: Keyword-heavy, search-engine style.
        - natural: Balanced, standard human phrasing.
        - verbose: Formal, conversational, and polite.
        """
        text = text.lower()
        
        if style == "short":
            # Strip common robotic filler
            text = text.replace("get ", "").replace("fetch ", "").replace("total number of ", "")
            return text.strip()

        if style == "verbose":
            # Add polite framing
            prefix = random.choice(self.prefixes)
            return f"{prefix} {text}"

        return text # Natural returns text as-is

    def augment(self, text, style="natural"):
        """
        The main transformation engine. Swaps synonyms and applies styles.
        """
        new_q = text

        # 1. Randomized Synonym Replacement
        for anchor, alts in self.variations.items():
            if anchor.lower() in new_q.lower():
                if random.random() < 0.8: # 80% chance to replace
                    pattern = re.compile(re.escape(anchor), re.IGNORECASE)
                    new_q = pattern.sub(random.choice(alts), new_q)

        # 2. Handle Location variations (ASIA -> the ASIA region)
        locations = ["USA", "EU", "ASIA", "AUSTRALIA"]
        for loc in locations:
            if loc in new_q:
                loc_variants = [loc, f"the {loc} region", f"customers in {loc}"]
                new_q = new_q.replace(loc, random.choice(loc_variants))

        # 3. Apply the requested persona
        new_q = self._apply_linguistic_style(new_q, style)

        # Cleanup whitespace and capitalization
        new_q = re.sub(r'\s+', ' ', new_q).strip()
        return new_q.capitalize()

    def log_transformation(self, original, augmented, style):
        """Debug helper to track how questions are being mutated."""
        if self.log_enabled:
            with open(self.log_path, "a") as f:
                log_entry = {"style": style, "from": original, "to": augmented}
                f.write(json.dumps(log_entry) + "\n")
           
# --- SCHEMA SERIALIZATION LOGIC ---

def serialize_schema(db_id, db_schemas, indices):
    """
    Constructs the 'prompt_schema' string. 
    Includes table definitions and foreign key relationships for the detected indices.
    """
    schema = db_schemas.get(db_id, {})
    if not schema: return ""

    table_names = schema['table_names_original']
    column_names = schema['column_names_original']
    foreign_keys = schema.get('foreign_keys', [])

    # Build Table Strings
    serialized_tables = []
    for i in sorted(indices):
        t_name = table_names[i]
        cols = [c[1] for c in column_names if c[0] == i]
        serialized_tables.append(f"Table {t_name}({', '.join(cols)})")

    schema_str = " | ".join(serialized_tables)

    # Build Relationship Strings
    rel_set = set()
    for src_idx, dest_idx in foreign_keys:
        s_tab_idx = column_names[src_idx][0]
        d_tab_idx = column_names[dest_idx][0]
        if s_tab_idx in indices and d_tab_idx in indices:
            rel = f"{table_names[s_tab_idx]}.{column_names[src_idx][1]} = {table_names[d_tab_idx]}.{column_names[dest_idx][1]}"
            rel_set.add(rel)

    # Add hardcoded business logic relationships for SLA reporter if not in FKs
    if 0 in indices:
        if 1 in indices: rel_set.add("epp_sla.client_name = epp_client.client_name")
        if 2 in indices: rel_set.add("epp_sla.date BETWEEN epp_release.release_start AND epp_release.release_end")

    if rel_set:
        schema_str += " | Relationships: " + ", ".join(sorted(list(rel_set)))

    return schema_str

# --- MAIN EXECUTION ---

def DataAugmentor():
    cfg = ConfigManager()
    data_dir = cfg.get_versioned_data_path()
    # File Paths
    
    input_file = os.path.join(data_dir, "train.json")
    output_file = os.path.join(data_dir, "train_augmented.json")
    tables_file = os.path.join(data_dir, "tables.json")
    augmentation_debug_file = os.path.join(data_dir, "augmentation_debug.jsonl")
   

    # Configuration
    print("--- EPP Data Augmentation Tool ---")
    print("1. Generate 'Short' variations")
    print("2. Generate 'Natural' variations")
    print("3. Generate 'Verbose' variations")
    print("4. Generate ALL variations (Recommended)")
    
    choice = input("Select an option (1-4): ")
    style_map = {"1": ["short"], "2": ["natural"], "3": ["verbose"], "4": ["short", "natural", "verbose"]}
    selected_styles = style_map.get(choice, ["natural"])

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Please run the generator first.")
        return

    # Load Data
    with open(input_file, 'r') as f:
        original_data = json.load(f)
    with open(tables_file, 'r') as f:
        tables = json.load(f)
    db_schemas = {db['db_id']: db for db in tables}

    augmentor = EPPAugmentor(log_path=augmentation_debug_file)
    final_dataset = []

    print(f"Processing {len(original_data)} items...")

    for entry in original_data:
        db_id = entry["db_id"]
        orig_q = entry["question"]
        table_names = db_schemas[db_id]['table_names_original']
        
        # 1. Identify relevant tables
        relevant_indices = augmentor.detect_relevant_indices(orig_q, table_names)
        
        # 2. Create Prompt Schema
        schema_str = serialize_schema(db_id, db_schemas, relevant_indices)

        # 3. Save the Ground Truth (is_augmented: false)
        orig_record = entry.copy()
        orig_record["prompt_schema"] = schema_str
        orig_record["is_augmented"] = False
        final_dataset.append(orig_record)

        # 4. Generate Augmentations
        for i, style in enumerate(selected_styles):
            aug_record = entry.copy()
            aug_q = augmentor.augment(orig_q, style=style)
            
            aug_record["question"] = aug_q
            aug_record["prompt_schema"] = schema_str
            aug_record["is_augmented"] = True
            aug_record["variation_id"] = i + 1
            
            augmentor.log_transformation(orig_q, aug_q, style)
            final_dataset.append(aug_record)

    # Save final results
    with open(output_file, "w") as f:
        json.dump(final_dataset, f, indent=2)

    print(f"✅ Success! Generated {len(final_dataset)} total records.")
    print(f"📁 Saved to: {output_file}")
    print(f"📁 Debug File Saved to: {augmentation_debug_file}")


if __name__ == "__main__":
    main()
