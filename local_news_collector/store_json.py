import os
import json
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(DATA_DIR, exist_ok=True)

def store_raw(item: dict):
    """Append raw item JSON to daily file."""
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    path = os.path.join(DATA_DIR, f"news_{date_str}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")
