# trust_registry.py
# Maintains trust levels of known sentiment sources

import json
import os

REGISTRY_PATH = os.getenv("TRUST_REGISTRY", "trust_sources.json")

# Default scores if file doesn't exist
def _default_registry():
    return {
        "coindesk.com": {"trust": 0.85, "type": "news", "decay": 0.01},
        "cointelegraph.com": {"trust": 0.8, "type": "news", "decay": 0.01},
        "r/CryptoCurrency": {"trust": 0.6, "type": "reddit", "decay": 0.02},
        "r/Bitcoin": {"trust": 0.7, "type": "reddit", "decay": 0.015},
        "u/shadyUser123": {"trust": 0.1, "type": "reddit", "decay": 0.05},
    }

def load_registry():
    if not os.path.exists(REGISTRY_PATH):
        return _default_registry()
    with open(REGISTRY_PATH, "r") as f:
        return json.load(f)

def save_registry(registry):
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)

def get_trusted_sources(threshold=0.5):
    registry = load_registry()
    return {
        src: meta for src, meta in registry.items()
        if meta.get("trust", 0) >= threshold
    }

def update_trust(source, delta):
    registry = load_registry()
    current = registry.get(source, {"trust": 0.5, "type": "unknown", "decay": 0.01})
    current["trust"] = max(0.0, min(1.0, current["trust"] + delta))
    registry[source] = current
    save_registry(registry)

def decay_all():
    registry = load_registry()
    for src, meta in registry.items():
        decay = meta.get("decay", 0.01)
        meta["trust"] *= (1 - decay)
        meta["trust"] = round(max(0.0, min(1.0, meta["trust"])), 4)
    save_registry(registry)

if __name__ == "__main__":
    print("[INFO] Trusted sources above 0.5:")
    for src in get_trusted_sources().keys():
        print(" -", src)
