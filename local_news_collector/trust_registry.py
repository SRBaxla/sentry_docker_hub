# trust_registry.py
# Unified Trust Registry with class-based API and persistent JSON backend

import os
import json
from collections import defaultdict

DEFAULT_TRUST_SCORE = 0.5
TRUST_FLOOR = 0.25
DEFAULT_DECAY = 0.01
TRUST_FILE = os.getenv("TRUST_REGISTRY", "trust_registry.json")

class TrustRegistry:
    def __init__(self):
        self.scores = defaultdict(lambda: {"trust": DEFAULT_TRUST_SCORE, "type": "unknown", "decay": DEFAULT_DECAY})
        self.load()

    def load(self):
        if os.path.exists(TRUST_FILE):
            with open(TRUST_FILE, 'r') as f:
                data = json.load(f)
                for source, meta in data.items():
                    self.scores[source] = meta
        else:
            self._initialize_default_sources()

    def _initialize_default_sources(self):
        defaults = {
            "coindesk.com": {"trust": 0.85, "type": "news", "decay": 0.01},
            "cointelegraph.com": {"trust": 0.8, "type": "news", "decay": 0.01},
            "r/CryptoCurrency": {"trust": 0.6, "type": "reddit", "decay": 0.02},
            "r/Bitcoin": {"trust": 0.7, "type": "reddit", "decay": 0.015},
            "u/shadyUser123": {"trust": 0.1, "type": "reddit", "decay": 0.05},
        }
        self.scores.update(defaults)
        self.save()

    def save(self):
        with open(TRUST_FILE, 'w') as f:
            json.dump(self.scores, f, indent=2)

    def get_score(self, source: str) -> float:
        return self.scores[source]["trust"]

    def update_score(self, source: str, delta: float):
        meta = self.scores[source]
        meta["trust"] = max(0.0, min(1.0, meta["trust"] + delta))
        self.save()

    def decay_scores(self):
        for source, meta in self.scores.items():
            decay = meta.get("decay", DEFAULT_DECAY)
            meta["trust"] *= (1 - decay)
            meta["trust"] = round(max(TRUST_FLOOR, min(1.0, meta["trust"])), 4)
        self.save()

    def is_trusted(self, source: str, threshold: float = 0.3, allow_exploration: bool = True) -> bool:
        score = self.get_score(source)
        if score >= threshold:
            return True
        return allow_exploration and score >= TRUST_FLOOR

    def get_trusted_sources(self, threshold=0.5):
        return {
            src: meta for src, meta in self.scores.items()
            if meta.get("trust", 0) >= threshold
        }

# Singleton for use across modules
trust_registry = TrustRegistry()

if __name__ == "__main__":
    print("[INFO] Trusted sources above 0.5:")
    for src in trust_registry.get_trusted_sources().keys():
        print(" -", src)
