# trust_registry.py

import os
import json
from collections import defaultdict

DEFAULT_TRUST_SCORE = 0.5  # Median trust score
TRUST_FLOOR = 0.25         # Soft lower bound for exploration
TRUST_FILE = os.path.join(os.path.dirname(__file__), "trust_registry.json")

class TrustRegistry:
    def __init__(self):
        self.scores = defaultdict(lambda: DEFAULT_TRUST_SCORE)
        self.load()

    def load(self):
        if os.path.exists(TRUST_FILE):
            with open(TRUST_FILE, 'r') as f:
                data = json.load(f)
                for source, score in data.items():
                    self.scores[source] = score

    def save(self):
        with open(TRUST_FILE, 'w') as f:
            json.dump(self.scores, f, indent=2)

    def get_score(self, source: str) -> float:
        return self.scores[source]  # returns default if not present

    def update_score(self, source: str, delta: float):
        self.scores[source] = max(0.0, min(1.0, self.scores[source] + delta))
        self.save()

    def decay_scores(self, rate: float = 0.01, floor: float = TRUST_FLOOR):
        for source, score in self.scores.items():
            if score > floor:
                self.scores[source] = max(floor, score - rate)
        self.save()

    def is_trusted(self, source: str, threshold: float = 0.3, allow_exploration: bool = True) -> bool:
        score = self.get_score(source)
        if score >= threshold:
            return True
        return allow_exploration and score >= TRUST_FLOOR

# Helper instance for import
trust_registry = TrustRegistry()
