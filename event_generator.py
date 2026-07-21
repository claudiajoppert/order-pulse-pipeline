"""Synthetic grocery order-event stream generator."""

import random
import time
import uuid

STORES = [f"store_{i:03d}" for i in range(1, 21)]
CATEGORIES = ["produce", "dairy", "frozen", "bakery", "pantry", "household", "meat_seafood"]
EVENT_TYPES = ["order_placed", "item_picked", "substitution", "delivered", "late"]

# Rough base probabilities so the stream isn't uniform noise.
CATEGORY_LATE_RISK = {
    "produce": 0.10,
    "dairy": 0.08,
    "frozen": 0.15,
    "bakery": 0.06,
    "pantry": 0.05,
    "household": 0.04,
    "meat_seafood": 0.12,
}


def generate_event(seed: int | None = None) -> dict:
    """Generate a single synthetic order event."""
    rng = random.Random(seed)
    category = rng.choice(CATEGORIES)
    store = rng.choice(STORES)
    is_late = rng.random() < CATEGORY_LATE_RISK[category]
    is_substitution = rng.random() < 0.12

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4())[:8],
        "store_id": store,
        "category": category,
        "event_type": "late" if is_late else ("substitution" if is_substitution else "order_placed"),
        "items_count": rng.randint(1, 40),
        "is_late": is_late,
        "is_substitution": is_substitution,
        "generated_at": time.time(),
    }


def generate_batch(n: int, seed: int | None = None) -> list[dict]:
    """Generate n synthetic events. Each event gets a distinct derived seed
    so batches are reproducible given the same base seed, without every
    event being identical.
    """
    base = seed if seed is not None else random.randint(0, 1_000_000)
    return [generate_event(seed=base + i) for i in range(n)]
