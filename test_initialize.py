"""
test_initialize.py
------------------
Runs ONLY the initialize() phase of ECommSimulator so you can verify that
all tables are created and seed data is inserted correctly without starting
the full simulation loop.

Usage (from project root):
    python test_initialize.py

Credentials are read from .env (same values used by docker-compose).
PostgreSQL is expected to be reachable on localhost:5433 (mapped port).
"""

import argparse
import asyncio
import logging
import os
import sys

# Make both 'src.*' and bare 'data.*' imports resolve when running from project root.
_root = os.path.dirname(__file__)
sys.path.insert(0, _root)          # enables  from src.generator import ...
sys.path.insert(0, os.path.join(_root, "src"))  # enables  from data.models import ...

from dotenv import load_dotenv
from faker import Faker

# Load credentials from .env in the project root
load_dotenv(dotenv_path=os.path.join(_root, ".env"))

from generator import ECommSimulator  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)


def _build_args() -> argparse.Namespace:
    """Build the minimal args Namespace that ECommSimulator requires."""
    return argparse.Namespace(
        # Database — credentials from .env, port 5433 is the host-mapped port
        db_host=f"localhost:5433",
        db_user=os.getenv("POSTGRES_USER", "postgres"),
        db_password=os.getenv("POSTGRES_PASSWORD", "postgres_password"),
        db_name=os.getenv("POSTGRES_DB", "default_db"),
        db_schema="demo",
        db_batch_size=500,
        # Seed size — keep small for a quick test
        init_num_users=50,
        # Simulation args (not used during initialize, but the class reads them)
        avg_qps=1.0,
        max_iter=0,
        user_create_prob=0.0,
        user_update_prob=0.0,
        order_update_prob=0.0,
        ghost_create_prob=0.0,
        bootstrap_servers="localhost:9092",
        topic_prefix="ecomm",
        config=None,
    )


async def main():
    args = _build_args()
    config = {
        "init_num_campaigns": 5,   # how many seed campaigns to create
    }

    simulator = ECommSimulator(args, Faker(), config)
    try:
        success = await simulator.initialize()
        if success:
            logging.info("✅  initialize() completed successfully.")
        else:
            logging.error("❌  initialize() returned False — check logs above.")
    finally:
        await simulator.close()


if __name__ == "__main__":
    asyncio.run(main())
