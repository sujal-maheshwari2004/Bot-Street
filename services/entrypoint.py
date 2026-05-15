import os
import sys
import importlib

SERVICE = os.getenv("SERVICE", "").lower()

runners = {
    "engine"    : "services.run_engine",
    "ledger"    : "services.run_ledger",
    "price-feed": "services.run_price_feed",
    "sentiment" : "services.run_sentiment",
    "candles"   : "services.run_candles",
    "circuit"   : "services.run_circuit",
    "bots"      : "services.run_bots",
    "api"       : "services.run_api",
}

if SERVICE not in runners:
    print(f"Unknown SERVICE={SERVICE!r}. Valid: {list(runners)}")
    sys.exit(1)

mod = importlib.import_module(runners[SERVICE])
mod.main()