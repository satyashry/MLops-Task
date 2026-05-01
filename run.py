import argparse
import pandas as pd
import yaml
import numpy as np
import time
import json
import logging

# CLI arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--config", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--log-file", required=True)
args = parser.parse_args()

# Logging setup
logging.basicConfig(
    filename=args.log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

start_time = time.time()

try:
    logging.info("Job started")

    # Load config
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    # Validate config
    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    seed = config["seed"]
    window = config["window"]
    version = config["version"]

    np.random.seed(seed)

    logging.info(f"Config loaded: {config}")

    # Load dataset
    df = pd.read_csv(args.input)

    # Fix bad CSV (all data in one column)
    if len(df.columns) == 1:
        df = df.iloc[:, 0].str.split(",", expand=True)
        df.columns = ["timestamp","open","high","low","close","volume_btc","volume_usd"]

    # Normalize column names
    df.columns = [col.strip().lower() for col in df.columns]

    # Validate dataset
    if df.empty:
        raise ValueError("Dataset is empty")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    # Clean + convert close column properly
    df["close"] = df["close"].astype(str).str.replace('"', '').str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # Remove invalid rows
    df = df.dropna(subset=["close"])

    logging.info(f"Rows loaded: {len(df)}")

    # Rolling mean
    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    # Signal generation
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

    # Remove NaNs from rolling mean
    valid_df = df.dropna()

    # Metrics
    signal_rate = valid_df["signal"].mean()
    latency = int((time.time() - start_time) * 1000)

    metrics = {
        "version": version,
        "rows_processed": len(df),
        "metric": "signal_rate",
        "value": float(signal_rate),
        "latency_ms": latency,
        "seed": seed,
        "status": "success"
    }

    logging.info("Processing completed")
    logging.info(f"Metrics: {metrics}")

except Exception as e:
    metrics = {
        "version": config.get("version", "unknown") if 'config' in locals() else "unknown",
        "status": "error",
        "error_message": str(e)
    }

    logging.error(f"Error occurred: {str(e)}")

# Always write metrics
with open(args.output, "w") as f:
    json.dump(metrics, f)

# Print to stdout (important for Docker)
print(json.dumps(metrics))