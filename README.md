# MLOps Batch Job

## Overview

This project implements a minimal MLOps-style batch pipeline:

* Loads configuration from YAML
* Reads OHLCV dataset
* Computes rolling mean on `close`
* Generates binary signal
* Outputs structured metrics and logs

---

## Local Run Instructions

Run the script locally:

```
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

After execution:

* `metrics.json` will be generated
* `run.log` will contain execution logs

---

## Docker Build & Run

Build the Docker image:

```
docker build -t mlops-task .
```

Run the container:

```
docker run --rm mlops-task
```

The container will:

* Execute the pipeline
* Print metrics JSON to stdout
* Generate `metrics.json` and `run.log`

---

## Example metrics.json

```
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.49909963985594236,
  "latency_ms": 108,
  "seed": 42,
  "status": "success"
}
```

---

## Notes

* The pipeline is deterministic using a fixed random seed
* Handles malformed CSV input (single-column issue)
* Includes validation and error handling
* Logs all major steps for observability
