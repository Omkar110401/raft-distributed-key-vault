This folder contains quick instructions to run experiments locally using Docker.

1) Build and run a 3-node cluster (local ports 8080,8081,8082):

```bash
# from repository root
docker-compose up -d --build
```

2) Run a single benchmark and export CSV using the provided script:

```bash
# make script executable once
chmod +x benchmarks/run-all.sh
# run
benchmarks/run-all.sh
```

3) Results are saved under `benchmarks/results/<timestamp>/` as `results.json` and `results.csv`.

Notes:
- The Docker image builds the project using the bundled Gradle wrapper. This is convenient but may take time the first run.
- For many repeated experiments, consider building the image once and using `docker-compose up --scale node=3` or running the jar directly on hosts.
- Tweak `benchmarks/run-all.sh` to change workload parameters or sweep configurations.

Run sweep script
----------------

A more advanced runner `benchmarks/run-sweep.sh` performs parameter sweeps, repeats runs, and produces an aggregated CSV. Example:

```bash
# make executable once
chmod +x benchmarks/run-sweep.sh
benchmarks/run-sweep.sh
```

Outputs:
- `benchmarks/results/<timestamp>/*.json` — raw run outputs
- `benchmarks/results/<timestamp>/*.csv` — per-run CSVs
- `benchmarks/results/<timestamp>/aggregate.csv` — concatenated CSV for analysis

Edit `benchmarks/run-sweep.sh` to change the parameter grid and repeat count.
