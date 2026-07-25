# Phase 3.1 Testing Framework

## Overview

This directory contains automated testing scripts for Phase 3.1 of the Distributed Key Vault project. The framework includes:

1. **test_phase_3_1.py** - Comprehensive functional test suite (Python)
2. **test_failures.sh** - Interactive failure scenario testing (Bash)
3. **run_test_suite.sh** - Full orchestration with reporting (Bash)
4. **analyze_metrics.py** - Metrics analysis and JSON reporting (Python)

## Quick Start

### Prerequisites

- All 3 nodes running (ports 8080, 8081, 8082)
- Python 3.6+
- curl installed

### Run All Tests

```bash
cd /Users/omkarjoshi/javaprojects/distributed-key-vault
./run_test_suite.sh
```

This will:
- ✓ Check cluster status
- ✓ Run basic functionality tests
- ✓ Collect metrics from all nodes
- ✓ Analyze metrics
- ✓ Generate HTML report with results

Reports are saved to: `test_reports/phase_3_1_report_YYYYMMDD_HHMMSS/`

## Test Scripts

### 1. test_phase_3_1.py

Comprehensive Python test suite covering all Phase 3.1 scenarios.

**Usage:**
```bash
python3 test_phase_3_1.py
```

**Test Scenarios:**
- ✓ test_all_nodes_accessible() - Verify cluster responsive
- ✓ test_basic_operations() - Write → Read → Delete workflow
- ✓ test_follower_rejection() - 403 on follower writes
- ✓ test_follower_read_rejection() - 403 on follower reads
- ✓ test_bulk_writes() - 10 sequential writes
- ✓ test_metrics_export() - Retrieve CSV metrics
- ✓ test_metrics_status() - Check buffer utilization

**Output:**
```
✓ test_all_nodes_accessible - PASS (1.05s)
✓ test_basic_operations - PASS (2.34s)
✓ test_follower_rejection - PASS (1.23s)
✓ test_follower_read_rejection - PASS (1.10s)
✓ test_bulk_writes - PASS (5.23s)
✓ test_metrics_export - PASS (0.87s)
✓ test_metrics_status - PASS (0.56s)

============================================
Test Results: 7/7 PASSED ✓
Total Time: 12.38 seconds
============================================
```

### 2. test_failures.sh

Interactive menu-driven failure scenario testing.

**Usage:**
```bash
./test_failures.sh
```

**Available Scenarios:**

1. **Leader Crash & Failover**
   - Identifies current leader
   - Writes test data
   - Simulates leader crash (manual)
   - Waits for failover election
   - Verifies new leader functionality
   - Validates data persistence

2. **Node Restart**
   - Gets cluster state
   - Writes data
   - Kills follower (manual)
   - Waits 30 seconds
   - Restarts follower
   - Verifies rejoin and correct state

3. **Rapid Writes During Churn**
   - Performs 20 sequential writes
   - Measures success rate
   - Verifies all writes persisted

4. **Metrics Consistency Across Nodes**
   - Exports metrics from all nodes
   - Analyzes election events per node
   - Compares consistency

**Example Run:**
```bash
$ ./test_failures.sh

[HEADER] ==========================================
[HEADER] SCENARIO 1: Leader Crash & Failover
[HEADER] ==========================================
[INFO] Step 1: Identify current leader
[✓] Leader found on port: 8080
[INFO] Step 2: Write test data to leader
[✓] Data written successfully
[INFO] Step 3: Kill the leader (this will happen externally)
[WARN] Please manually kill the leader process in 30 seconds
[INFO] Step 4: Wait for new leader election (20 seconds)
[✓] New leader elected on port: 8081
```

### 3. run_test_suite.sh

Full end-to-end test orchestration with comprehensive reporting.

**Usage:**
```bash
./run_test_suite.sh
```

**What It Does:**
1. Checks all 3 nodes are responding
2. Runs basic functionality tests
3. Collects metrics from all nodes
4. Analyzes metrics for patterns
5. Generates HTML report
6. Saves all artifacts in timestamped directory

**Output Structure:**
```
test_reports/phase_3_1_report_20250117_143022/
├── index.html                 # HTML summary report
├── metrics/
│   ├── metrics_8080.csv      # Raw events from node 8080
│   ├── metrics_8081.csv      # Raw events from node 8081
│   ├── metrics_8082.csv      # Raw events from node 8082
│   ├── analysis_8080.json    # Analyzed metrics for node 8080
│   ├── analysis_8081.json    # Analyzed metrics for node 8081
│   ├── analysis_8082.json    # Analyzed metrics for node 8082
│   ├── state_8080.json       # Node state snapshot
│   ├── state_8081.json       # Node state snapshot
│   └── state_8082.json       # Node state snapshot
```

### 4. analyze_metrics.py

Standalone metrics analyzer for detailed performance analysis.

**Usage:**
```bash
# Analyze specific node metrics
python3 analyze_metrics.py /tmp/metrics_8080.csv

# Export analysis as JSON
python3 analyze_metrics.py /tmp/metrics_8080.csv /tmp/analysis.json
```

**Output:**
```
✓ Loaded 47 events from /tmp/metrics_8080.csv

============================================================
METRICS ANALYSIS SUMMARY
============================================================

1. EVENT DISTRIBUTION:
   ELECTION_START: 1
   ELECTION_END: 1
   ROLE_CHANGE: 2
   READ_REQUEST: 3
   WRITE_REQUEST: 5
   STATE_MACHINE_APPLY: 5

2. NODES TRACKED: 1
   Node IDs: ['1']

3. ELECTIONS: 1
   Election Latencies (ms):
     Min: 156.23
     Max: 156.23
     Mean: 156.23

4. TERM CHANGES: 2

5. REPLICATION EVENTS: 0

6. OPERATIONS:
   Writes: 5
   Reads: 3

7. TIME RANGE:
   First: 2025-01-17T14:30:22.123456Z
   Last: 2025-01-17T14:31:45.654321Z
```

## Metrics CSV Format

Each node exports metrics in CSV format:

```csv
timestamp,node_id,event_type,term,previous_value,new_value,latency_ms,details
2025-01-17T14:30:22.123456Z,1,ELECTION_START,1,,,,timeout
2025-01-17T14:30:22.279679Z,1,ELECTION_END,1,,,,WON
2025-01-17T14:30:22.279729Z,1,ROLE_CHANGE,1,FOLLOWER,LEADER,,
2025-01-17T14:30:47.342567Z,1,WRITE_REQUEST,,key1,SUCCESS,1,
2025-01-17T14:30:50.876543Z,1,READ_REQUEST,,key1,SUCCESS,0,
```

**Fields:**
- `timestamp`: ISO 8601 with nanosecond precision
- `node_id`: Node identifier (1, 2, 3)
- `event_type`: ELECTION_START, ELECTION_END, ROLE_CHANGE, TERM_CHANGE, WRITE_REQUEST, READ_REQUEST, STATE_MACHINE_APPLY, LOG_REPLICATION_*
- `term`: Raft term at event time
- `previous_value`: Previous value (for state changes)
- `new_value`: New value (for state changes)
- `latency_ms`: Operation latency (for timed operations)
- `details`: Additional context

## Testing Workflow

### For Phase 3.1 Validation

```bash
# 1. Ensure cluster is running
# 2. Run full test suite
./run_test_suite.sh

# 3. Review HTML report
open test_reports/phase_3_1_report_*/index.html

# 4. Run individual Python tests for debugging
python3 test_phase_3_1.py

# 5. Analyze specific metrics
python3 analyze_metrics.py test_reports/phase_3_1_report_*/metrics/metrics_8080.csv
```

### For Failure Scenario Testing

```bash
# 1. Run interactive failure scenario menu
./test_failures.sh

# 2. Choose scenario 1-4
# 3. Follow prompts
# 4. Manually kill/restart nodes as instructed
# 5. Export metrics: curl http://localhost:8080/metrics/export > report.csv
# 6. Analyze: python3 analyze_metrics.py report.csv

# OR: Run rapid writes test
./test_failures.sh
# Choose option 3
```

## Integration with Paper

### Collecting Data for Conference Paper

**Baseline Metrics (Healthy Cluster):**
```bash
./run_test_suite.sh
# Store: reports/baseline_healthy_cluster_*.json
```

**Failover Scenarios (for "Leadership Stability" section):**
```bash
# Run 10 times: Leader crash → collect metrics
for i in {1..10}; do
    ./test_failures.sh  # Scenario 1
    cp test_reports/phase_3_1_report_*/metrics/metrics_*.csv reports/failover_run_$i.csv
done

# Analyze across runs
python3 analyze_metrics.py reports/failover_run_*.csv > reports/failover_analysis.json
```

**Churn Scenarios (for "Performance Under Stress" section):**
```bash
./test_failures.sh  # Scenario 3: Rapid Writes
# Collect metrics with churn ongoing
```

## Debugging Failed Tests

### If nodes not responding

```bash
# Check if servers are running
curl -I http://localhost:8080/raft/state

# Check server logs
tail -f logs/node*.log
```

### If metrics not exporting

```bash
# Verify metrics endpoint works
curl http://localhost:8080/metrics/status

# Check buffer capacity
# Output should show: "buffer_size": 10000, "current_events": <count>

# If full, clear and retry
curl -X DELETE http://localhost:8080/metrics/clear
```

### If tests hang

- Press Ctrl+C to stop
- Check network connectivity: `ping localhost`
- Restart cluster and try again

## Next Steps

1. **Phase 3.2**: Implement multi-entry log replication
   - Current system: Only NOOP entries
   - Next: Actual command propagation to followers
   - Metrics: Track replication lag per entry

2. **Phase 4**: Failure injection framework
   - Automate leader kill/restart
   - Network partition simulation
   - Latency injection
   - Run 50+ iterations per scenario

3. **Phase 5**: Statistical analysis
   - Mean/median election latency
   - P99 latency percentiles
   - Term growth patterns
   - Leadership churn metrics

4. **Paper Writing**
   - Use metrics to generate figures
   - Election latency distribution plots
   - Term growth over time
   - Compare against Raft paper baseline

## Contact & Issues

For test framework issues:
1. Check that all 3 nodes are running
2. Verify ports 8080-8082 are accessible
3. Clear metrics buffer if full
4. Check disk space in ./data directory
5. Review server logs for exceptions

---

**Last Updated**: 2025-01-17
**Test Framework Version**: 1.0
