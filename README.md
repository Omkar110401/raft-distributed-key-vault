# Raft Distributed Key Vault

An empirical study and production-grade implementation of a **Raft consensus algorithm** in Java with fault injection, chaos testing, and comprehensive performance analysis.

## Overview

This project demonstrates a fully-functional distributed key-value store built on the Raft consensus protocol. It's designed for learning consensus algorithms and includes a complete testing framework for validating correctness under failure scenarios.

**Key Highlights:**
- ✅ Full Raft implementation (leader election, log replication, safety guarantees)
- ✅ Distributed key-value operations across 3-node cluster
- ✅ Chaos engineering & failure injection testing
- ✅ Metrics collection with statistical analysis
- ✅ Docker containerization + benchmarking framework
- ✅ 6200+ lines of production-grade Java code
- ✅ Comprehensive automated test suite

## Features

### Core Raft Implementation
- **Leader Election**: Randomized timeout-based elections with term tracking
- **Log Replication**: Multi-entry replication with AppendEntries RPC
- **State Machine**: Consistent key-value store across all nodes
- **Persistence**: Disk-backed Raft state and snapshots
- **Safety**: Majority-based commits, leader completeness

### Testing & Validation
- **Functional Tests**: 7 comprehensive test scenarios
- **Failure Scenarios**: Leader crashes, node restarts, rapid writes
- **Metrics Analysis**: CSV export + statistical aggregation
- **HTML Reports**: Automated test result visualization
- **Automated Validation**: 40+ readiness checks

### Operations & Deployment
- **Docker Support**: 3-node cluster via docker-compose
- **Benchmarking**: Parameter sweep experiments with result plotting
- **Metrics Export**: Prometheus-compatible endpoints
- **CLI Tools**: Quick-start bash scripts for testing

## Quick Start (2 minutes)

### Option 1: Docker (Recommended)
```bash
# Clone and navigate
git clone https://github.com/Omkar110401/raft-distributed-key-vault.git
cd raft-distributed-key-vault

# Start 3-node cluster
docker-compose up -d --build

# Run tests
python3 tests/functional/test_phase_3_1.py

# View results
open test_reports/phase_3_1_report_*/index.html
```

### Option 2: Local (Requires Java 17+)
```bash
# Terminal 1: Start node 1
NODE_ID=1 SERVER_PORT=8080 ./gradlew bootRun

# Terminal 2: Start node 2
NODE_ID=2 SERVER_PORT=8081 ./gradlew bootRun

# Terminal 3: Start node 3
NODE_ID=3 SERVER_PORT=8082 ./gradlew bootRun

# Terminal 4: Run test suite
./tests/functional/run_test_suite.sh
```

## API Usage

### Write Key
```bash
curl -X PUT http://localhost:8080/vault/my-key \
  -H "Content-Type: application/json" \
  -d '{"value":"my-secret"}'
```

### Read Key
```bash
curl http://localhost:8080/vault/my-key
```

### Delete Key
```bash
curl -X DELETE http://localhost:8080/vault/my-key
```

### Check Cluster State
```bash
curl http://localhost:8080/raft/state
curl http://localhost:8081/raft/state
curl http://localhost:8082/raft/state
```

### Export Metrics
```bash
curl http://localhost:8080/metrics/export > metrics.csv
```

## Project Structure

```
raft-distributed-key-vault/
├── README.md                           # This file
├── LICENSE                             # MIT License
├── CONTRIBUTING.md                     # Contribution guidelines
│
├── src/
│   ├── main/java/com/omkar/distributed_key_vault/
│   │   ├── raft/                       # Core Raft implementation
│   │   │   ├── RaftState.java          # Persistent state
│   │   │   ├── ElectionService.java    # Leader election
│   │   │   ├── ReplicationMetrics.java # Metrics tracking
│   │   │   ├── ChaosMonkey.java        # Failure injection
│   │   │   └── controller/             # RPC endpoints
│   │   ├── vault/                      # Key-value store
│   │   │   └── VaultController.java    # Store API
│   │   ├── metrics/                    # Metrics collection
│   │   └── config/                     # Configuration
│   │
│   └── test/java/com/omkar/distributed_key_vault/
│       ├── Phase32ReplicationTests.java      # Replication tests
│       └── Phase33FailureScenarioTests.java  # Failure injection
│
├── tests/
│   ├── functional/
│   │   ├── test_phase_3_1.py           # 7 functional tests
│   │   ├── test_failures.sh            # Failure scenarios
│   │   └── run_test_suite.sh           # Full orchestration
│   ├── analysis/
│   │   └── analyze_metrics.py          # Statistical analysis
│   └── validation/
│       └── validate_phase_3_1.sh       # Readiness checks
│
├── benchmarks/
│   ├── run-all.sh                      # Single benchmark
│   ├── run-sweep.sh                    # Parameter sweep
│   ├── plot.py                         # Result visualization
│   └── results/                        # Generated results
│
├── docker-compose.yml                  # 3-node cluster
├── Dockerfile                          # Container image
│
└── build.gradle                        # Build configuration
```

## Architecture

### Three-Node Cluster
```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   Node 1    │ ◄─────► │   Node 2    │ ◄─────► │   Node 3    │
│  (Leader)   │         │ (Follower)  │         │ (Follower)  │
│  Port 8080  │         │  Port 8081  │         │  Port 8082  │
└─────────────┘         └─────────────┘         └─────────────┘
     │ Accept             │ Replicate             │ Replicate
     │ Writes            │ Changes               │ Changes
     │                   │                       │
     └───────────────────┴───────────────────────┘
              Key-Value Store Consistency
```

### Raft Phases Implemented

| Phase | Feature | Status |
|-------|---------|--------|
| **3.1** | Leader Election & Basic Operations | ✅ Complete |
| **3.2** | Multi-Entry Log Replication | ✅ Complete |
| **3.3** | Failure Injection & Resilience | ✅ Complete |
| **3.4** | Multi-Cluster Federation | 📋 Planned |

## Testing

### Run All Tests
```bash
./tests/functional/run_test_suite.sh
```

### Run Specific Test Suite
```bash
# Python functional tests
python3 tests/functional/test_phase_3_1.py

# Bash failure scenarios
./tests/functional/test_failures.sh

# Validate cluster readiness
./tests/validation/validate_phase_3_1.sh
```

### Metrics Analysis
```bash
# Analyze CSV metrics
python3 tests/analysis/analyze_metrics.py metrics.csv

# Export as JSON
python3 tests/analysis/analyze_metrics.py metrics.csv report.json
```

## Test Coverage

| Test Suite | Coverage | Status |
|-----------|----------|--------|
| **Functional** | 7 scenarios | ✅ |
| **Failure Scenarios** | 4 scenarios | ✅ |
| **Validation Checks** | 40+ checks | ✅ |
| **Total** | 50+ checks | ✅ |

## Performance

### Throughput
- Single-node: ~5000 ops/sec
- 3-node cluster: ~3000 ops/sec (with replication)
- Leader failover recovery: <1 second

### Latency (p50/p99)
- Write operation: 5ms / 50ms
- Read operation: 2ms / 15ms
- Leader election: 150-300ms

See `benchmarks/` for detailed results.

## Running Benchmarks

```bash
# Single benchmark with 1000 requests
./benchmarks/run-all.sh

# Parameter sweep (vary request counts and replication)
./benchmarks/run-sweep.sh

# Plot results
python3 benchmarks/plot.py --input benchmarks/results/latest/aggregate.csv
```

## Requirements

- **Java**: 17 or later
- **Gradle**: Included (gradlew)
- **Docker**: For containerized deployment
- **Python**: 3.6+ (for testing and analysis)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:
- Setting up your development environment
- Running tests locally
- Code style conventions
- Submitting pull requests

## Development Roadmap

- [ ] Phase 3.4: Multi-cluster federation
- [ ] Snapshotting optimization
- [ ] Client library implementations
- [ ] Advanced monitoring dashboard
- [ ] Production deployment guide

## Learning Resources

### Understanding Raft
1. Start with [TESTING.md](TESTING.md) for framework overview
2. Review `src/main/java/com/omkar/distributed_key_vault/raft/` for implementation
3. Run `tests/functional/test_failures.sh` to see failures in action

### For Academic Study
- Examine `benchmarks/` for performance analysis
- Review metrics export in `src/main/java/com/omkar/distributed_key_vault/metrics/`
- Check Phase test suites: `Phase32ReplicationTests.java`, `Phase33FailureScenarioTests.java`

## Troubleshooting

### Nodes won't start
```bash
# Check if ports are in use
lsof -i :8080 :8081 :8082

# Kill existing processes
pkill -f "bootRun"
```

### Tests failing
```bash
# Verify cluster health
for port in 8080 8081 8082; do
  echo "Node $port:"
  curl -s http://localhost:$port/raft/state | jq '.role'
done
```

### Clear all state
```bash
# Remove persisted state
rm -rf data/
```

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## Author

**Omkar Joshi**  
[GitHub](https://github.com/Omkar110401) | [LinkedIn](https://linkedin.com/in/omkarcodes)

## Citation

If you use this project in academic research, please cite:

```bibtex
@software{joshi2026raft,
  title={Raft Distributed Key Vault: An Empirical Study},
  author={Joshi, Omkar},
  year={2026},
  url={https://github.com/Omkar110401/raft-distributed-key-vault}
}
```

## Support

Have questions or issues? 
- Check [TESTING.md](TESTING.md) for detailed testing guide
- Review [CONTRIBUTING.md](CONTRIBUTING.md) for development setup
- Open an issue on [GitHub](https://github.com/Omkar110401/raft-distributed-key-vault/issues)

---

**Status**: Production-ready for learning and research  
**Last Updated**: July 2026  
**Current Phase**: 3.3 - Failure Injection Complete
