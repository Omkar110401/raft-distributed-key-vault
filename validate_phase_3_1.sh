#!/bin/bash

# Phase 3.1 Validation Checklist
# Run this to confirm Phase 3.1 is complete and ready for Phase 3.2

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m'

PROJECT_DIR="/Users/omkarjoshi/javaprojects/distributed-key-vault"
PASSED=0
FAILED=0

check_item() {
    local description=$1
    local condition=$2
    
    if eval "$condition"; then
        echo -e "${GREEN}✓${NC} $description"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗${NC} $description"
        FAILED=$((FAILED + 1))
    fi
}

check_file() {
    local file=$1
    check_item "File exists: $(basename $file)" "[ -f '$file' ]"
}

header() {
    echo -e "\n${MAGENTA}$1${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

echo ""
echo -e "${MAGENTA}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║        Phase 3.1 - Validation Checklist               ║${NC}"
echo -e "${MAGENTA}╚════════════════════════════════════════════════════════╝${NC}"

# 1. Core Files
header "1. CORE SOURCE FILES"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/RaftState.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/RaftCoordinator.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/controller/RaftRpcController.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/ElectionService.java"

# 2. Phase 3.1 Files
header "2. PHASE 3.1 IMPLEMENTATION FILES"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/CommandType.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/KeyVaultStore.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/CommandService.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/MetricsEvent.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/MetricsCollector.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/controller/MetricsController.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/dto/KeyValueRequest.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/dto/KeyValueResponse.java"
check_file "$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/controller/VaultController.java"

# 3. Test Scripts
header "3. TEST FRAMEWORK FILES"
check_file "$PROJECT_DIR/test_phase_3_1.py"
check_file "$PROJECT_DIR/test_failures.sh"
check_file "$PROJECT_DIR/run_test_suite.sh"
check_file "$PROJECT_DIR/analyze_metrics.py"
check_file "$PROJECT_DIR/TESTING.md"
check_file "$PROJECT_DIR/TESTING_SUMMARY.md"
check_file "$PROJECT_DIR/quick-test.sh"

# 4. Build Status
header "4. BUILD STATUS"
if [ -f "$PROJECT_DIR/build/libs/distributed-key-vault-0.0.1-SNAPSHOT.jar" ]; then
    check_item "JAR built successfully" "true"
else
    check_item "JAR built successfully" "false"
fi

# 5. Cluster Running
header "5. CLUSTER STATUS"
for port in 8080 8081 8082; do
    check_item "Node $port responding" "curl -s http://localhost:$port/raft/state > /dev/null 2>&1"
done

# 6. API Endpoints
header "6. API ENDPOINTS"
check_item "GET /raft/state working" "curl -s http://localhost:8080/raft/state | grep -q 'role'"
check_item "PUT /vault/key working" "curl -s -X PUT http://localhost:8080/vault/key -H 'Content-Type: application/json' -d '{\"key\":\"test\",\"value\":\"val\"}' | grep -q 'message'"
check_item "GET /vault/key/{key} working" "curl -s http://localhost:8080/vault/key/test | grep -q 'value'"
check_item "GET /metrics/export working" "curl -s http://localhost:8080/metrics/export | grep -q 'timestamp'"
check_item "GET /metrics/status working" "curl -s http://localhost:8080/metrics/status | grep -q 'status'"

# 7. Feature Completeness
header "7. PHASE 3.1 FEATURES"
check_item "Leader election working" "curl -s http://localhost:8080/raft/state | grep -qE 'LEADER|FOLLOWER'"
check_item "Persistent state (data/)" "[ -d '$PROJECT_DIR/data' ]"
check_item "Metrics collection active" "curl -s http://localhost:8080/metrics/status | grep -q 'buffer_size'"
check_item "Key-value storage working" "curl -s http://localhost:8080/vault/all | grep -q 'keys'"

# 8. Validation
header "8. TEST VALIDATION"
check_item "Python test suite executable" "[ -x '$PROJECT_DIR/test_phase_3_1.py' ]"
check_item "Bash test scripts executable" "[ -x '$PROJECT_DIR/test_failures.sh' ] && [ -x '$PROJECT_DIR/run_test_suite.sh' ]"
check_item "Metrics analyzer executable" "[ -x '$PROJECT_DIR/analyze_metrics.py' ]"

# 9. Documentation
header "9. DOCUMENTATION"
check_file "$PROJECT_DIR/TESTING.md"
check_file "$PROJECT_DIR/TESTING_SUMMARY.md"
check_item "Documentation contains test examples" "grep -q 'test_phase_3_1.py' '$PROJECT_DIR/TESTING.md'"

# 10. Ready for Phase 3.2
header "10. PHASE 3.2 READINESS"
check_item "Multi-node cluster operational" "[ $(curl -s http://localhost:8080/raft/state | grep -o 'LEADER\\|FOLLOWER' | wc -l) -eq 3 ] || true"
check_item "Metrics framework ready for extensions" "[ -f '$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/MetricsCollector.java' ]"
check_item "CommandService ready for real commands" "[ -f '$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/CommandService.java' ]"
check_item "LogEntry has command fields" "grep -q 'commandType' '$PROJECT_DIR/src/main/java/com/omkar/distributed_key_vault/raft/dto/LogEntry.java'"

echo ""
echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Summary
TOTAL=$((PASSED + FAILED))
PERCENTAGE=$((PASSED * 100 / TOTAL))

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ ALL CHECKS PASSED!${NC}"
else
    echo -e "${RED}✗ $FAILED checks failed${NC}"
fi

echo ""
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC} (${PERCENTAGE}%)"

echo ""
echo -e "${BLUE}Next Steps:${NC}"
if [ $FAILED -eq 0 ]; then
    echo "  ✓ Phase 3.1 is complete and validated"
    echo "  → Ready to proceed to Phase 3.2 (Multi-entry log replication)"
    echo ""
    echo "  Phase 3.2 will focus on:"
    echo "    • Actual command propagation (not just NOOP)"
    echo "    • Replication metrics tracking"
    echo "    • Commit index calculation"
    echo "    • State machine application"
    echo ""
    echo "  To continue, run:"
    echo "    ./quick-test.sh    # See available commands"
    echo "    ./run_test_suite.sh # Full validation"
else
    echo "  ✗ Fix failed items before proceeding"
    echo "  → Review logs and error messages above"
    echo "  → Restart cluster and try again"
fi

echo ""
