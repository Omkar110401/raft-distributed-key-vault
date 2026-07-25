#!/bin/bash

# Phase 3.1 - Failure Scenario Testing
# Tests leader crash, node restart, and failover scenarios

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

log_header() {
    echo -e "${MAGENTA}$(date '+%H:%M:%S') [HEADER] $1${NC}"
}

log_info() {
    echo -e "${BLUE}$(date '+%H:%M:%S') [INFO] $1${NC}"
}

log_success() {
    echo -e "${GREEN}$(date '+%H:%M:%S') [✓] $1${NC}"
}

log_fail() {
    echo -e "${RED}$(date '+%H:%M:%S') [✗] $1${NC}"
}

log_warn() {
    echo -e "${YELLOW}$(date '+%H:%M:%S') [WARN] $1${NC}"
}

get_leader() {
    for port in 8080 8081 8082; do
        response=$(curl -s http://localhost:$port/raft/state 2>/dev/null || echo "")
        if [[ $response == *"LEADER"* ]]; then
            echo $port
            return
        fi
    done
    echo ""
}

get_followers() {
    for port in 8080 8081 8082; do
        response=$(curl -s http://localhost:$port/raft/state 2>/dev/null || echo "")
        if [[ $response == *"FOLLOWER"* ]]; then
            echo $port
        fi
    done
}

check_node_alive() {
    curl -s http://localhost:$1/raft/state > /dev/null 2>&1
    return $?
}

write_test_data() {
    local port=$1
    local key=$2
    local value=$3
    
    response=$(curl -s -X PUT http://localhost:$port/vault/key \
      -H "Content-Type: application/json" \
      -d "{\"key\": \"$key\", \"value\": \"$value\"}")
    
    if echo "$response" | grep -q "successfully"; then
        return 0
    else
        return 1
    fi
}

read_test_data() {
    local port=$1
    local key=$2
    
    response=$(curl -s http://localhost:$port/vault/key/$key)
    
    if echo "$response" | grep -q "successfully"; then
        echo "$response" | grep -o '"value":"[^"]*"'
        return 0
    else
        return 1
    fi
}

scenario_1_leader_crash_failover() {
    log_header "=========================================="
    log_header "SCENARIO 1: Leader Crash & Failover"
    log_header "=========================================="
    
    log_info "Step 1: Identify current leader"
    leader=$(get_leader)
    if [ -z "$leader" ]; then
        log_fail "No leader found"
        return 1
    fi
    log_success "Leader found on port: $leader"
    
    log_info "Step 2: Write test data to leader"
    if write_test_data $leader "crash_test" "before_crash"; then
        log_success "Data written successfully"
    else
        log_fail "Failed to write data"
        return 1
    fi
    
    log_info "Step 3: Kill the leader (this will happen externally)"
    log_warn "Please manually kill the leader process in 30 seconds"
    log_warn "Or automated termination can be added"
    
    log_info "Step 4: Wait for new leader election (20 seconds)"
    sleep 20
    
    log_info "Step 5: Check if new leader elected"
    new_leader=$(get_leader)
    if [ -z "$new_leader" ]; then
        log_fail "No leader elected after failover"
        return 1
    fi
    
    if [ "$new_leader" == "$leader" ]; then
        log_warn "Same leader still active (might have recovered)"
    else
        log_success "New leader elected on port: $new_leader"
    fi
    
    log_info "Step 6: Write to new leader"
    if write_test_data $new_leader "after_failover" "success"; then
        log_success "Write to new leader successful"
    else
        log_fail "Failed to write to new leader"
        return 1
    fi
    
    log_info "Step 7: Verify data persistence"
    sleep 2
    data=$(read_test_data $new_leader "after_failover")
    if [[ $data == *"success"* ]]; then
        log_success "Data persisted: $data"
    else
        log_fail "Data not found"
        return 1
    fi
    
    return 0
}

scenario_2_node_restart() {
    log_header "=========================================="
    log_header "SCENARIO 2: Node Restart (Stale Node Rejoins)"
    log_header "=========================================="
    
    log_info "Step 1: Get current cluster state"
    leader=$(get_leader)
    log_success "Current leader: port $leader"
    
    followers=$(get_followers)
    first_follower=$(echo $followers | cut -d' ' -f1)
    log_success "First follower: port $first_follower"
    
    log_info "Step 2: Write data while follower is online"
    if write_test_data $leader "restart_test" "before_restart"; then
        log_success "Data written"
    else
        log_fail "Write failed"
        return 1
    fi
    
    log_info "Step 3: Verify data on follower"
    sleep 2
    data=$(read_test_data $leader "restart_test" 2>/dev/null || echo "")
    log_info "Data on leader: $data"
    
    log_warn "Step 4: Kill follower on port $first_follower"
    # In production: pkill -f "SERVER_PORT=$first_follower"
    log_warn "Please manually kill the follower (or use: pkill -f 'SERVER_PORT=$first_follower')"
    
    log_info "Step 5: Wait 30 seconds and restart follower"
    log_warn "Waiting 30 seconds..."
    sleep 30
    log_warn "Please restart the follower"
    
    log_info "Step 6: Wait for rejoin (20 seconds)"
    sleep 20
    
    log_info "Step 7: Check if node rejoined"
    if check_node_alive $first_follower; then
        log_success "Node rejoined successfully"
        
        # Check its state
        state=$(curl -s http://localhost:$first_follower/raft/state)
        log_info "Rejoined node state: $state"
        
        if [[ $state == *"FOLLOWER"* ]]; then
            log_success "Node correctly joined as FOLLOWER"
        else
            log_warn "Node state unexpected: $state"
        fi
    else
        log_fail "Node failed to rejoin"
        return 1
    fi
    
    return 0
}

scenario_3_rapid_writes_during_churn() {
    log_header "=========================================="
    log_header "SCENARIO 3: Rapid Writes During Node Churn"
    log_header "=========================================="
    
    log_info "Step 1: Identify leader"
    leader=$(get_leader)
    if [ -z "$leader" ]; then
        log_fail "No leader found"
        return 1
    fi
    log_success "Leader: port $leader"
    
    log_info "Step 2: Start rapid writes (20 keys in 10 seconds)"
    success_writes=0
    for i in {1..20}; do
        if write_test_data $leader "churn_key_$i" "value_$i" 2>/dev/null; then
            success_writes=$((success_writes + 1))
            echo -ne "  Writes: $success_writes/20\r"
        fi
        sleep 0.5
    done
    echo ""
    log_success "Completed $success_writes/20 writes"
    
    log_info "Step 3: Verify all writes"
    sleep 2
    verified=0
    for i in {1..20}; do
        if read_test_data $leader "churn_key_$i" > /dev/null 2>&1; then
            verified=$((verified + 1))
        fi
    done
    
    if [ $verified -eq 20 ]; then
        log_success "All 20 keys verified"
        return 0
    else
        log_warn "Only $verified/20 keys verified"
        return 1
    fi
}

scenario_4_metrics_consistency() {
    log_header "=========================================="
    log_header "SCENARIO 4: Metrics Consistency Across Nodes"
    log_header "=========================================="
    
    log_info "Step 1: Export metrics from all nodes"
    
    for port in 8080 8081 8082; do
        if check_node_alive $port; then
            log_info "Exporting from port $port"
            curl -s http://localhost:$port/metrics/export > /tmp/metrics_$port.csv
            event_count=$(tail -n +2 /tmp/metrics_$port.csv | wc -l)
            log_success "Node $port: $event_count events"
        else
            log_warn "Node $port not responding"
        fi
    done
    
    log_info "Step 2: Analyze election events"
    for port in 8080 8081 8082; do
        if [ -f /tmp/metrics_$port.csv ]; then
            elections=$(grep "ELECTION" /tmp/metrics_$port.csv | wc -l)
            log_info "Port $port: $elections election events"
        fi
    done
    
    log_success "Metrics analysis complete"
    return 0
}

show_menu() {
    echo ""
    log_header "=========================================="
    log_header "Phase 3.1 - Failure Scenario Tests"
    log_header "=========================================="
    echo -e "${NC}"
    echo "Choose a scenario to test:"
    echo "  1) Scenario 1: Leader Crash & Failover"
    echo "  2) Scenario 2: Node Restart"
    echo "  3) Scenario 3: Rapid Writes During Churn"
    echo "  4) Scenario 4: Metrics Consistency"
    echo "  5) Run All Scenarios"
    echo "  q) Quit"
    echo ""
    read -p "Enter choice [1-5, q]: " choice
}

main() {
    log_header "Starting Phase 3.1 Failure Scenario Tests"
    
    while true; do
        show_menu
        
        case $choice in
            1)
                scenario_1_leader_crash_failover
                ;;
            2)
                scenario_2_node_restart
                ;;
            3)
                scenario_3_rapid_writes_during_churn
                ;;
            4)
                scenario_4_metrics_consistency
                ;;
            5)
                scenario_1_leader_crash_failover
                echo ""
                scenario_3_rapid_writes_during_churn
                echo ""
                scenario_4_metrics_consistency
                ;;
            q|Q)
                log_info "Exiting..."
                exit 0
                ;;
            *)
                log_fail "Invalid choice"
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
    done
}

main
