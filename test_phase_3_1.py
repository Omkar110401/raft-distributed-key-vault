#!/usr/bin/env python3
"""
Phase 3.1 Testing Suite - Distributed Key Vault
Comprehensive test scenarios with metrics collection
"""

import requests
import json
import time
import sys
from datetime import datetime
from typing import Dict, List, Tuple
import subprocess
import os

class VaultTester:
    def __init__(self):
        self.nodes = {
            'node-1': 'http://localhost:8080',
            'node-2': 'http://localhost:8081',
            'node-3': 'http://localhost:8082'
        }
        self.results = []
        self.start_time = datetime.now()
        
    def log(self, message: str, level: str = "INFO"):
        """Print colored log messages"""
        colors = {
            "INFO": "\033[94m",      # Blue
            "SUCCESS": "\033[92m",   # Green
            "FAIL": "\033[91m",      # Red
            "WARN": "\033[93m",      # Yellow
            "HEADER": "\033[95m"     # Magenta
        }
        reset = "\033[0m"
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"{colors.get(level, '')}[{timestamp}] {level}: {message}{reset}")
    
    def get_leader(self) -> Tuple[str, str]:
        """Find the current leader"""
        for node_id, url in self.nodes.items():
            try:
                resp = requests.get(f"{url}/raft/state", timeout=2)
                if resp.status_code == 200:
                    state = resp.text
                    if "LEADER" in state:
                        return node_id, url
            except:
                pass
        return None, None
    
    def get_followers(self) -> List[Tuple[str, str]]:
        """Get all follower nodes"""
        followers = []
        for node_id, url in self.nodes.items():
            try:
                resp = requests.get(f"{url}/raft/state", timeout=2)
                if resp.status_code == 200:
                    state = resp.text
                    if "FOLLOWER" in state:
                        followers.append((node_id, url))
            except:
                pass
        return followers
    
    def test_basic_operations(self):
        """Test 1: Basic Write/Read/Delete"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 1: Basic Operations (Write/Read/Delete)", "HEADER")
        self.log("=" * 60, "HEADER")
        
        leader_id, leader_url = self.get_leader()
        if not leader_url:
            self.log("No leader found!", "FAIL")
            return False
        
        self.log(f"Leader: {leader_id}", "INFO")
        
        # Write
        self.log("Writing key: username=alice", "INFO")
        try:
            resp = requests.put(
                f"{leader_url}/vault/key",
                json={"key": "username", "value": "alice"},
                timeout=5
            )
            if resp.status_code == 200:
                self.log("✓ Write successful", "SUCCESS")
            else:
                self.log(f"✗ Write failed: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Write error: {e}", "FAIL")
            return False
        
        # Read
        self.log("Reading key: username", "INFO")
        try:
            resp = requests.get(f"{leader_url}/vault/key/username", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("value") == "alice":
                    self.log(f"✓ Read successful: {data['value']}", "SUCCESS")
                else:
                    self.log(f"✗ Read value mismatch: {data.get('value')}", "FAIL")
                    return False
            else:
                self.log(f"✗ Read failed: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Read error: {e}", "FAIL")
            return False
        
        # Delete
        self.log("Deleting key: username", "INFO")
        try:
            resp = requests.delete(f"{leader_url}/vault/key/username", timeout=5)
            if resp.status_code == 200:
                self.log("✓ Delete successful", "SUCCESS")
            else:
                self.log(f"✗ Delete failed: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Delete error: {e}", "FAIL")
            return False
        
        # Verify deletion
        self.log("Verifying deletion", "INFO")
        try:
            resp = requests.get(f"{leader_url}/vault/key/username", timeout=5)
            if resp.status_code == 404:
                self.log("✓ Key confirmed deleted", "SUCCESS")
            else:
                self.log(f"✗ Key still exists: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Verification error: {e}", "FAIL")
            return False
        
        self.log("TEST 1 PASSED\n", "SUCCESS")
        return True
    
    def test_follower_rejection(self):
        """Test 2: Follower Rejects Writes"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 2: Follower Rejects Writes", "HEADER")
        self.log("=" * 60, "HEADER")
        
        followers = self.get_followers()
        if not followers:
            self.log("No followers found!", "WARN")
            return False
        
        follower_id, follower_url = followers[0]
        self.log(f"Using follower: {follower_id}", "INFO")
        
        # Try write on follower
        self.log("Attempting write on follower", "INFO")
        try:
            resp = requests.put(
                f"{follower_url}/vault/key",
                json={"key": "test", "value": "value"},
                timeout=5
            )
            if resp.status_code == 403:
                self.log("✓ Follower correctly rejected write (403)", "SUCCESS")
                return True
            else:
                self.log(f"✗ Unexpected status code: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Error: {e}", "FAIL")
            return False
    
    def test_follower_read_rejection(self):
        """Test 3: Follower Rejects Reads"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 3: Follower Rejects Reads", "HEADER")
        self.log("=" * 60, "HEADER")
        
        followers = self.get_followers()
        if not followers:
            self.log("No followers found!", "WARN")
            return False
        
        follower_id, follower_url = followers[0]
        self.log(f"Using follower: {follower_id}", "INFO")
        
        # Try read on follower
        self.log("Attempting read on follower", "INFO")
        try:
            resp = requests.get(f"{follower_url}/vault/key/any", timeout=5)
            if resp.status_code == 403:
                self.log("✓ Follower correctly rejected read (403)", "SUCCESS")
                return True
            else:
                self.log(f"✗ Unexpected status code: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Error: {e}", "FAIL")
            return False
    
    def test_bulk_writes(self):
        """Test 4: Bulk Writes"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 4: Bulk Writes (10 keys)", "HEADER")
        self.log("=" * 60, "HEADER")
        
        leader_id, leader_url = self.get_leader()
        if not leader_url:
            self.log("No leader found!", "FAIL")
            return False
        
        self.log(f"Leader: {leader_id}", "INFO")
        
        success_count = 0
        for i in range(10):
            try:
                resp = requests.put(
                    f"{leader_url}/vault/key",
                    json={"key": f"key_{i}", "value": f"value_{i}"},
                    timeout=5
                )
                if resp.status_code == 200:
                    success_count += 1
                    self.log(f"  ✓ Write {i+1}/10", "INFO")
                else:
                    self.log(f"  ✗ Write {i+1}/10 failed", "FAIL")
            except Exception as e:
                self.log(f"  ✗ Write {i+1}/10 error: {e}", "FAIL")
        
        if success_count == 10:
            self.log(f"✓ All 10 writes successful", "SUCCESS")
            
            # Verify all keys exist
            self.log("Verifying all keys exist", "INFO")
            all_exist = True
            for i in range(10):
                try:
                    resp = requests.get(f"{leader_url}/vault/key/key_{i}", timeout=5)
                    if resp.status_code != 200:
                        all_exist = False
                        break
                except:
                    all_exist = False
                    break
            
            if all_exist:
                self.log("✓ All keys verified", "SUCCESS")
                return True
            else:
                self.log("✗ Some keys missing", "FAIL")
                return False
        else:
            self.log(f"✗ Only {success_count}/10 writes succeeded", "FAIL")
            return False
    
    def test_metrics_export(self):
        """Test 5: Metrics Export"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 5: Metrics Collection & Export", "HEADER")
        self.log("=" * 60, "HEADER")
        
        leader_id, leader_url = self.get_leader()
        if not leader_url:
            self.log("No leader found!", "FAIL")
            return False
        
        self.log(f"Exporting metrics from {leader_id}", "INFO")
        try:
            resp = requests.get(f"{leader_url}/metrics/export", timeout=5)
            if resp.status_code == 200:
                lines = resp.text.strip().split('\n')
                event_count = len(lines) - 1  # Exclude header
                self.log(f"✓ Metrics exported: {event_count} events", "SUCCESS")
                
                # Parse and show summary
                events = {}
                for line in lines[1:]:
                    if line:
                        parts = line.split(',')
                        event_type = parts[2]
                        events[event_type] = events.get(event_type, 0) + 1
                
                self.log("Event breakdown:", "INFO")
                for event_type, count in sorted(events.items()):
                    self.log(f"  {event_type}: {count}", "INFO")
                
                # Save to file
                with open('/tmp/metrics_test.csv', 'w') as f:
                    f.write(resp.text)
                self.log("✓ Metrics saved to /tmp/metrics_test.csv", "SUCCESS")
                return True
            else:
                self.log(f"✗ Export failed: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Error: {e}", "FAIL")
            return False
    
    def test_metrics_status(self):
        """Test 6: Metrics Status"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 6: Metrics Status", "HEADER")
        self.log("=" * 60, "HEADER")
        
        leader_id, leader_url = self.get_leader()
        if not leader_url:
            self.log("No leader found!", "FAIL")
            return False
        
        try:
            resp = requests.get(f"{leader_url}/metrics/status", timeout=5)
            if resp.status_code == 200:
                status = resp.json()
                self.log(f"Buffer size: {status['bufferSize']}/{status['maxSize']}", "INFO")
                self.log("✓ Metrics status retrieved", "SUCCESS")
                return True
            else:
                self.log(f"✗ Status retrieval failed: {resp.status_code}", "FAIL")
                return False
        except Exception as e:
            self.log(f"✗ Error: {e}", "FAIL")
            return False
    
    def test_all_nodes_accessible(self):
        """Test 7: All Nodes Accessible"""
        self.log("=" * 60, "HEADER")
        self.log("TEST 7: All Nodes Accessible", "HEADER")
        self.log("=" * 60, "HEADER")
        
        accessible = []
        for node_id, url in self.nodes.items():
            try:
                resp = requests.get(f"{url}/raft/state", timeout=2)
                if resp.status_code == 200:
                    state = resp.text
                    accessible.append(node_id)
                    self.log(f"✓ {node_id}: {state.strip()}", "SUCCESS")
                else:
                    self.log(f"✗ {node_id}: Status {resp.status_code}", "FAIL")
            except Exception as e:
                self.log(f"✗ {node_id}: {e}", "FAIL")
        
        if len(accessible) == 3:
            self.log("✓ All 3 nodes accessible", "SUCCESS")
            return True
        else:
            self.log(f"✗ Only {len(accessible)}/3 nodes accessible", "FAIL")
            return False
    
    def run_all_tests(self):
        """Run all tests"""
        self.log("\n" + "=" * 60, "HEADER")
        self.log("PHASE 3.1 - COMPREHENSIVE TEST SUITE", "HEADER")
        self.log("=" * 60, "HEADER")
        
        tests = [
            ("All Nodes Accessible", self.test_all_nodes_accessible),
            ("Basic Operations", self.test_basic_operations),
            ("Follower Rejects Writes", self.test_follower_rejection),
            ("Follower Rejects Reads", self.test_follower_read_rejection),
            ("Bulk Writes", self.test_bulk_writes),
            ("Metrics Export", self.test_metrics_export),
            ("Metrics Status", self.test_metrics_status),
        ]
        
        passed = 0
        failed = 0
        
        for test_name, test_func in tests:
            try:
                if test_func():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                self.log(f"TEST {test_name} CRASHED: {e}", "FAIL")
                failed += 1
            
            time.sleep(1)
        
        # Summary
        self.log("\n" + "=" * 60, "HEADER")
        self.log("TEST SUMMARY", "HEADER")
        self.log("=" * 60, "HEADER")
        self.log(f"Passed: {passed}/{len(tests)}", "SUCCESS" if failed == 0 else "WARN")
        self.log(f"Failed: {failed}/{len(tests)}", "FAIL" if failed > 0 else "SUCCESS")
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        self.log(f"Total time: {elapsed:.2f}s", "INFO")
        
        return failed == 0

if __name__ == "__main__":
    tester = VaultTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
