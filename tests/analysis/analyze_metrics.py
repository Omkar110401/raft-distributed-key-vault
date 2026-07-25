#!/usr/bin/env python3

"""
Phase 3.1 - Metrics Analysis & Reporting
Analyzes metrics CSV exports to generate statistics for paper
"""

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import statistics

class MetricsAnalyzer:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.events = []
        self.load_csv()
    
    def load_csv(self):
        """Load events from CSV file"""
        try:
            with open(self.csv_file, 'r') as f:
                reader = csv.DictReader(f)
                self.events = list(reader)
            print(f"✓ Loaded {len(self.events)} events from {self.csv_file}")
        except FileNotFoundError:
            print(f"✗ File not found: {self.csv_file}")
            sys.exit(1)
    
    def get_event_counts(self):
        """Count events by type"""
        counts = defaultdict(int)
        for event in self.events:
            counts[event['event_type']] += 1
        return dict(counts)
    
    def get_term_changes(self):
        """Analyze term progression"""
        term_events = [e for e in self.events if e['event_type'] == 'TERM_CHANGE']
        return term_events
    
    def get_elections(self):
        """Get election start/end pairs"""
        elections = []
        current_election = {}
        
        for event in self.events:
            if event['event_type'] == 'ELECTION_START':
                current_election = {'start': event, 'end': None}
            elif event['event_type'] == 'ELECTION_END':
                if current_election:
                    current_election['end'] = event
                    elections.append(current_election)
                    current_election = {}
        
        return elections
    
    def calculate_election_latencies(self):
        """Calculate election latency in milliseconds"""
        elections = self.get_elections()
        latencies = []
        
        for election in elections:
            if election['end']:
                start_time = datetime.fromisoformat(election['start']['timestamp'].replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(election['end']['timestamp'].replace('Z', '+00:00'))
                latency_ms = (end_time - start_time).total_seconds() * 1000
                latencies.append(latency_ms)
        
        return latencies
    
    def get_replication_stats(self):
        """Analyze log replication events"""
        replication_events = [e for e in self.events if 'REPLICATION' in e['event_type']]
        return len(replication_events)
    
    def get_node_ids(self):
        """Get unique node IDs"""
        return set(e['node_id'] for e in self.events)
    
    def print_summary(self):
        """Print analysis summary"""
        print("\n" + "="*60)
        print("METRICS ANALYSIS SUMMARY")
        print("="*60)
        
        # Event counts
        print("\n1. EVENT DISTRIBUTION:")
        event_counts = self.get_event_counts()
        for event_type, count in sorted(event_counts.items()):
            print(f"   {event_type}: {count}")
        
        # Node coverage
        print(f"\n2. NODES TRACKED: {len(self.get_node_ids())}")
        print(f"   Node IDs: {sorted(self.get_node_ids())}")
        
        # Election statistics
        elections = self.get_elections()
        print(f"\n3. ELECTIONS: {len(elections)}")
        
        latencies = self.calculate_election_latencies()
        if latencies:
            print(f"   Election Latencies (ms):")
            print(f"     Min: {min(latencies):.2f}")
            print(f"     Max: {max(latencies):.2f}")
            print(f"     Mean: {statistics.mean(latencies):.2f}")
            if len(latencies) > 1:
                print(f"     StdDev: {statistics.stdev(latencies):.2f}")
        
        # Term analysis
        print(f"\n4. TERM CHANGES: {len(self.get_term_changes())}")
        
        # Replication stats
        print(f"\n5. REPLICATION EVENTS: {self.get_replication_stats()}")
        
        # Read/Write operations
        writes = len([e for e in self.events if e['event_type'] == 'WRITE_REQUEST'])
        reads = len([e for e in self.events if e['event_type'] == 'READ_REQUEST'])
        print(f"\n6. OPERATIONS:")
        print(f"   Writes: {writes}")
        print(f"   Reads: {reads}")
        
        # Time range
        if self.events:
            first_time = self.events[0]['timestamp']
            last_time = self.events[-1]['timestamp']
            print(f"\n7. TIME RANGE:")
            print(f"   First: {first_time}")
            print(f"   Last: {last_time}")
    
    def export_json_report(self, output_file):
        """Export analysis as JSON"""
        report = {
            'event_counts': self.get_event_counts(),
            'total_events': len(self.events),
            'nodes': sorted(list(self.get_node_ids())),
            'elections': len(self.get_elections()),
            'election_latencies_ms': self.calculate_election_latencies(),
            'writes': len([e for e in self.events if e['event_type'] == 'WRITE_REQUEST']),
            'reads': len([e for e in self.events if e['event_type'] == 'READ_REQUEST']),
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n✓ Report exported to {output_file}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_metrics.py <csv_file> [output_json]")
        print("Example: python3 analyze_metrics.py /tmp/metrics_8080.csv /tmp/report.json")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    output_json = sys.argv[2] if len(sys.argv) > 2 else None
    
    analyzer = MetricsAnalyzer(csv_file)
    analyzer.print_summary()
    
    if output_json:
        analyzer.export_json_report(output_json)

if __name__ == '__main__':
    main()
