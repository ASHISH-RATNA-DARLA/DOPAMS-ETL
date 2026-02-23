#!/usr/bin/env python3
"""
Analyze test responses to identify issues
"""

import csv
import json
import re
from pathlib import Path
from typing import List, Dict, Any

def analyze_responses(csv_file: str) -> Dict[str, Any]:
    """Analyze test responses for issues"""
    issues = {
        'clarification_requests': [],
        'no_results': [],
        'short_responses': [],
        'missing_fir_number': [],
        'wrong_format': [],
        'error_patterns': []
    }
    
    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
        # Increase field size limit
        csv.field_size_limit(1000000)
        reader = csv.DictReader(f)
        
        for row in reader:
            qid = row['Question ID']
            question = row['Question']
            response = row['Response']
            status = row['Status']
            sql = row.get('SQL Query', '')
            
            resp_lower = response.lower()
            q_lower = question.lower()
            
            # Check for clarification requests
            if any(phrase in resp_lower for phrase in ['clarification', 'need more', 'unclear', 'please specify', 'i need', 'what would you like']):
                issues['clarification_requests'].append({
                    'id': qid,
                    'question': question[:80],
                    'response_preview': response[:200]
                })
            
            # Check for no results
            if any(phrase in resp_lower for phrase in ['no records found', 'no results', 'no data', 'not found', 'no crimes', 'no persons', 'no accused']):
                issues['no_results'].append({
                    'id': qid,
                    'question': question[:80],
                    'response_preview': response[:200]
                })
            
            # Check for very short responses
            if len(response) < 100 and status == 'success':
                issues['short_responses'].append({
                    'id': qid,
                    'question': question[:80],
                    'response_length': len(response),
                    'response': response
                })
            
            # Check if FIR number was requested but not shown
            if 'fir number' in q_lower or 'fir num' in q_lower:
                if 'fir num' not in resp_lower and 'fir number' not in resp_lower:
                    issues['missing_fir_number'].append({
                        'id': qid,
                        'question': question[:80]
                    })
            
            # Check for error patterns in responses
            if any(pattern in resp_lower for pattern in ['error', 'exception', 'failed', 'invalid', 'syntax error']):
                issues['error_patterns'].append({
                    'id': qid,
                    'question': question[:80],
                    'response_preview': response[:200]
                })
    
    return issues

def print_analysis(issues: Dict[str, Any]):
    """Print analysis results"""
    print("=" * 80)
    print("TEST RESPONSE ANALYSIS")
    print("=" * 80)
    
    total_issues = sum(len(v) for v in issues.values())
    print(f"\nTotal potential issues found: {total_issues}\n")
    
    # Clarification requests
    if issues['clarification_requests']:
        print(f"\n⚠️  CLARIFICATION REQUESTS ({len(issues['clarification_requests'])}):")
        print("-" * 80)
        for item in issues['clarification_requests'][:20]:
            print(f"Q{item['id']}: {item['question']}")
            print(f"   Response: {item['response_preview'][:150]}...")
    
    # No results
    if issues['no_results']:
        print(f"\n⚠️  NO RESULTS FOUND ({len(issues['no_results'])}):")
        print("-" * 80)
        for item in issues['no_results'][:20]:
            print(f"Q{item['id']}: {item['question']}")
            print(f"   Response: {item['response_preview'][:150]}...")
    
    # Short responses
    if issues['short_responses']:
        print(f"\n⚠️  SHORT RESPONSES ({len(issues['short_responses'])}):")
        print("-" * 80)
        for item in issues['short_responses'][:20]:
            print(f"Q{item['id']}: {item['question']}")
            print(f"   Length: {item['response_length']} chars")
            print(f"   Response: {item['response']}")
    
    # Missing FIR number
    if issues['missing_fir_number']:
        print(f"\n⚠️  MISSING FIR NUMBER ({len(issues['missing_fir_number'])}):")
        print("-" * 80)
        for item in issues['missing_fir_number'][:20]:
            print(f"Q{item['id']}: {item['question']}")
    
    # Error patterns
    if issues['error_patterns']:
        print(f"\n⚠️  ERROR PATTERNS ({len(issues['error_patterns'])}):")
        print("-" * 80)
        for item in issues['error_patterns'][:20]:
            print(f"Q{item['id']}: {item['question']}")
            print(f"   Response: {item['response_preview'][:150]}...")

if __name__ == '__main__':
    import sys
    
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'test_responses_20251114_232525.csv'
    
    if not Path(csv_file).exists():
        print(f"Error: File {csv_file} not found")
        sys.exit(1)
    
    print(f"Analyzing {csv_file}...")
    issues = analyze_responses(csv_file)
    print_analysis(issues)
    
    # Save to JSON
    output_file = csv_file.replace('.csv', '_analysis.json')
    with open(output_file, 'w') as f:
        json.dump(issues, f, indent=2)
    print(f"\n✅ Analysis saved to {output_file}")


