#!/usr/bin/env python3
"""
Comprehensive analysis of test results to identify all issues
"""

import json
import csv
import sys
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

class TestAnalyzer:
    def __init__(self, json_file: str, csv_file: str, txt_file: str):
        self.json_file = json_file
        self.csv_file = csv_file
        self.txt_file = txt_file
        self.issues = defaultdict(list)
        
    def analyze_all(self):
        """Run comprehensive analysis"""
        print("=" * 80)
        print("COMPREHENSIVE TEST RESULTS ANALYSIS")
        print("=" * 80)
        
        # Load data
        print("\n📊 Loading test data...")
        json_data = self._load_json()
        csv_data = self._load_csv()
        
        print(f"✅ Loaded {len(json_data.get('results', []))} test results from JSON")
        print(f"✅ Loaded {len(csv_data)} test results from CSV")
        
        # Analyze each result
        print("\n🔍 Analyzing test results...")
        for result in json_data.get('results', []):
            self._analyze_result(result)
        
        # Generate report
        self._generate_report()
        
    def _load_json(self) -> Dict:
        """Load JSON test results"""
        with open(self.json_file, 'r') as f:
            return json.load(f)
    
    def _load_csv(self) -> List[Dict]:
        """Load CSV test results"""
        csv.field_size_limit(sys.maxsize)
        with open(self.csv_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            return list(reader)
    
    def _analyze_result(self, result: Dict):
        """Analyze a single test result"""
        qid = result.get('question_id')
        question = result.get('question', '')
        response = result.get('response', '')
        status = result.get('status', '')
        sql = result.get('sql_query', '')
        mongo = result.get('mongodb_query', '')
        error = result.get('error_message', '')
        
        q_lower = question.lower()
        resp_lower = response.lower() if response else ''
        
        # 1. Check for "no matching records" when query should return results
        if 'no matching records' in resp_lower or 'didn\'t find any matching records' in resp_lower:
            # Check if this is a query that should definitely return results
            if any(kw in q_lower for kw in ['list all', 'show all', 'find all', 'get all', 'all crimes', 'all accused', 'all persons']):
                self.issues['no_results_for_all_queries'].append({
                    'id': qid,
                    'question': question,
                    'response': response[:200]
                })
        
        # 2. Check for clarification requests when query is clear
        if any(phrase in resp_lower for phrase in [
            'just to confirm', 'is this correct', 'please provide more details',
            'need more information', 'unclear', 'please specify'
        ]):
            # Check if query has clear domain keywords
            clear_keywords = [
                'case status', 'crime type', 'io name', 'io rank', 'fir number',
                'pending', 'accused', 'person', 'crime', 'drug', 'property',
                'police station', 'district', 'date', 'year', 'month'
            ]
            if any(kw in q_lower for kw in clear_keywords):
                self.issues['unnecessary_clarification'].append({
                    'id': qid,
                    'question': question,
                    'response': response[:200]
                })
        
        # 3. Check for missing SQL queries when status is success
        if status == 'success' and not sql and not mongo:
            # Check if this should have generated a query
            if any(kw in q_lower for kw in ['find', 'show', 'list', 'get', 'search', 'count', 'calculate']):
                self.issues['missing_queries'].append({
                    'id': qid,
                    'question': question,
                    'response': response[:200] if response else 'No response'
                })
        
        # 4. Check for wrong SQL patterns
        if sql:
            # Pending cases using wrong pattern
            if 'pending' in q_lower and 'case_status' in sql:
                if 'ILIKE' in sql and '%pending%' in sql:
                    self.issues['wrong_pending_pattern'].append({
                        'id': qid,
                        'question': question,
                        'sql': sql[:300]
                    })
            
            # IO rank using io_name instead of io_rank
            if 'io rank' in q_lower and 'io_name' in sql.lower() and 'io_rank' not in sql.lower():
                self.issues['wrong_io_rank'].append({
                    'id': qid,
                    'question': question,
                    'sql': sql[:300]
                })
            
            # CCL queries using wrong pattern
            if any(kw in q_lower for kw in ['ccl', 'is_ccl', 'child in conflict']) and sql:
                if 'is_ccl' not in sql.lower():
                    self.issues['missing_ccl_field'].append({
                        'id': qid,
                        'question': question,
                        'sql': sql[:300]
                    })
                elif 'is_ccl' in sql.lower() and '= true' not in sql.lower() and '= \'true\'' in sql.lower():
                    self.issues['wrong_ccl_type'].append({
                        'id': qid,
                        'question': question,
                        'sql': sql[:300]
                    })
        
        # 5. Check for very short responses (might indicate issues)
        if status == 'success' and response and len(response) < 100:
            self.issues['short_responses'].append({
                'id': qid,
                'question': question,
                'response': response,
                'length': len(response)
            })
        
        # 6. Check for error patterns in responses
        if any(pattern in resp_lower for pattern in ['error', 'exception', 'failed', 'invalid', 'syntax error', 'programming error']):
            self.issues['error_in_response'].append({
                'id': qid,
                'question': question,
                'response': response[:300],
                'error': error
            })
        
        # 7. Check for missing requested fields
        if 'fir number' in q_lower or 'fir num' in q_lower:
            if 'fir num' not in resp_lower and 'fir number' not in resp_lower:
                self.issues['missing_fir_number'].append({
                    'id': qid,
                    'question': question
                })
        
        # 8. Check for GROUP BY vs WHERE confusion
        if sql and any(kw in q_lower for kw in ['by status', 'by type', 'by district', 'by month', 'by year', 'distributions']):
            if 'GROUP BY' not in sql.upper() and 'WHERE' in sql.upper():
                self.issues['wrong_group_by'].append({
                    'id': qid,
                    'question': question,
                    'sql': sql[:300]
                })
        
        # 9. Check for accused_type vs type confusion
        if sql and any(kw in q_lower for kw in ['accused type', 'accused role', 'peddler', 'supplier', 'consumer']):
            if 'bfa.type' in sql or 'a.type' in sql or 'accused.type' in sql:
                if 'bfa.accused_type' not in sql and 'brief_facts_accused.accused_type' not in sql:
                    self.issues['wrong_accused_type_column'].append({
                        'id': qid,
                        'question': question,
                        'sql': sql[:300]
                    })
    
    def _generate_report(self):
        """Generate comprehensive report"""
        print("\n" + "=" * 80)
        print("ISSUES FOUND")
        print("=" * 80)
        
        total_issues = sum(len(issues) for issues in self.issues.values())
        print(f"\n📊 Total issues found: {total_issues}\n")
        
        # Report each category
        for category, items in sorted(self.issues.items()):
            if items:
                print(f"\n{'='*80}")
                print(f"⚠️  {category.upper().replace('_', ' ')} ({len(items)} issues)")
                print("=" * 80)
                
                for item in items[:10]:  # Show first 10
                    print(f"\nQ{item['id']}: {item['question'][:80]}")
                    if 'sql' in item:
                        print(f"   SQL: {item['sql'][:200]}...")
                    if 'response' in item:
                        print(f"   Response: {item['response'][:150]}...")
                    if 'error' in item and item['error']:
                        print(f"   Error: {item['error']}")
                
                if len(items) > 10:
                    print(f"\n   ... and {len(items) - 10} more")
        
        # Summary statistics
        print("\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)
        for category, items in sorted(self.issues.items()):
            if items:
                print(f"  {category.replace('_', ' ').title()}: {len(items)}")
        
        # Save detailed report
        output_file = 'test_analysis_detailed.json'
        with open(output_file, 'w') as f:
            json.dump(dict(self.issues), f, indent=2)
        print(f"\n✅ Detailed analysis saved to {output_file}")

if __name__ == '__main__':
    json_file = sys.argv[1] if len(sys.argv) > 1 else 'test_results_20251114_232525.json'
    csv_file = sys.argv[2] if len(sys.argv) > 2 else 'test_responses_20251114_232525.csv'
    txt_file = sys.argv[3] if len(sys.argv) > 3 else 'test_report_20251114_232525.txt'
    
    if not all(Path(f).exists() for f in [json_file, csv_file]):
        print(f"Error: Required files not found")
        print(f"  JSON: {json_file} - {'✅' if Path(json_file).exists() else '❌'}")
        print(f"  CSV: {csv_file} - {'✅' if Path(csv_file).exists() else '❌'}")
        sys.exit(1)
    
    analyzer = TestAnalyzer(json_file, csv_file, txt_file)
    analyzer.analyze_all()


