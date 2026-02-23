#!/usr/bin/env python3
"""
Analyze test results and generate insights for code improvements
"""

import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Any
import re


class ResultsAnalyzer:
    """Analyze test results to identify patterns and issues"""
    
    def __init__(self, results_file: str):
        with open(results_file, 'r') as f:
            self.data = json.load(f)
        self.results = self.data.get('results', [])
    
    def analyze(self) -> Dict[str, Any]:
        """Perform comprehensive analysis"""
        analysis = {
            'summary': self._get_summary(),
            'error_patterns': self._analyze_errors(),
            'sql_issues': self._analyze_sql(),
            'category_performance': self._analyze_by_category(),
            'common_fixes': self._suggest_fixes()
        }
        return analysis
    
    def _get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        total = len(self.results)
        # Map status values: 'success' -> passed, 'error'/'timeout' -> failed, 'partial' -> partial
        passed = sum(1 for r in self.results if r['status'] == 'success')
        failed = sum(1 for r in self.results if r['status'] in ['error', 'timeout'])
        partial = sum(1 for r in self.results if r['status'] == 'partial')
        
        return {
            'total': total,
            'passed': passed,
            'failed': failed,
            'partial': partial,
            'pass_rate': (passed / total * 100) if total > 0 else 0
        }
    
    def _analyze_errors(self) -> Dict[str, Any]:
        """Analyze error patterns"""
        error_types = Counter()
        error_messages = []
        
        for result in self.results:
            if result['error_message']:
                error_msg = result['error_message']
                error_messages.append({
                    'question_id': result['question_id'],
                    'question': result['question'][:100],
                    'error': error_msg
                })
                
                # Categorize errors
                if 'Programming error' in error_msg:
                    error_types['PostgreSQL Error'] += 1
                elif 'column' in error_msg.lower() and 'does not exist' in error_msg.lower():
                    error_types['Column Not Found'] += 1
                elif 'timeout' in error_msg.lower():
                    error_types['Timeout'] += 1
                elif 'unknown operator' in error_msg.lower():
                    error_types['MongoDB Operator Error'] += 1
                else:
                    error_types['Other'] += 1
        
        return {
            'error_types': dict(error_types),
            'total_errors': len(error_messages),
            'sample_errors': error_messages[:20]  # Top 20
        }
    
    def _analyze_sql(self) -> Dict[str, Any]:
        """Analyze SQL query issues"""
        sql_queries = []
        sql_errors = []
        
        for result in self.results:
            if result.get('sql_query'):
                sql_queries.append({
                    'question_id': result['question_id'],
                    'question': result['question'][:100],
                    'sql': result['sql_query']
                })
            
            # Check for common SQL issues
            if result.get('error_message'):
                error = result['error_message']
                if 'column' in error.lower() and 'does not exist' in error.lower():
                    # Extract column name
                    col_match = re.search(r'column\s+"?(\w+)"?\s+does not exist', error, re.IGNORECASE)
                    if col_match:
                        sql_errors.append({
                            'question_id': result['question_id'],
                            'column': col_match.group(1),
                            'error': error
                        })
        
        return {
            'total_sql_queries': len(sql_queries),
            'sql_errors': sql_errors[:20],
            'sample_queries': sql_queries[:10]
        }
    
    def _analyze_by_category(self) -> Dict[str, Dict[str, Any]]:
        """Analyze performance by category"""
        by_category = defaultdict(lambda: {'total': 0, 'passed': 0, 'failed': 0, 'partial': 0, 'timeout': 0})
        
        # Map status values from test_runner to analyzer keys
        status_map = {
            'success': 'passed',
            'error': 'failed',
            'partial': 'partial',
            'timeout': 'failed'  # Treat timeout as failed
        }
        
        for result in self.results:
            cat = result['category']
            by_category[cat]['total'] += 1
            status = result['status']
            mapped_status = status_map.get(status, 'failed')  # Default to 'failed' for unknown statuses
            by_category[cat][mapped_status] += 1
        
        # Calculate pass rates
        for cat, stats in by_category.items():
            stats['pass_rate'] = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        
        return dict(by_category)
    
    def _suggest_fixes(self) -> List[Dict[str, Any]]:
        """Suggest code fixes based on error patterns"""
        fixes = []
        
        # Analyze column errors
        column_errors = []
        for result in self.results:
            if result.get('error_message'):
                error = result['error_message']
                if 'column' in error.lower() and 'does not exist' in error.lower():
                    col_match = re.search(r'column\s+"?(\w+)"?\s+does not exist', error, re.IGNORECASE)
                    if col_match:
                        column_errors.append({
                            'column': col_match.group(1),
                            'question': result['question'][:100]
                        })
        
        if column_errors:
            col_counter = Counter(e['column'] for e in column_errors)
            fixes.append({
                'type': 'Column Name Fixes',
                'priority': 'HIGH',
                'description': f"Found {len(column_errors)} column errors. Most common: {col_counter.most_common(5)}",
                'action': 'Update SQL generation prompts with correct column names'
            })
        
        # Analyze timeout issues
        timeouts = sum(1 for r in self.results if 'timeout' in str(r.get('error_message', '')).lower())
        if timeouts > 0:
            fixes.append({
                'type': 'Performance Optimization',
                'priority': 'MEDIUM',
                'description': f"Found {timeouts} timeout errors",
                'action': 'Optimize query generation or increase timeout limits'
            })
        
        return fixes
    
    def generate_report(self, output_file: str):
        """Generate analysis report"""
        analysis = self.analyze()
        
        with open(output_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("TEST RESULTS ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary
            f.write("SUMMARY\n")
            f.write("-" * 80 + "\n")
            summary = analysis['summary']
            f.write(f"Total Questions: {summary['total']}\n")
            f.write(f"Passed: {summary['passed']} ({summary['pass_rate']:.1f}%)\n")
            f.write(f"Failed: {summary['failed']}\n")
            f.write(f"Partial: {summary['partial']}\n\n")
            
            # Error Patterns
            f.write("ERROR PATTERNS\n")
            f.write("-" * 80 + "\n")
            for error_type, count in analysis['error_patterns']['error_types'].items():
                f.write(f"{error_type}: {count}\n")
            f.write("\n")
            
            # Category Performance
            f.write("PERFORMANCE BY CATEGORY\n")
            f.write("-" * 80 + "\n")
            for category, stats in sorted(analysis['category_performance'].items()):
                f.write(f"{category}:\n")
                f.write(f"  Total: {stats['total']}, Passed: {stats['passed']}, Failed: {stats['failed']}\n")
                f.write(f"  Pass Rate: {stats['pass_rate']:.1f}%\n\n")
            
            # Suggested Fixes
            f.write("SUGGESTED FIXES\n")
            f.write("-" * 80 + "\n")
            for fix in analysis['common_fixes']:
                f.write(f"[{fix['priority']}] {fix['type']}\n")
                f.write(f"  {fix['description']}\n")
                f.write(f"  Action: {fix['action']}\n\n")
            
            # Sample Errors
            f.write("SAMPLE ERRORS\n")
            f.write("-" * 80 + "\n")
            for error in analysis['error_patterns']['sample_errors'][:10]:
                f.write(f"Q{error['question_id']}: {error['question']}\n")
                f.write(f"  Error: {error['error'][:200]}\n\n")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze test results')
    parser.add_argument('results_file', help='Test results JSON file')
    parser.add_argument('--output', default='analysis_report.txt', help='Output report file')
    
    args = parser.parse_args()
    
    analyzer = ResultsAnalyzer(args.results_file)
    analyzer.generate_report(args.output)
    
    print(f"Analysis complete! Report saved to {args.output}")


if __name__ == '__main__':
    main()


