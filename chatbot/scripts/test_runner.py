#!/usr/bin/env python3
"""
Automated Test Runner for DOPAMAS Chatbot
Processes questions.txt, runs each question, captures logs, and generates reports
"""

import os
import sys
import json
import time
import logging
import requests
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import re

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Test result for a single question"""
    question_id: int
    category: str
    question: str
    status: str  # 'success', 'error', 'partial', 'timeout'
    response: Optional[str] = None
    sql_query: Optional[str] = None
    mongodb_query: Optional[str] = None
    error_message: Optional[str] = None
    execution_time: float = 0.0
    timestamp: str = ""
    log_snippet: Optional[str] = None


@dataclass
class TestReport:
    """Complete test report"""
    total_questions: int
    passed: int
    failed: int
    partial: int
    errors: List[str]
    results: List[TestResult]
    execution_time: float
    timestamp: str


class QuestionParser:
    """Parse questions.txt file"""
    
    def __init__(self, questions_file: str):
        self.questions_file = questions_file
        self.questions = []
    
    def parse(self) -> List[Dict[str, Any]]:
        """Parse questions file and return structured list"""
        with open(self.questions_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        questions = []
        current_category = "General"
        question_id = 1
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Detect category headers (lines with all caps or specific patterns)
            if line.isupper() or 'QUERIES' in line.upper() or 'SCENARIOS' in line.upper():
                # Extract category name
                category_match = re.match(r'^(\d+\.\s*)?([A-Z\s&]+)', line)
                if category_match:
                    current_category = category_match.group(2).strip()
                continue
            
            # Detect numbered questions
            question_match = re.match(r'^(\d+)\.\s*(.+)', line)
            if question_match:
                question_num = question_match.group(1)
                question_text = question_match.group(2).strip()
                
                questions.append({
                    'id': question_id,
                    'category': current_category,
                    'question': question_text,
                    'original_line': line
                })
                question_id += 1
            # Also capture unnumbered questions (like "Basic Crime Information" items)
            elif not line.startswith('#') and len(line) > 10:
                # Check if it looks like a question
                if any(keyword in line.lower() for keyword in ['find', 'get', 'list', 'search', 'show', 'calculate', 'analyze']):
                    questions.append({
                        'id': question_id,
                        'category': current_category,
                        'question': line,
                        'original_line': line
                    })
                    question_id += 1
        
        logger.info(f"Parsed {len(questions)} questions from {self.questions_file}")
        return questions


class ChatbotTester:
    """Test chatbot with questions"""
    
    def __init__(self, api_url: str = "http://localhost:5008", log_file: str = "app.log"):
        self.api_url = api_url
        self.log_file = log_file
        self.session_id = f"test_session_{int(time.time())}"
    
    def test_question(self, question: str, timeout: int = 300) -> Dict[str, Any]:
        """Test a single question"""
        start_time = time.time()
        
        try:
            # Make API request
            response = requests.post(
                f"{self.api_url}/api/chat",
                json={
                    "message": question,
                    "session_id": self.session_id
                },
                timeout=timeout
            )
            
            execution_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'status': 'success' if data.get('success') else 'error',
                    'response': data.get('response', ''),
                    'queries': data.get('queries', {}),
                    'execution_time': execution_time,
                    'error': data.get('error')
                }
            else:
                return {
                    'status': 'error',
                    'response': None,
                    'queries': {},
                    'execution_time': execution_time,
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
        
        except requests.exceptions.Timeout:
            return {
                'status': 'timeout',
                'response': None,
                'queries': {},
                'execution_time': timeout,
                'error': f"Request timed out after {timeout} seconds"
            }
        except Exception as e:
            return {
                'status': 'error',
                'response': None,
                'queries': {},
                'execution_time': time.time() - start_time,
                'error': str(e)
            }
    
    def get_recent_logs(self, question: str, lines: int = 50) -> str:
        """Get recent log entries related to the question"""
        try:
            if not os.path.exists(self.log_file):
                return "Log file not found"
            
            with open(self.log_file, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines = f.readlines()
                # Get last N lines
                recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
                return ''.join(recent_lines)
        except Exception as e:
            return f"Error reading log: {str(e)}"


class LogAnalyzer:
    """Analyze logs to extract SQL queries, errors, etc."""
    
    def __init__(self, log_file: str):
        self.log_file = log_file
    
    def extract_sql_from_logs(self, log_text: str) -> Optional[str]:
        """Extract SQL query from log text"""
        # Look for "Generated PostgreSQL query:" pattern
        sql_match = re.search(r'Generated PostgreSQL query:\s*(.+?)(?:\n|$)', log_text, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()
        return None
    
    def extract_mongodb_from_logs(self, log_text: str) -> Optional[str]:
        """Extract MongoDB query from log text"""
        mongo_match = re.search(r'Generated MongoDB query:\s*(.+?)(?:\n|$)', log_text, re.DOTALL)
        if mongo_match:
            return mongo_match.group(1).strip()
        return None
    
    def extract_errors(self, log_text: str) -> List[str]:
        """Extract error messages from log text"""
        errors = []
        # Look for ERROR level logs
        error_pattern = r'ERROR.*?:\s*(.+?)(?:\n|$)'
        error_matches = re.findall(error_pattern, log_text, re.MULTILINE)
        errors.extend(error_matches)
        
        # Look for Programming error
        pg_error_pattern = r'Programming error:\s*(.+?)(?:\n|$)'
        pg_errors = re.findall(pg_error_pattern, log_text, re.MULTILINE)
        errors.extend(pg_errors)
        
        return errors


class TestRunner:
    """Main test runner"""
    
    def __init__(self, questions_file: str, api_url: str = "http://localhost:5008", 
                 log_file: str = "app.log", output_dir: str = "test_results", timeout: int = 300):
        self.questions_file = questions_file
        self.api_url = api_url
        self.log_file = log_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.timeout = timeout
        
        self.parser = QuestionParser(questions_file)
        self.tester = ChatbotTester(api_url, log_file)
        self.analyzer = LogAnalyzer(log_file)
        
        self.results: List[TestResult] = []
    
    def run_all_tests(self, start_from: int = 1, limit: Optional[int] = None) -> TestReport:
        """Run all tests"""
        questions = self.parser.parse()
        
        if limit:
            questions = questions[:limit]
        
        # Filter by start_from
        questions = [q for q in questions if q['id'] >= start_from]
        
        total = len(questions)
        logger.info(f"Running {total} tests...")
        
        start_time = time.time()
        
        for idx, question_data in enumerate(questions, 1):
            question_id = question_data['id']
            category = question_data['category']
            question = question_data['question']
            
            logger.info(f"[{idx}/{total}] Testing Q{question_id}: {question[:60]}...")
            
            # Test the question
            test_response = self.tester.test_question(question, timeout=self.timeout)
            
            # Get recent logs
            log_snippet = self.tester.get_recent_logs(question, lines=100)
            
            # Extract SQL/MongoDB queries from logs
            sql_query = self.analyzer.extract_sql_from_logs(log_snippet)
            mongodb_query = self.analyzer.extract_mongodb_from_logs(log_snippet)
            errors = self.analyzer.extract_errors(log_snippet)
            
            # Determine status
            status = test_response['status']
            if status == 'success' and test_response.get('error'):
                status = 'partial'
            
            # Create result
            result = TestResult(
                question_id=question_id,
                category=category,
                question=question,
                status=status,
                response=test_response.get('response'),
                sql_query=sql_query,
                mongodb_query=mongodb_query,
                error_message=test_response.get('error') or ('; '.join(errors) if errors else None),
                execution_time=test_response['execution_time'],
                timestamp=datetime.now().isoformat(),
                log_snippet=log_snippet[-500:] if log_snippet else None  # Last 500 chars
            )
            
            self.results.append(result)
            
            # Save intermediate results
            if idx % 10 == 0:
                self._save_intermediate_results()
            
            # Small delay to avoid overwhelming the server
            time.sleep(0.5)
        
        execution_time = time.time() - start_time
        
        # Generate report
        report = self._generate_report(execution_time)
        
        # Save final results
        self._save_results(report)
        
        return report
    
    def _generate_report(self, execution_time: float) -> TestReport:
        """Generate test report"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == 'success')
        failed = sum(1 for r in self.results if r.status == 'error')
        partial = sum(1 for r in self.results if r.status == 'partial')
        
        # Collect all errors
        errors = []
        for result in self.results:
            if result.error_message:
                errors.append(f"Q{result.question_id}: {result.error_message}")
        
        return TestReport(
            total_questions=total,
            passed=passed,
            failed=failed,
            partial=partial,
            errors=errors,
            results=self.results,
            execution_time=execution_time,
            timestamp=datetime.now().isoformat()
        )
    
    def _save_intermediate_results(self):
        """Save intermediate results"""
        report = self._generate_report(0)
        filename = self.output_dir / f"test_results_intermediate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(asdict(report), f, indent=2, default=str)
    
    def _save_results(self, report: TestReport):
        """Save final results"""
        # Save JSON
        json_file = self.output_dir / f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_file, 'w') as f:
            json.dump(asdict(report), f, indent=2, default=str)
        
        # Save human-readable report
        txt_file = self.output_dir / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self._generate_text_report(report, txt_file)
        
        # Save CSV for easy review
        csv_file = self.output_dir / f"test_responses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self._generate_csv_report(report, csv_file)
        
        logger.info(f"Results saved to:")
        logger.info(f"  - JSON: {json_file}")
        logger.info(f"  - Text Report: {txt_file}")
        logger.info(f"  - CSV (Responses): {csv_file}")
    
    def _generate_text_report(self, report: TestReport, filename: Path):
        """Generate human-readable text report"""
        with open(filename, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DOPAMAS CHATBOT TEST REPORT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Timestamp: {report.timestamp}\n")
            f.write(f"Total Questions: {report.total_questions}\n")
            f.write(f"Passed: {report.passed} ({report.passed/report.total_questions*100:.1f}%)\n")
            f.write(f"Failed: {report.failed} ({report.failed/report.total_questions*100:.1f}%)\n")
            f.write(f"Partial: {report.partial} ({report.partial/report.total_questions*100:.1f}%)\n")
            f.write(f"Execution Time: {report.execution_time:.2f} seconds\n\n")
            
            # Group by category
            by_category = {}
            for result in report.results:
                if result.category not in by_category:
                    by_category[result.category] = []
                by_category[result.category].append(result)
            
            f.write("=" * 80 + "\n")
            f.write("RESULTS BY CATEGORY\n")
            f.write("=" * 80 + "\n\n")
            
            for category, results in by_category.items():
                f.write(f"\n{category} ({len(results)} questions)\n")
                f.write("-" * 80 + "\n")
                
                for result in results:
                    status_icon = "✅" if result.status == 'success' else "❌" if result.status == 'error' else "⚠️"
                    f.write(f"{status_icon} Q{result.question_id}: {result.question}\n")
                    
                    # Show actual chat response
                    if result.response:
                        # Truncate very long responses for readability
                        response_preview = result.response[:500] + "..." if len(result.response) > 500 else result.response
                        f.write(f"   📝 Response: {response_preview}\n")
                    else:
                        f.write(f"   📝 Response: (No response received)\n")
                    
                    if result.error_message:
                        f.write(f"   ⚠️  Error: {result.error_message[:200]}\n")
                    if result.sql_query:
                        f.write(f"   🔍 SQL: {result.sql_query[:150]}...\n")
                    if result.mongodb_query:
                        f.write(f"   🔍 MongoDB: {result.mongodb_query[:150]}...\n")
                    f.write(f"   ⏱️  Time: {result.execution_time:.2f}s\n")
                    f.write("\n")
            
            # Detailed response section
            f.write("\n" + "=" * 80 + "\n")
            f.write("DETAILED RESPONSES (Question -> Response)\n")
            f.write("=" * 80 + "\n\n")
            
            for result in report.results:
                f.write(f"Q{result.question_id} [{result.status.upper()}]: {result.question}\n")
                f.write("-" * 80 + "\n")
                if result.response:
                    f.write(f"Response:\n{result.response}\n")
                else:
                    f.write("Response: (No response received)\n")
                if result.error_message:
                    f.write(f"\nError: {result.error_message}\n")
                f.write("\n" + "=" * 80 + "\n\n")
            
            # Error summary
            if report.errors:
                f.write("\n" + "=" * 80 + "\n")
                f.write("ERROR SUMMARY\n")
                f.write("=" * 80 + "\n\n")
                for error in report.errors[:50]:  # Top 50 errors
                    f.write(f"- {error}\n")
    
    def _generate_csv_report(self, report: TestReport, filename: Path):
        """Generate CSV report with question-response pairs for easy review"""
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'Question ID',
                'Category',
                'Status',
                'Question',
                'Response',
                'Error Message',
                'SQL Query',
                'MongoDB Query',
                'Execution Time (s)'
            ])
            
            # Write each result
            for result in report.results:
                writer.writerow([
                    result.question_id,
                    result.category,
                    result.status,
                    result.question,
                    result.response or '(No response)',
                    result.error_message or '',
                    result.sql_query or '',
                    result.mongodb_query or '',
                    f"{result.execution_time:.2f}"
                ])


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run automated tests on DOPAMAS chatbot')
    parser.add_argument('--questions', default='questions.txt', help='Questions file path')
    parser.add_argument('--api-url', default='http://localhost:5008', help='Chatbot API URL')
    parser.add_argument('--log-file', default='app-test.log', help='Log file path (where app writes logs)')
    parser.add_argument('--output-dir', default='test_results', help='Output directory')
    parser.add_argument('--start-from', type=int, default=1, help='Start from question ID')
    parser.add_argument('--limit', type=int, help='Limit number of questions to test')
    parser.add_argument('--timeout', type=int, default=300, help='Timeout per question in seconds (default: 300)')
    
    args = parser.parse_args()
    
    # ⭐ IMPORTANT: Check if log file exists and warn if app might be using different log file
    log_file_path = Path(args.log_file)
    if not log_file_path.exists():
        print(f"⚠️  WARNING: Log file '{args.log_file}' does not exist yet.")
        print(f"   The application will create it when it starts logging.")
        print(f"   Make sure your Flask app is configured to log to: {args.log_file}")
        print(f"   You can set LOG_FILE environment variable before starting the app:")
        print(f"   export LOG_FILE={args.log_file}")
        print(f"   python app.py\n")
    
    runner = TestRunner(
        questions_file=args.questions,
        api_url=args.api_url,
        log_file=args.log_file,
        output_dir=args.output_dir,
        timeout=args.timeout
    )
    
    report = runner.run_all_tests(start_from=args.start_from, limit=args.limit)
    
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Total: {report.total_questions}")
    print(f"Passed: {report.passed} ({report.passed/report.total_questions*100:.1f}%)")
    print(f"Failed: {report.failed} ({report.failed/report.total_questions*100:.1f}%)")
    print(f"Partial: {report.partial} ({report.partial/report.total_questions*100:.1f}%)")
    print(f"Time: {report.execution_time:.2f}s")
    print("=" * 80)


if __name__ == '__main__':
    main()


