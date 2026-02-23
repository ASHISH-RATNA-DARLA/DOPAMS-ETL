#!/usr/bin/env python3
"""
Test script to verify fixes are working:
1. Fallback query generation for Q40-Q44
2. Response formatter for "information about" queries
3. LIMIT injection
4. Performance optimizations
"""

import sys
import requests
import json
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Test questions
TEST_QUESTIONS = {
    'Q40': "List all accused with height information (show accused who have height data)",
    'Q41': "List accused with build type information",
    'Q43': "Find accused with hair color information",
    'Q44': "List accused with mole or leucoderma marks",
    'Q38': "Show persons from state 'Telangana' (show both present_state_ut and permanent_state_ut - show both if available, show one if only one available, show no data if neither available)",
    'Q68': "List drugs by brand name (use drug_name from brief_facts_drugs or nature from properties as brand name alternatives)",
    'Q1': "Show me all crimes with FIR number, date, and police station code",  # Test LIMIT injection
}

API_URL = "http://localhost:5008"
SESSION_ID = f"test_fixes_{int(time.time())}"

def test_question(question_id: str, question: str) -> dict:
    """Test a single question"""
    print(f"\n{'='*80}")
    print(f"Testing {question_id}: {question}")
    print('='*80)
    
    try:
        start_time = time.time()
        response = requests.post(
            f"{API_URL}/api/chat",
            json={
                "message": question,
                "session_id": SESSION_ID
            },
            timeout=120
        )
        execution_time = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract information
            result = {
                'question_id': question_id,
                'question': question,
                'status': 'success' if data.get('success') else 'error',
                'response': data.get('response', ''),
                'queries': data.get('queries', {}),
                'execution_time': execution_time,
                'error': data.get('error')
            }
            
            # Check for SQL query
            sql_query = result['queries'].get('postgresql', '')
            has_sql = bool(sql_query)
            
            # Check response message
            resp_lower = result['response'].lower()
            has_no_records = any(phrase in resp_lower for phrase in [
                'no records found', 'no matching records', 'didn\'t find any matching records'
            ])
            has_verified = any(phrase in resp_lower for phrase in [
                'verified the database', 'no data is available', 'fields exist but are currently empty'
            ])
            
            # Check for LIMIT
            has_limit = 'LIMIT' in sql_query.upper() if sql_query else False
            
            # Print results
            print(f"✅ Status: {result['status']}")
            print(f"⏱️  Execution Time: {execution_time:.2f}s")
            print(f"🔍 SQL Generated: {'✅ YES' if has_sql else '❌ NO'}")
            if has_sql:
                print(f"   SQL Preview: {sql_query[:150]}...")
                print(f"   Has LIMIT: {'✅ YES' if has_limit else '❌ NO'}")
            print(f"📝 Response Type:")
            if has_verified:
                print(f"   ✅ Shows 'verified but no data available' message")
            elif has_no_records:
                print(f"   ❌ Shows 'no records found' (should show verified message)")
            else:
                print(f"   ℹ️  Shows data (not empty)")
            
            # Check specific expectations
            if question_id in ['Q40', 'Q41', 'Q43', 'Q44']:
                print(f"\n🔍 Expected for {question_id}:")
                print(f"   ✅ SQL query should be generated (fallback)")
                print(f"   ✅ Response should show 'verified but no data available'")
                print(f"   ✅ Query should have LIMIT clause")
                
                if not has_sql:
                    print(f"   ❌ FAIL: SQL query not generated!")
                if not has_verified and has_no_records:
                    print(f"   ❌ FAIL: Response formatter not working!")
                if has_sql and not has_limit:
                    print(f"   ⚠️  WARNING: LIMIT not added to query")
            
            if question_id == 'Q1':
                print(f"\n🔍 Expected for Q1 (LIMIT injection test):")
                print(f"   ✅ Query should have LIMIT clause")
                if not has_limit:
                    print(f"   ❌ FAIL: LIMIT not injected!")
            
            return result
        else:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return {
                'question_id': question_id,
                'status': 'error',
                'error': f"HTTP {response.status_code}"
            }
    
    except requests.exceptions.Timeout:
        print(f"❌ Request timed out after 120 seconds")
        return {
            'question_id': question_id,
            'status': 'timeout',
            'error': 'Timeout'
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            'question_id': question_id,
            'status': 'error',
            'error': str(e)
        }

def main():
    """Run all tests"""
    print("="*80)
    print("FIXES VERIFICATION TEST")
    print("="*80)
    print(f"API URL: {API_URL}")
    print(f"Session ID: {SESSION_ID}")
    print("\nTesting:")
    print("  1. Fallback query generation (Q40-Q44)")
    print("  2. Response formatter for 'information about' queries")
    print("  3. LIMIT injection (Q1)")
    print("  4. State query (Q38)")
    print("  5. Brand name query (Q68)")
    
    # Check if server is running
    try:
        response = requests.get(f"{API_URL}/api/health", timeout=5)
        if response.status_code != 200:
            print(f"\n⚠️  WARNING: Server health check failed (status {response.status_code})")
            print(f"   Server might not be running or health endpoint not available")
    except requests.exceptions.ConnectionError:
        print(f"\n❌ ERROR: Cannot connect to server at {API_URL}")
        print(f"   Please make sure the server is running:")
        print(f"   python app.py")
        return
    except Exception as e:
        print(f"\n⚠️  WARNING: Could not check server health: {e}")
    
    results = []
    for qid, question in TEST_QUESTIONS.items():
        result = test_question(qid, question)
        results.append(result)
        time.sleep(1)  # Small delay between requests
    
    # Summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print('='*80)
    
    total = len(results)
    success = sum(1 for r in results if r.get('status') == 'success')
    has_sql = sum(1 for r in results if r.get('queries', {}).get('postgresql'))
    has_limit = sum(1 for r in results if 'LIMIT' in r.get('queries', {}).get('postgresql', '').upper())
    
    print(f"Total Tests: {total}")
    print(f"Successful: {success}/{total} ({success/total*100:.1f}%)")
    print(f"SQL Generated: {has_sql}/{total} ({has_sql/total*100:.1f}%)")
    print(f"LIMIT Added: {has_limit}/{total} ({has_limit/total*100:.1f}%)")
    
    # Check specific fixes
    info_about_queries = ['Q40', 'Q41', 'Q43', 'Q44']
    info_about_results = [r for r in results if r.get('question_id') in info_about_queries]
    
    if info_about_results:
        print(f"\n🔍 'Information About' Queries (Q40-Q44):")
        sql_generated = sum(1 for r in info_about_results if r.get('queries', {}).get('postgresql'))
        verified_message = sum(1 for r in info_about_results if 'verified' in r.get('response', '').lower())
        
        print(f"   SQL Generated: {sql_generated}/{len(info_about_results)}")
        print(f"   Verified Message: {verified_message}/{len(info_about_results)}")
        
        if sql_generated == len(info_about_results):
            print(f"   ✅ Fallback queries are working!")
        else:
            print(f"   ❌ Fallback queries NOT working - {len(info_about_results) - sql_generated} queries missing SQL")
        
        if verified_message == len(info_about_results):
            print(f"   ✅ Response formatter is working!")
        else:
            print(f"   ❌ Response formatter NOT working - {len(info_about_results) - verified_message} queries showing wrong message")
    
    print(f"\n💡 If fixes are not working:")
    print(f"   1. Make sure server is restarted with latest code")
    print(f"   2. Check server logs for 'Generated fallback PostgreSQL query' messages")
    print(f"   3. Verify code changes are deployed")

if __name__ == '__main__':
    main()


