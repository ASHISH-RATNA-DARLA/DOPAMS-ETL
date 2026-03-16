#!/usr/bin/env python3
"""
Verification Script - Ensure all production files are in place and ready
Run: python verify_kb_matching_deployment.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime


def print_header(text):
    print("\n" + "="*70)
    print(f"  {text}")
    print("="*70)


def print_section(text):
    print(f"\n{'─'*70}")
    print(f"  {text}")
    print(f"{'─'*70}")


def check_file_exists(path, description):
    """Check if file exists and show status."""
    exists = Path(path).exists()
    status = "✅" if exists else "❌"
    size = ""
    if exists:
        size = f" ({Path(path).stat().st_size} bytes)"
    print(f"  {status} {description:40s} {path}{size}")
    return exists


def check_file_content(path, search_text, description):
    """Check if file contains specific text."""
    if not Path(path).exists():
        print(f"  ❌ {description:40s} (FILE NOT FOUND)")
        return False
    
    try:
        with open(path, 'r') as f:
            content = f.read()
            if search_text in content:
                print(f"  ✅ {description:40s} Found")
                return True
            else:
                print(f"  ❌ {description:40s} Not found")
                return False
    except Exception as e:
        print(f"  ❌ {description:40s} Error: {e}")
        return False


def verify_core_files():
    """Verify all core production files exist."""
    print_section("1. CORE PRODUCTION FILES")
    
    base_path = Path('.')
    
    results = {
        'kb_matcher': check_file_exists('kb_matcher_advanced.py', 'KB Matcher (core logic)'),
        'integration': check_file_exists('extractor_integration.py', 'Integration layer'),
        'tests': check_file_exists('test_kb_matcher_advanced.py', 'Unit tests'),
    }
    
    return all(results.values())


def verify_documentation():
    """Verify all documentation files exist."""
    print_section("2. DOCUMENTATION FILES")
    
    results = {
        'prod_guide': check_file_exists(
            'PRODUCTION_DEPLOYMENT_GUIDE.md',
            'Deployment guide'
        ),
        'summary': check_file_exists(
            'COMPLETE_SOLUTION_SUMMARY.md',
            'Solution summary'
        ),
        'quick_ref': check_file_exists(
            'QUICK_REFERENCE.md',
            'Quick reference card'
        ),
        'exact_code': check_file_exists(
            'EXACT_CODE_INTEGRATION.md',
            'Exact code changes'
        ),
    }
    
    return all(results.values())


def verify_deployment_tools():
    """Verify deployment automation tools exist."""
    print_section("3. DEPLOYMENT TOOLS")
    
    results = {
        'deploy_script': check_file_exists(
            'deploy_kb_matching.py',
            'Deployment automation'
        ),
    }
    
    return all(results.values())


def verify_code_quality():
    """Verify code has expected components."""
    print_section("4. CODE QUALITY CHECKS")
    
    checks = [
        ('kb_matcher_advanced.py', 'class DrugKBMatcherAdvanced', 
         'Matcher class definition'),
        ('kb_matcher_advanced.py', 'NDPS_TIER1_DRUGS', 
         'NDPS tier1 drugs mapping'),
        ('kb_matcher_advanced.py', 'COMMERCIAL_QUANTITY_NDPS', 
         'Commercial thresholds'),
        ('extractor_integration.py', 'def refine_drugs_with_advanced_kb',
         'Refinement pipeline'),
        ('extractor_integration.py', 'class DrugValidationFilter',
         'Validation filters'),
        ('test_kb_matcher_advanced.py', 'class TestDrugNormalization',
         'Unit tests'),
    ]
    
    results = []
    for filepath, search_text, description in checks:
        result = check_file_content(filepath, search_text, description)
        results.append(result)
    
    return all(results)


def verify_python_syntax():
    """Verify Python files have valid syntax."""
    print_section("5. PYTHON SYNTAX CHECK")
    
    import py_compile
    
    files_to_check = [
        'kb_matcher_advanced.py',
        'extractor_integration.py',
        'test_kb_matcher_advanced.py',
        'deploy_kb_matching.py',
    ]
    
    results = []
    for filename in files_to_check:
        if not Path(filename).exists():
            print(f"  ❌ {filename:40s} (NOT FOUND)")
            results.append(False)
            continue
        
        try:
            py_compile.compile(filename, doraise=True)
            print(f"  ✅ {filename:40s} Valid syntax")
            results.append(True)
        except py_compile.PyCompileError as e:
            print(f"  ❌ {filename:40s} Syntax error: {str(e)[:50]}")
            results.append(False)
    
    return all(results)


def verify_imports():
    """Verify key imports are available."""
    print_section("6. IMPORT AVAILABILITY")
    
    imports_to_check = [
        ('difflib', 'Standard library for fuzzy matching'),
        ('json', 'Standard library for JSON handling'),
        ('langchain_core.output_parsers', 'Langchain parser'),
        ('pydantic', 'Pydantic for data validation'),
    ]
    
    results = []
    for module, description in imports_to_check:
        try:
            __import__(module)
            print(f"  ✅ {module:40s} {description}")
            results.append(True)
        except ImportError:
            print(f"  ⚠️  {module:40s} Not installed (optional)")
            results.append(True)  # Optional imports are OK
    
    return all(results)


def verify_documentation_content():
    """Verify documentation has required sections."""
    print_section("7. DOCUMENTATION CONTENT")
    
    checks = [
        ('EXACT_CODE_INTEGRATION.md', 'UPDATE: extractor.py',
         'Exact extractor.py changes'),
        ('PRODUCTION_DEPLOYMENT_GUIDE.md', 'Step 1: Code Deployment',
         'Deployment steps'),
        ('QUICK_REFERENCE.md', 'Quick Start (5 min)',
         'Quick start guide'),
        ('COMPLETE_SOLUTION_SUMMARY.md', 'Architecture',
         'Architecture documentation'),
    ]
    
    results = []
    for filepath, search_text, description in checks:
        result = check_file_content(filepath, search_text, description)
        results.append(result)
    
    return all(results)


def get_file_stats():
    """Get statistics on production files."""
    print_section("8. FILE STATISTICS")
    
    files = {
        'kb_matcher_advanced.py': 'Core matcher (lines)',
        'extractor_integration.py': 'Integration (lines)',
        'test_kb_matcher_advanced.py': 'Tests (lines)',
    }
    
    total_lines = 0
    for filename, description in files.items():
        if Path(filename).exists():
            with open(filename, 'r') as f:
                lines = len(f.readlines())
                total_lines += lines
                print(f"  {filename:35s} {lines:5d} {description}")
        else:
            print(f"  {filename:35s} ----- NOT FOUND")
    
    print(f"  {'TOTAL':35s} {total_lines:5d} lines of code")
    
    # Documentation
    doc_files = {
        'PRODUCTION_DEPLOYMENT_GUIDE.md': 'Deployment (lines)',
        'EXACT_CODE_INTEGRATION.md': 'Integration (lines)',
        'QUICK_REFERENCE.md': 'Quick ref (lines)',
        'COMPLETE_SOLUTION_SUMMARY.md': 'Summary (lines)',
    }
    
    total_doc = 0
    for filename, description in doc_files.items():
        if Path(filename).exists():
            with open(filename, 'r') as f:
                lines = len(f.readlines())
                total_doc += lines
                print(f"  {filename:35s} {lines:5d} {description}")
    
    print(f"  {'TOTAL DOCUMENTATION':35s} {total_doc:5d} lines")


def show_deployment_readiness():
    """Show overall deployment readiness."""
    print_section("DEPLOYMENT READINESS ASSESSMENT")
    
    print("""
┌─────────────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT CHECKLIST                             │
├─────────────────────────────────────────────────────────────────────┤
│  Phase 1: Pre-Deployment (Before Integration)                       │
│    □ All core files present and valid Python                        │
│    □ Documentation complete with exact code changes                 │
│    □ Tests executable (70+ test cases)                              │
│    □ Deployment script ready                                        │
│    □ Database migrations generated                                  │
│                                                                     │
│  Phase 2: Integration (Manual, ~90 minutes)                         │
│    □ Copy kb_matcher_advanced.py to brief_facts_drugs/              │
│    □ Copy extractor_integration.py to brief_facts_drugs/            │
│    □ Update extractor.py (4 edits) - See EXACT_CODE_INTEGRATION.md │
│    □ Update db.py (add 2 functions)                                 │
│    □ Update main.py (adjust validation)                             │
│    □ Update config.py (add KB_MATCH_CONFIG)                         │
│    □ Create database tables (SQL migration)                         │
│                                                                     │
│  Phase 3: Testing (Before Production)                               │
│    □ Run pytest test_kb_matcher_advanced.py                         │
│    □ Test with 50 sample crimes                                     │
│    □ Verify audit tables are populated                              │
│    □ Check match quality metrics (>80% success)                     │
│    □ Monitor performance (<50ms per drug)                           │
│                                                                     │
│  Phase 4: Deployment                                                │
│    □ Staging deployment (100 crimes)                                │
│    □ Production ramp-up (1000 → 5000 → 10000 crimes)                │
│    □ Monitor audit tables continuously                              │
│    □ Have rollback plan ready                                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
""")


def main():
    print_header("DOPAMS ETL - KB Matching Deployment Verification")
    print(f"  Generated: {datetime.now().isoformat()}")
    
    # Run all verifications
    checks = {
        'Core Files': verify_core_files(),
        'Documentation': verify_documentation(),
        'Deployment Tools': verify_deployment_tools(),
        'Code Quality': verify_code_quality(),
        'Python Syntax': verify_python_syntax(),
        'Imports': verify_imports(),
        'Documentation Content': verify_documentation_content(),
    }
    
    get_file_stats()
    
    # Summary
    print_section("VERIFICATION SUMMARY")
    
    passed = sum(1 for v in checks.values() if v)
    total = len(checks)
    
    for check_name, result in checks.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status:10s} {check_name}")
    
    print(f"\n  Overall: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n  ✅ ALL CHECKS PASSED - Ready for Integration!")
    else:
        print(f"\n  ⚠️  {total - passed} check(s) failed - See details above")
    
    show_deployment_readiness()
    
    print_section("NEXT STEPS")
    print("""
1. Review all checks above
2. Fix any failures indicated with ❌
3. Read: EXACT_CODE_INTEGRATION.md (copy-paste code changes)
4. Run: python deploy_kb_matching.py --guide (deployment walkthrough)
5. Follow: PRODUCTION_DEPLOYMENT_GUIDE.md (detailed integration)
6. Test: pytest test_kb_matcher_advanced.py -v (verify tests)
7. Deploy to staging first (not production yet)
    """)
    
    print("\n" + "="*70)
    
    # Exit code
    return 0 if passed == total else 1


if __name__ == '__main__':
    sys.exit(main())
