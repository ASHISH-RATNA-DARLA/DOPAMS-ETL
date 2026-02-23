#!/usr/bin/env python3
"""
Setup Verification Script
Checks if all required services and dependencies are properly configured
"""
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from typing import Dict, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SetupVerifier:
    """Verify system setup and dependencies"""
    
    def __init__(self):
        self.results = {}
        self.all_passed = True
    
    def print_header(self, title: str):
        """Print section header"""
        print("\n" + "=" * 70)
        print(f"  {title}")
        print("=" * 70)
    
    def print_result(self, name: str, passed: bool, message: str = ""):
        """Print test result"""
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {name}")
        if message and not passed:
            print(f"       {message}")
        self.results[name] = passed
        if not passed:
            self.all_passed = False
    
    def check_python_version(self) -> Tuple[bool, str]:
        """Check Python version"""
        version = sys.version_info
        if version.major == 3 and version.minor >= 9:
            return True, f"Python {version.major}.{version.minor}.{version.micro}"
        return False, f"Python {version.major}.{version.minor} - Need 3.9+"
    
    def check_python_packages(self) -> Dict[str, Tuple[bool, str]]:
        """Check required Python packages"""
        packages = {
            'flask': 'Flask',
            'flask_cors': 'Flask-CORS',
            'flask_limiter': 'Flask-Limiter',
            'psycopg2': 'PostgreSQL Driver',
            'pymongo': 'MongoDB Driver',
            'redis': 'Redis Client',
            'langgraph': 'LangGraph',
            'requests': 'Requests',
            'dotenv': 'Python Dotenv'
        }
        
        results = {}
        for module, name in packages.items():
            try:
                __import__(module)
                results[name] = (True, "Installed")
            except ImportError:
                results[name] = (False, "Not installed")
        
        return results
    
    def check_env_file(self) -> Tuple[bool, str]:
        """Check if .env file exists"""
        env_path = Path(__file__).parent.parent / '.env'
        if env_path.exists():
            return True, f"Found at {env_path}"
        return False, ".env file not found - copy from env.example"
    
    def check_env_variables(self) -> Dict[str, Tuple[bool, str]]:
        """Check required environment variables"""
        from dotenv import load_dotenv
        load_dotenv()
        
        required_vars = {
            'FLASK_SECRET_KEY': 'Flask Secret Key',
            'POSTGRES_HOST': 'PostgreSQL Host',
            'POSTGRES_DB': 'PostgreSQL Database',
            'POSTGRES_USER': 'PostgreSQL User',
            'MONGO_HOST': 'MongoDB Host',
            'MONGO_DB': 'MongoDB Database',
            'REDIS_HOST': 'Redis Host',
            'LLM_API_URL': 'LLM API URL',
            'LLM_MODEL': 'LLM Model'
        }
        
        results = {}
        for var, name in required_vars.items():
            value = os.getenv(var)
            if value and value != '':
                results[name] = (True, f"Set")
            else:
                results[name] = (False, f"{var} not set")
        
        return results
    
    def check_postgresql(self) -> Tuple[bool, str]:
        """Check PostgreSQL connection"""
        try:
            from config import Config
            from database.postgres_executor import PostgreSQLExecutor
            
            executor = PostgreSQLExecutor()
            if executor.test_connection():
                executor.close()
                return True, "Connected successfully"
            return False, "Connection test failed"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def check_mongodb(self) -> Tuple[bool, str]:
        """Check MongoDB connection"""
        try:
            from config import Config
            from database.mongo_executor import MongoDBExecutor
            
            executor = MongoDBExecutor()
            if executor.test_connection():
                executor.close()
                return True, "Connected successfully"
            return False, "Connection test failed"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def check_redis(self) -> Tuple[bool, str]:
        """Check Redis connection"""
        try:
            from config import Config
            from cache.redis_manager import RedisManager
            
            manager = RedisManager()
            if manager.is_available():
                manager.close()
                return True, "Connected successfully"
            return False, "Connection failed"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def check_ollama(self) -> Tuple[bool, str]:
        """Check Ollama/LLM service"""
        try:
            import requests
            from config import Config
            
            url = Config.LLM_CONFIG['api_url']
            response = requests.get(f"{url}/api/tags", timeout=5)
            
            if response.status_code == 200:
                models = response.json().get('models', [])
                model_name = Config.LLM_CONFIG['model']
                
                # Check if configured model exists
                model_exists = any(
                    model_name in m.get('name', '') for m in models
                )
                
                if model_exists:
                    return True, f"Connected, model '{model_name}' available"
                else:
                    return False, f"Model '{model_name}' not found - run: ollama pull {model_name}"
            
            return False, f"HTTP {response.status_code}"
        except requests.exceptions.ConnectionError:
            return False, "Cannot connect - Is Ollama running? Run: ollama serve"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def check_llm_client(self) -> Tuple[bool, str]:
        """Check LLM client initialization"""
        try:
            from agents.llm_client import LocalLLMClient
            
            client = LocalLLMClient()
            # Try a simple detection
            intent = client.detect_intent("Show me all users")
            if intent:
                return True, f"LLM client working, detected intent: {intent}"
            return False, "LLM client initialized but no response"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def run_all_checks(self):
        """Run all verification checks"""
        print("\n" + "╔" + "═" * 68 + "╗")
        print("║" + " " * 15 + "🔍 SETUP VERIFICATION SCRIPT" + " " * 24 + "║")
        print("╚" + "═" * 68 + "╝")
        
        # 1. Python Environment
        self.print_header("1. Python Environment")
        passed, msg = self.check_python_version()
        self.print_result("Python Version", passed, msg)
        
        # 2. Python Packages
        self.print_header("2. Python Packages")
        packages = self.check_python_packages()
        for name, (passed, msg) in packages.items():
            self.print_result(name, passed, msg)
        
        # 3. Environment Configuration
        self.print_header("3. Environment Configuration")
        passed, msg = self.check_env_file()
        self.print_result(".env File", passed, msg)
        
        if passed:
            env_vars = self.check_env_variables()
            for name, (var_passed, var_msg) in env_vars.items():
                self.print_result(name, var_passed, var_msg)
        
        # 4. Database Services
        self.print_header("4. Database Services")
        
        passed, msg = self.check_postgresql()
        self.print_result("PostgreSQL Connection", passed, msg)
        
        passed, msg = self.check_mongodb()
        self.print_result("MongoDB Connection", passed, msg)
        
        passed, msg = self.check_redis()
        self.print_result("Redis Connection", passed, msg)
        
        # 5. LLM Service
        self.print_header("5. LLM Service")
        
        passed, msg = self.check_ollama()
        self.print_result("Ollama Service", passed, msg)
        
        if passed:
            llm_passed, llm_msg = self.check_llm_client()
            self.print_result("LLM Client", llm_passed, llm_msg)
        
        # Summary
        self.print_summary()
    
    def print_summary(self):
        """Print verification summary"""
        self.print_header("VERIFICATION SUMMARY")
        
        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)
        failed = total - passed
        
        print(f"\nTotal Checks: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        
        if self.all_passed:
            print("\n" + "🎉" * 30)
            print("✅ ALL CHECKS PASSED! System is ready to run.")
            print("🎉" * 30)
            print("\nYou can now start the application:")
            print("  python app.py")
        else:
            print("\n" + "⚠️" * 30)
            print("❌ SOME CHECKS FAILED! Please fix the issues above.")
            print("⚠️" * 30)
            print("\nReview the failed checks and:")
            print("  1. Ensure all services are running")
            print("  2. Verify .env configuration")
            print("  3. Check connection credentials")
            print("  4. See UBUNTU_SETUP.md for detailed instructions")
        
        print("\n" + "=" * 70 + "\n")
        
        return 0 if self.all_passed else 1


def main():
    """Main entry point"""
    verifier = SetupVerifier()
    exit_code = verifier.run_all_checks()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()

