"""
Universal LLM Client - Professional Version with DOPAMS AI Enhancements
Supports Ollama (local), OpenAI, and Anthropic
"""

import re
import json
import logging
import requests
from typing import Optional, Dict, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
import sys
import os

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from core.llm_service import get_llm

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class LLMConfig:
    """Configuration for LLM providers"""
    provider: str = 'ollama'
    api_url: str = 'http://localhost:11434'
    api_key: Optional[str] = None
    model: str = 'llama2'
    temperature: float = 0.7
    max_tokens: int = 500
    timeout: int = 120
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'LLMConfig':
        """Create config from dictionary"""
        return cls(**{k: v for k, v in config.items() if k in cls.__annotations__})
    
    @classmethod
    def from_env(cls) -> 'LLMConfig':
        """Create config from environment variables"""
        import os
        return cls(
            provider=os.getenv('LLM_PROVIDER'),
            api_url=os.getenv('LLM_API_URL'),
            api_key=os.getenv('LLM_API_KEY'),
            model=os.getenv('LLM_MODEL_SQL'),
            temperature=float(os.getenv('LLM_TEMPERATURE')),
            max_tokens=int(os.getenv('LLM_MAX_TOKENS')),
            timeout=int(os.getenv('LLM_TIMEOUT_SECONDS'))
        )

# ============================================================================
# Base Provider Interface
# ============================================================================

class LLMProvider(ABC):
    """Abstract base class for LLM providers"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
    
    @abstractmethod
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """Generate text from prompt"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if provider is accessible"""
        pass

# ============================================================================
# Ollama Provider (Optimized for DOPAMS AI)
# ============================================================================

class OllamaProvider(LLMProvider):
    """Ollama local LLM provider - Optimized version"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.llm_service = get_llm('sql')
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """Generate using Ollama with detailed logging via core/llm_service"""
        return self.llm_service.generate(prompt=prompt, system_prompt=system_prompt)
    
    def test_connection(self) -> bool:
        """Test Ollama connection"""
        import requests
        try:
            response = requests.get(f"{self.llm_service.api_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False

# ============================================================================
# OpenAI Provider
# ============================================================================

class OpenAIProvider(LLMProvider):
    """OpenAI API provider"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize OpenAI client"""
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=self.config.api_key)
            logger.info(f"OpenAI initialized: {self.config.model}")
        except ImportError:
            logger.error("OpenAI package not installed. Run: pip install openai")
        except Exception as e:
            logger.error(f"OpenAI initialization failed: {e}")
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """Generate using OpenAI"""
        if not self.client:
            logger.error("OpenAI client not initialized")
            return None
        
        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            logger.info(f"OpenAI request: {self.config.model}")
            
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                timeout=self.config.timeout
            )
            
            result = response.choices[0].message.content.strip()
            logger.info(f"OpenAI response: {len(result)} chars")
            return result
            
        except Exception as e:
            logger.error(f"OpenAI request failed: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test OpenAI connection"""
        return self.client is not None

# ============================================================================
# Anthropic Provider
# ============================================================================

class AnthropicProvider(LLMProvider):
    """Anthropic Claude API provider"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Anthropic client"""
        try:
            import anthropic
            self.client = anthropic.Anthropic(api_key=self.config.api_key)
            logger.info(f"Anthropic initialized: {self.config.model}")
        except ImportError:
            logger.error("Anthropic package not installed. Run: pip install anthropic")
        except Exception as e:
            logger.error(f"Anthropic initialization failed: {e}")
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """Generate using Anthropic Claude"""
        if not self.client:
            logger.error("Anthropic client not initialized")
            return None
        
        try:
            logger.info(f"Anthropic request: {self.config.model}")
            
            kwargs = {
                "model": self.config.model,
                "max_tokens": self.config.max_tokens,
                "temperature": self.config.temperature,
                "messages": [{"role": "user", "content": prompt}]
            }
            
            if system_prompt:
                kwargs["system"] = system_prompt
            
            response = self.client.messages.create(**kwargs)
            
            result = response.content[0].text.strip()
            logger.info(f"Anthropic response: {len(result)} chars")
            return result
            
        except Exception as e:
            logger.error(f"Anthropic request failed: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test Anthropic connection"""
        return self.client is not None

# ============================================================================
# Universal LLM Client (DOPAMS AI Enhanced)
# ============================================================================

class UniversalLLMClient:
    """Universal client supporting multiple LLM providers"""
    
    def __init__(self, config: Optional[LLMConfig] = None):
        """
        Initialize Universal LLM Client
        
        Args:
            config: LLMConfig instance. If None, loads from environment
        """
        self.config = config or LLMConfig.from_env()
        self.provider = self._create_provider()
        
        logger.info(f"Using {self.config.provider} with model: {self.config.model}")
    
    def _create_provider(self) -> LLMProvider:
        """Create appropriate provider based on config"""
        providers = {
            'openai': OpenAIProvider,
            'ollama': OllamaProvider,
            'anthropic': AnthropicProvider
        }
        
        provider_class = providers.get(self.config.provider.lower())
        if not provider_class:
            raise ValueError(f"Unsupported provider: {self.config.provider}")
        
        return provider_class(self.config)
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """Generate text using configured provider"""
        return self.provider.generate(prompt, system_prompt)
    
    def test_connection(self) -> bool:
        """Test if LLM is accessible"""
        return self.provider.test_connection()
    
    # ========================================================================
    # SQL Generation (DOPAMS AI Optimized)
    # ========================================================================
    
    def generate_sql_with_context(self, user_message: str, schema: str, query_plan: Dict[str, Any]) -> Optional[str]:
        """Generate SQL query with intelligent context (DOPAMS AI method)"""
        
        # Build context hints
        hints = []
        
        # ⭐ NEW: Query Pattern Recognition (WHO/WHAT/WHERE/WHEN/HOW MANY/WHY)
        # This helps the LLM understand user intent and choose the right query strategy
        message_lower = user_message.lower()
        
        # WHO questions → Person/Accused queries
        if any(kw in message_lower for kw in ['who is', 'who are', 'who committed', 'who involved', 'find person', 'find accused']):
            hints.append("⭐ WHO QUESTION: User wants person/accused information. Use persons table (person_id) or accused table (accused_id). Join with crimes for crime context.")
        
        # WHAT questions → Details/Information queries
        if any(kw in message_lower for kw in ['what is', 'what are', 'what happened', 'what drugs', 'what properties', 'what details']):
            hints.append("⭐ WHAT QUESTION: User wants detailed information. Include comprehensive fields from relevant tables. Use brief_facts tables for additional context.")
        
        # WHERE questions → Geographic/Location queries
        if any(kw in message_lower for kw in ['where', 'in district', 'at police station', 'in area', 'location', 'geographic']):
            hints.append("⭐ WHERE QUESTION: User wants geographic information. JOIN hierarchy table using ps_code. Use hierarchy columns: ps_name, dist_name, circle_name, zone_name.")
        
        # WHEN questions → Temporal/Time-based queries
        if any(kw in message_lower for kw in ['when', 'recent', 'trend', 'monthly', 'yearly', 'over time', 'between dates', 'date range']):
            hints.append("⭐ WHEN QUESTION: User wants temporal analysis. Use DATE_TRUNC('month', fir_date) or EXTRACT(YEAR FROM fir_date) for grouping. Use BETWEEN for date ranges.")
        
        # HOW MANY questions → Count/Aggregation queries
        if any(kw in message_lower for kw in ['how many', 'count', 'total', 'number of', 'how much']):
            hints.append("⭐ HOW MANY QUESTION: User wants counts/statistics. Use COUNT(*), SUM(), AVG() with appropriate GROUP BY. This is an aggregation query!")
        
        # WHY questions → Analytical queries (combine multiple strategies)
        if any(kw in message_lower for kw in ['why', 'analyze', 'pattern', 'trend', 'compare', 'analysis']):
            hints.append("⭐ WHY/ANALYSIS QUESTION: User wants analytical insights. Combine multiple strategies: temporal trends + geographic analysis + aggregations. Use CTEs if needed.")
        
        # ⚠️ CRITICAL: Detect acts/sections queries and force correct column!
        if any(kw in message_lower for kw in ['acts', 'sections', 'ndpsa', 'ipc', '8c', '20b', '27', 'r/w']):
            hints.append("⚠️ CRITICAL: User mentioned acts/sections! Search crimes.acts_sections column ONLY! NOT crime_type, NOT major_head!")
            hints.append("Example: WHERE c.acts_sections ILIKE '%8c%' OR c.acts_sections ILIKE '%NDPSA%'")
        
        # ⚠️ CRITICAL: Detect commercial quantity queries
        if any(kw in message_lower for kw in ['commercial quantity', 'commercial drug', 'commercial case', 'is commercial']):
            hints.append("⚠️ CRITICAL: User asked for commercial quantity drug cases! Use brief_facts_drugs.is_commercial = true (BOOLEAN field)!")
            hints.append("Example: WHERE d.is_commercial = true (NOT WHERE d.total_quantity LIKE '%commercial%' - that's wrong!)")
        
        # ⚠️ CRITICAL: Handle queries asking for "information about" fields (don't filter by NOT NULL)
        info_about_keywords = [
            'hair color', 'hair color information', 'eye color', 'eye color details',
            'build type', 'build type information', 'mole', 'leucoderma', 
            'height information', 'height', 'seizure worth', 'packaging details',
            'information about', 'with hair', 'with eye', 'with build',
            'with mole', 'with leucoderma', 'with height', 'show drugs with',
            'show accused with', 'list accused with', 'find accused with',
            'source location', 'seizure location', 'show drugs with source',
            'show drugs with seizure', 'find drugs seized from', 'find drugs seized at'
        ]
        if any(kw in message_lower for kw in info_about_keywords):
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'information about' a field!")
            hints.append("⚠️ CRITICAL: DO NOT filter by 'field IS NOT NULL' - show ALL records and indicate which have the field!")
            hints.append("✅ CORRECT: SELECT all records, show field value if exists, show 'Not available' if NULL")
            hints.append("❌ WRONG: WHERE field IS NOT NULL (this will exclude records where field is empty!)")
            hints.append("Example: 'Show accused with hair color' → SELECT all accused, show a.hair if exists (column is 'hair' NOT 'hair_color'!)")
            hints.append("Example: 'Show accused with eye color' → SELECT all accused, show a.eyes if exists (column is 'eyes' NOT 'eye_color'!)")
            hints.append("Example: 'Find drugs seized from source location' → SELECT all drugs, show d.source_location if exists")
            hints.append("⚠️ CRITICAL: Actual column names in accused table: hair (NOT hair_color), eyes (NOT eye_color), height, build, color, mole, leucoderma")
        
        # ⚠️ CRITICAL: Performance optimization hints
        if any(kw in message_lower for kw in ['complete', 'all details', 'full', 'everything', 'all accused', 'all properties', 'all drugs']):
            hints.append("⚠️ PERFORMANCE: User asked for 'complete' or 'all' details - this may be slow!")
            hints.append("💡 OPTIMIZATION: Use LIMIT 500 for 'all' queries (NOT unlimited!)")
            hints.append("💡 OPTIMIZATION: Use SELECT only needed columns, not SELECT *")
            hints.append("💡 OPTIMIZATION: Add WHERE clauses to filter early, reduce JOIN data")
        
        # ⚠️ CRITICAL: Always add LIMIT for performance (unless user explicitly wants unlimited)
        if 'limit' not in message_lower and 'unlimited' not in message_lower:
            hints.append("💡 PERFORMANCE: Always add LIMIT clause (default: LIMIT 100) to prevent slow queries!")
            hints.append("💡 PERFORMANCE: LIMIT 100 is automatically added if missing - but you should include it in generated query!")
        
        # ⚠️ CRITICAL: Complex JOIN optimization
        if any(kw in message_lower for kw in ['with accused', 'with properties', 'with drugs', 'crime profile', 'complete crime']):
            hints.append("⚠️ PERFORMANCE: Complex query with multiple JOINs detected!")
            hints.append("💡 OPTIMIZATION: Use INNER JOIN (not LEFT JOIN) when possible to reduce result set")
            hints.append("💡 OPTIMIZATION: Add WHERE clauses BEFORE JOINs to filter early")
            hints.append("💡 OPTIMIZATION: Consider using subqueries for large result sets")
            hints.append("💡 OPTIMIZATION: Use DISTINCT only when necessary (it's expensive!)")
            hints.append("💡 OPTIMIZATION: ALWAYS add LIMIT clause (e.g., LIMIT 100) for complex JOINs!")
        
        # ⭐ NEW: Use auto-detected columns from column mapper!
        required_columns = query_plan.get('required_columns', [])
        if required_columns:
            # Group columns by table
            columns_by_table = {}
            for col in required_columns:
                table = col.get('table', '')
                if table not in columns_by_table:
                    columns_by_table[table] = []
                columns_by_table[table].append(col.get('column', ''))
            
            # Add hints for each table
            for table, cols in columns_by_table.items():
                table_alias = {
                    'brief_facts_drugs': 'd',
                    'properties': 'pr',
                    'persons': 'p',
                    'crimes': 'c',
                    'brief_facts_accused': 'bfa',
                    'accused': 'a',
                    'hierarchy': 'h'
                }.get(table, table[:1])
                
                col_list = ', '.join([f"{table_alias}.{col}" for col in cols])
                hints.append(f"⭐ USER SPECIFICALLY ASKED FOR: {col_list} from {table} table - MUST INCLUDE these columns!")
                hints.append(f"These columns are AUTO-DETECTED from user question - they KNOW what they want!")
        
        # ⚠️ CRITICAL: Detect drug supply chain/seizure queries - must use BOTH tables!
        if any(kw in message_lower for kw in ['drug supply chain', 'supply chain', 'drug seizure', 'drug information', 'drug details']):
            hints.append("⚠️ CRITICAL: User asked about drugs! MUST JOIN BOTH brief_facts_drugs AND properties tables!")
            hints.append("Include: d.supply_chain, d.source_location, d.destination, d.transport_method from brief_facts_drugs")
            hints.append("Include: pr.nature, pr.category, pr.particular_of_property, pr.property_status, pr.estimate_value from properties")
            hints.append("Use correct column names: pr.nature (NOT pr.property_type!), pr.particular_of_property (NOT pr.property_description!)")
        
        # ⚠️ CRITICAL: Location queries need ILIKE for partial matching (values contain full text)
        if any(kw in message_lower for kw in ['source location', 'seizure location', 'seized from', 'seized at']):
            hints.append("⚠️ CRITICAL: Location values contain full text descriptions, not exact matches!")
            hints.append("Example: source_location = 'Ganja originated from Guntur, AP State (supplied by...)'")
        
        # ⚠️ CRITICAL: State queries - show both present_state_ut and permanent_state_ut
        if any(kw in message_lower for kw in ['from state', 'state', 'present state', 'permanent state']):
            hints.append("⚠️ CRITICAL: User asked about state information!")
            hints.append("✅ MUST include BOTH present_state_ut AND permanent_state_ut columns from persons table!")
            hints.append("✅ Show both values if both are available, show one if only one is available, show 'No data available' if neither is available")
            hints.append("Example: SELECT p.full_name, p.present_state_ut, p.permanent_state_ut FROM persons p WHERE p.present_state_ut ILIKE '%Telangana%' OR p.permanent_state_ut ILIKE '%Telangana%'")
        
        # ⚠️ CRITICAL: Brand name queries - use drug_name or properties.nature as alternatives
        if any(kw in message_lower for kw in ['brand name', 'by brand', 'brand']):
            hints.append("⚠️ CRITICAL: User asked about brand name!")
            hints.append("⚠️ NOTE: brand_name column in brief_facts_drugs is mostly empty (0% populated)")
            hints.append("✅ USE ALTERNATIVES: Use drug_name from brief_facts_drugs OR nature from properties table as brand name alternatives")
            hints.append("✅ CORRECT: SELECT d.drug_name as brand_name, pr.nature as property_brand FROM brief_facts_drugs d LEFT JOIN properties pr ON d.crime_id = pr.crime_id")
            hints.append("✅ GROUP BY: Use GROUP BY d.drug_name or GROUP BY pr.nature to list drugs by brand/name")
        
        # ⚠️ CRITICAL: Detect "pending cases" queries - map to actual case_status values
        if any(kw in message_lower for kw in ['pending cases', 'pending', 'list pending', 'show pending']):
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'pending cases'!")
            hints.append("⚠️ CRITICAL: Actual case_status values in database: 'UI' (Under Investigation), 'PT' (Pending Trial), 'Disposal' (Closed)")
            hints.append("❌ WRONG: WHERE c.case_status ILIKE '%pending%' (this won't match 'UI' or 'PT'!)")
            hints.append("✅ CORRECT: WHERE c.case_status IN ('UI', 'PT') (to get all pending/non-closed cases)")
            hints.append("✅ ALTERNATIVE: WHERE c.case_status NOT IN ('Disposal') (exclude closed cases)")
        
        # ⚠️ CRITICAL: Detect date queries with "onwards" or "from date"
        if any(kw in message_lower for kw in ['onwards', 'from', 'after', 'since']):
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for date range with 'onwards' or 'from'!")
            hints.append("⚠️ CRITICAL: Date format in database is YYYY-MM-DD HH:MM:SS (e.g., '2025-02-27 17:30:00.42')")
            hints.append("⚠️ CRITICAL: User may provide dates in DD-MM-YYYY format (e.g., '11-05-2025') - convert to YYYY-MM-DD!")
            hints.append("✅ CORRECT: WHERE c.fir_date >= '2025-05-11'::date (for 'from 11-05-2025 onwards')")
            hints.append("✅ CORRECT: WHERE c.fir_date >= DATE('2025-05-11') (alternative syntax)")
            hints.append("⚠️ CRITICAL: For 'onwards' queries, use >= operator, NOT = operator!")
            hints.append("Example: 'from 11-05-2025 onwards' → WHERE c.fir_date >= '2025-05-11'::date")
        
        # ⚠️ CRITICAL: Detect date queries with DD-MM-YYYY format (e.g., "27-02-2025")
        if re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}', user_message):
            # Check if it's DD-MM-YYYY (first part is day, <= 31)
            date_match = re.search(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', user_message)
            if date_match:
                day, month, year = date_match.groups()
                if int(day) <= 31 and int(month) <= 12:
                    # Likely DD-MM-YYYY format
                    hints.append("⚠️⚠️⚠️ CRITICAL: User provided date in DD-MM-YYYY format!")
                    hints.append(f"⚠️ CRITICAL: Convert '{day}-{month}-{year}' to YYYY-MM-DD format: '{year}-{month}-{day}'")
                    hints.append("✅ CORRECT: WHERE c.fir_date::date = '2025-02-27' (for '27-02-2025')")
                    hints.append("✅ CORRECT: WHERE DATE(c.fir_date) = '2025-02-27' (alternative syntax)")
                    hints.append("❌ WRONG: WHERE c.fir_date = '27-02-2025' (this format doesn't match database!)")
        
        # ⚠️ CRITICAL: Detect "last 7 days", "recently modified", "recently created"
        if any(kw in message_lower for kw in ['last 7 days', 'recently modified', 'recently created', 'last week']):
            from datetime import datetime, timedelta
            today = datetime.now()
            seven_days_ago = today - timedelta(days=7)
            today_str = today.strftime('%Y-%m-%d')
            seven_days_ago_str = seven_days_ago.strftime('%Y-%m-%d')
            hints.append(f"⚠️⚠️⚠️ CRITICAL: User asked for 'last 7 days' or 'recently' - use date range!")
            hints.append(f"⚠️ CRITICAL: Date range: from {seven_days_ago_str} to {today_str}")
            hints.append(f"✅ CORRECT: WHERE date_modified >= '{seven_days_ago_str}' AND date_modified <= '{today_str}'")
            hints.append(f"✅ CORRECT: WHERE date_created >= '{seven_days_ago_str}' AND date_created <= '{today_str}'")
            hints.append("✅ CORRECT: Use BETWEEN for date ranges: WHERE date_modified BETWEEN '2025-11-09' AND '2025-11-16'")
        
        # ⚠️ CRITICAL: District/State queries need case-insensitive matching
        if any(kw in message_lower for kw in ['district', 'state', 'from district', 'from state', 'present district', 'present state']):
            hints.append("⚠️ CRITICAL: District and State values in database are UPPERCASE (e.g., 'TELANGANA', 'KARIMNAGAR')!")
            hints.append("⚠️ CRITICAL: User may provide mixed case (e.g., 'Hyderabad', 'Telangana') - use ILIKE for case-insensitive matching!")
            hints.append("✅ CORRECT: WHERE p.present_district ILIKE '%Hyderabad%' (case-insensitive)")
            hints.append("✅ CORRECT: WHERE p.present_state_ut ILIKE '%Telangana%' (case-insensitive)")
            hints.append("❌ WRONG: WHERE p.present_district = 'Hyderabad' (exact match won't work - database has 'KARIMNAGAR' format!)")
        
        # ⚠️ CRITICAL: Missing fields queries (IS NULL)
        if any(kw in message_lower for kw in ['missing', 'without', 'not have', 'no', 'null']):
            if any(kw in message_lower for kw in ['physical features', 'height', 'build', 'color', 'hair', 'eyes']):
                hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'missing physical features' - use IS NULL for multiple fields!")
                hints.append("✅ CORRECT: WHERE (a.height IS NULL AND a.build IS NULL AND a.color IS NULL AND a.hair IS NULL AND a.eyes IS NULL)")
                hints.append("✅ CORRECT: Use OR for 'any missing': WHERE (a.height IS NULL OR a.build IS NULL OR a.color IS NULL)")
            elif 'estimate_value' in message_lower:
                hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'missing estimate_value' - use IS NULL!")
                hints.append("✅ CORRECT: WHERE pr.estimate_value IS NULL")
            elif 'case_status' in message_lower:
                hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'missing case_status' - use IS NULL!")
                hints.append("✅ CORRECT: WHERE c.case_status IS NULL")
        
        # ⚠️ CRITICAL: Properties linkage queries
        if any(kw in message_lower for kw in ['properties recovered from', 'properties linked to', 'properties seized from']):
            hints.append("⚠️⚠️⚠️ CRITICAL: Properties are linked via crime_id, NOT directly to person_id or accused_id!")
            hints.append("⚠️ CRITICAL: To find properties for a person: properties → crimes → accused → persons")
            hints.append("✅ CORRECT: JOIN properties pr ON pr.crime_id = c.crime_id JOIN accused a ON c.crime_id = a.crime_id JOIN persons p ON a.person_id = p.person_id")
            hints.append("⚠️ CRITICAL: Also check pr.recovered_from field - it may contain person information!")
        
        # ⚠️ CRITICAL: Multiple states query (GROUP BY with HAVING)
        if 'multiple states' in message_lower or 'accused from multiple' in message_lower:
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'crimes with accused from multiple states'!")
            hints.append("⚠️ CRITICAL: Need GROUP BY crime_id with HAVING COUNT(DISTINCT state) > 1")
            hints.append("✅ CORRECT: SELECT c.crime_id, COUNT(DISTINCT p.present_state_ut) as state_count FROM crimes c JOIN accused a ON c.crime_id = a.crime_id JOIN persons p ON a.person_id = p.person_id GROUP BY c.crime_id HAVING COUNT(DISTINCT p.present_state_ut) > 1")
        
        # ⚠️ CRITICAL: High-value property queries
        if any(kw in message_lower for kw in ['high-value', 'high value', 'seizure report', 'properties >', 'estimate_value >']):
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'high-value' properties!")
            hints.append("⚠️ CRITICAL: Filter by pr.estimate_value > threshold (e.g., 100000)")
            hints.append("✅ CORRECT: WHERE pr.estimate_value > 100000")
            hints.append("⚠️ CRITICAL: Must JOIN properties pr ON pr.crime_id = c.crime_id to get crime details!")
        
        # ⚠️ CRITICAL: Text search queries (similar, pattern matching)
        if any(kw in message_lower for kw in ['similar', 'pattern', 'matching', 'search']):
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'similar' or 'pattern matching' - use ILIKE!")
            hints.append("✅ CORRECT: WHERE p.full_name ILIKE '%pattern%' (for name search)")
            hints.append("✅ CORRECT: WHERE pr.particular_of_property ILIKE '%pattern%' (for property description)")
            hints.append("✅ CORRECT: WHERE c.crime_type ILIKE '%pattern%' (for crime type)")
            hints.append("✅ CORRECT: WHERE c.brief_facts ILIKE '%pattern%' (for brief facts)")
        
        # ⚠️ CRITICAL: Purity query (show all drugs, indicate which have purity)
        if 'purity' in message_lower:
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'purity' - most drugs don't have purity data!")
            hints.append("⚠️ CRITICAL: DO NOT filter by 'd.purity IS NOT NULL' - show ALL drugs and indicate which have purity!")
            hints.append("✅ CORRECT: SELECT all drugs, show d.purity if exists, show 'Not available' if NULL")
        
        # ⚠️ CRITICAL: Detect MongoDB-specific date field queries
        if any(kw in message_lower for kw in ['reg_dt', 'from_dt', 'to_dt', 'ps_recv_inform_dt', 'fir_reg_num', 'face_type', 'int_father']):
            hints.append("⚠️⚠️⚠️ CRITICAL: User mentioned MongoDB-specific fields!")
            hints.append("⚠️ CRITICAL: These fields exist ONLY in MongoDB (fir_records collection), NOT in PostgreSQL!")
            hints.append("⚠️ CRITICAL: MongoDB field names are UPPERCASE: REG_DT, FROM_DT, TO_DT, PS_RECV_INFORM_DT, FIR_REG_NUM, FACE_TYPE, INT_FATHER_NAME, etc.")
            hints.append("⚠️ CRITICAL: If target_database is 'both' or 'mongodb', generate MongoDB query with UPPERCASE field names!")
            hints.append("⚠️ CRITICAL: For date queries, use REG_DT, FROM_DT, or TO_DT (NOT fir_date - that's PostgreSQL!)")
            hints.append("Example MongoDB query: {\"collection\": \"fir_records\", \"query\": {\"REG_DT\": {\"$gte\": {\"$date\": \"2025-05-11T00:00:00Z\"}}}}")
        
        # Detect GROUP BY queries
        if any(kw in message_lower for kw in ['by status', 'by class', 'by type', 'by district', 'by property_status', 'by property', 'crimes by', 'cases by']):
            hints.append("⚠️ This is a GROUP BY query! Use: SELECT field, COUNT(*) FROM crimes GROUP BY field")
            if 'property_status' in message_lower or 'property' in message_lower:
                hints.append("⚠️ For property_status: JOIN properties table! Example: SELECT pr.property_status, COUNT(*) FROM crimes c JOIN properties pr ON c.crime_id = pr.crime_id GROUP BY pr.property_status")
        
        # ⚠️ CRITICAL: Detect NDPS Act queries
        if any(kw in message_lower for kw in ['ndps act', 'ndpsa', 'ndps violations', 'ndps act violations']):
            hints.append("⚠️ CRITICAL: User asked about NDPS Act violations!")
            hints.append("⚠️ CRITICAL: Use crimes.acts_sections column (contains NDPS Act sections)!")
            hints.append("✅ CORRECT: WHERE c.acts_sections ILIKE '%NDPS%' OR c.acts_sections ILIKE '%NDPSA%'")
            hints.append("⚠️ CRITICAL: acts_sections is a TEXT field that may contain multiple sections separated by commas!")
            hints.append("Example: acts_sections = '8c NDPSA, r/w 20(b)(ii)(B) NDPSA, 27(A) NDPSA'")
        
        # ⚠️ CRITICAL: Detect police station/hierarchy queries
        if any(kw in message_lower for kw in ['all police stations', 'list police stations', 'police stations with', 'hierarchy']):
            hints.append("⚠️ CRITICAL: User asked for police station information!")
            hints.append("⚠️ CRITICAL: Use hierarchy table (NOT crimes table directly)!")
            hints.append("✅ CORRECT: SELECT DISTINCT h.ps_code, h.ps_name, h.dist_name FROM hierarchy h ORDER BY h.ps_name")
            hints.append("⚠️ CRITICAL: hierarchy table has: ps_code, ps_name, dist_name, circle_name, zone_name")
            hints.append("❌ WRONG: SELECT ps_code FROM crimes (this won't show all stations, only those with crimes!)")
        
        # ⚠️ CRITICAL: Detect CCL (Child in Conflict with Law) queries
        if any(kw in message_lower for kw in ['is_ccl', 'ccl', 'child in conflict', 'is ccl', 'who are ccl']):
            hints.append("⚠️ CRITICAL: User asked about CCL (Child in Conflict with Law)!")
            hints.append("⚠️ CRITICAL: Use accused.is_ccl = true (BOOLEAN field)!")
            hints.append("✅ CORRECT: WHERE a.is_ccl = true (NOT WHERE a.is_ccl = 'true' - it's a boolean!)")
            hints.append("⚠️ CRITICAL: Must JOIN accused table! Example: FROM crimes c JOIN accused a ON c.crime_id = a.crime_id WHERE a.is_ccl = true")
        
        # ⚠️ CRITICAL: Detect IO rank queries (NOT IO name!)
        if any(kw in message_lower for kw in ['io rank', 'io_rank', 'rank is', 'where io rank', 'io rank is']):
            hints.append("⚠️⚠️⚠️ CRITICAL: User asked about IO RANK (NOT IO name)!")
            hints.append("⚠️ CRITICAL: Use c.io_rank column (NOT c.io_name!)")
            hints.append("❌ WRONG: WHERE c.io_name = 'Inspector' (io_name is the person's name, NOT the rank!)")
            hints.append("✅ CORRECT: WHERE c.io_rank ILIKE '%Inspector%' (io_rank is the rank field!)")
            # Extract rank value if mentioned
            rank_match = re.search(r"['\"]([^'\"]+)['\"]", message_lower)
            if rank_match:
                rank_value = rank_match.group(1)
                hints.append(f"Filter by rank: WHERE c.io_rank ILIKE '%{rank_value}%'")
        
        # Detect accused role/type queries
        if any(kw in message_lower for kw in ['accused role', 'accused type', 'by accused role', 'by accused type', 'accused involvement', 'involvement patterns']):
            hints.append("⚠️ CRITICAL: User asked about accused role/type/involvement! Use brief_facts_accused.accused_type column (NOT type, NOT role_in_crime)!")
            hints.append("⚠️ CRITICAL JOIN ORDER: JOIN accused a FIRST, THEN JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id")
            hints.append("⚠️ CRITICAL: Column name is accused_type (NOT type!)")
            hints.append("⚠️ CRITICAL: When user mentions 'peddler', 'supplier', etc. in context of accused type/involvement → Use bfa.accused_type, NOT persons.alias or accused.type!")
            # Check for specific role types
            role_types = ['peddler', 'consumer', 'organizer_kingpin', 'supplier', 'manufacturer', 'processor', 'harbourer', 'kingpin', 'dealer']
            detected_role = next((role for role in role_types if role in message_lower), None)
            if detected_role:
                # User specified a specific role
                hints.append(f"Filter by role: WHERE bfa.accused_type ILIKE '%{detected_role}%'")
                hints.append("Example roles: peddler, consumer, organizer_kingpin, supplier, manufacturer, processor, harbourer")
                hints.append(f"❌ WRONG: WHERE a.person_id IN (SELECT person_id FROM persons WHERE alias_name ILIKE '%{detected_role}%') - alias_name doesn't exist!")
                hints.append(f"❌ WRONG: WHERE a.person_id IN (SELECT person_id FROM accused WHERE type = '{detected_role}') - accused has NO type column!")
                hints.append(f"✅ CORRECT: WHERE bfa.accused_type ILIKE '%{detected_role}%' (after joining brief_facts_accused!)")
            else:
                # User wants GROUP BY to see all roles
                hints.append("Use GROUP BY bfa.accused_type to show all roles with counts")
                hints.append("Possible accused_type values: peddler, consumer, organizer_kingpin, supplier, manufacturer, processor, harbourer")
        
        # ⭐ NEW: Query Complexity Decision Tree Hints
        # Help LLM choose the right query strategy based on complexity
        if any(kw in message_lower for kw in ['similar', 'like this', 'pattern', 'lookalike', 'fuzzy']):
            hints.append("⭐ COMPLEXITY: Similarity/Fuzzy match query. Consider vector embeddings if available (brief_facts_embedding, name_embedding). Otherwise use ILIKE with wildcards.")
        
        if any(kw in message_lower for kw in ['unique', 'deduplicate', 'canonical', 'repeat offender', 'multiple crimes']):
            hints.append("⭐ COMPLEXITY: Unique person/deduplication query. Consider person_deduplication_tracker table if available. Otherwise use GROUP BY with HAVING COUNT > 1.")
        
        if any(kw in message_lower for kw in ['complete', 'all details', 'everything', 'full profile', 'comprehensive']):
            hints.append("⭐ COMPLEXITY: Multi-table comprehensive query. Join ALL related tables: crimes + accused + persons + properties + brief_facts_drugs + hierarchy + brief_facts_crime_summaries.")
        
        if any(kw in message_lower for kw in ['statistics', 'trend', 'analysis', 'compare', 'distribution']):
            hints.append("⭐ COMPLEXITY: Statistical/analytical query. Use aggregations (COUNT, SUM, AVG) with GROUP BY. Consider temporal grouping (DATE_TRUNC) for trends.")
        
        if query_plan.get('needs_aggregation'):
            hints.append("COUNT/SUM/AVG")
        if query_plan.get('limit'):
            hints.append(f"LIMIT {query_plan['limit']}")
        
        context = ". ".join(hints) if hints else ""
        
        system_prompt = f"""PostgreSQL SQL expert. Generate valid SELECT query using ONLY column names EXACTLY as shown in schema.

⚠️ CRITICAL - DO NOT INVENT ANYTHING:
1. Use ONLY column names from schema - NO EXCEPTIONS!
2. accused table has NO name columns! Only person_id. Must join persons for names.
3. Use lowercase with underscores: person_id (NOT personId), crime_id (NOT crimeId)
4. Table is 'accused' (NOT 'accuseds'), 'persons' (NOT 'users')
5. For names: persons.full_name or persons.name (NOT accused.name - doesn't exist!)

⚠️⚠️⚠️ ACTS/SECTIONS: USE acts_sections COLUMN ONLY! ⚠️⚠️⚠️
6. User mentions "8c", "27", "NDPSA", "IPC" → Search crimes.acts_sections ONLY!
7. WRONG: WHERE c.crime_type LIKE '%NDPSA%' ❌
8. WRONG: WHERE c.fir_num LIKE '%8c%' ❌
9. RIGHT: WHERE c.acts_sections ILIKE '%8c%' OR c.acts_sections ILIKE '%NDPSA%' ✅
10. acts_sections contains: "8C NDPSA", "20B NDPSA", "IPC 379", etc.

11. If table referenced in WHERE, it MUST be in FROM/JOIN
12. When user asks for "details", "show me", "all info" → SELECT MANY columns (Example 2!)
13. When asking about crime → join brief_facts_crime_summaries for summary_text
14. When user mentions "FIR" → search crimes.fir_num OR crimes.fir_reg_num (Example 7!)
15. "FIR" = First Information Report → stored in crimes table!
16. For name WITH ALIAS (e.g., "Rajendra Prasad @ Sachin") → use OR, not AND! (Example 3b!)
       12. "Name @ Alias" means person known by EITHER name → use OR between conditions!
       13. CRITICAL: When searching ANY entity → use ALL relevant columns with OR!
       14. Name search → Check ALL name columns (Example 3)
       15. Location search → Check ALL location columns (ps_name, dist_name, locality, etc.)
       16. Phone search → Check ALL phone columns (phone_number, phone_numbers, mobiles)
       17. Date search → Check ALL date columns (fir_date, date_created, etc.)
       18. PRINCIPLE: For comprehensive results, search ALL columns related to user's query!
       19. ⚠️⚠️⚠️ CRITICAL: "by X" = GROUP BY (NOT WHERE!) ⚠️⚠️⚠️
       20. User says "crimes by status" → SELECT case_status, COUNT(*) ... GROUP BY case_status
       21. User says "crimes by class" → SELECT class_classification, COUNT(*) ... GROUP BY class_classification
       22. User says "by classification" → GROUP BY c.class_classification (Example 12b!)
       23. "by X" queries = ALWAYS GROUP BY! NEVER FILTER with WHERE!
       24. WRONG: WHERE c.case_status = 'pending' (this is filtering, not grouping!)
       25. RIGHT: GROUP BY c.case_status (this shows ALL statuses with counts!)
       26. For STATISTICS (count, average, sum, total) → Use aggregation functions! (Example 13!)
       27. For TEMPORAL (by month, by year, trends) → Use DATE_TRUNC! (Example 14!)
       28. For REPEAT OFFENDERS → GROUP BY person, HAVING count > 1! (Example 15!)
       29. For SIMILARITY → Use vector operators (<->) for embeddings! (Example 16!)
       30. For RANGES (age 25-35, date ranges) → Use BETWEEN! (Examples 17, 18!)
       31. For MULTI-TABLE PROFILE → Join ALL related tables! (Example 20!)
       32. For COMPREHENSIVE DATA → Include counts, sums, averages in one query!
       33. Use STRING_AGG for comma-separated lists of values!

❌ DON'T USE THESE (THEY DON'T EXIST OR ARE WRONG COLUMNS):
- accused.name, accused.surname (NO name in accused!)
- personId, crimeId (use person_id, crime_id)
- accuseds (use accused)
- telephone_residence, mobile_number (use phone_numbers or phone_number)
- c.police_station, c.district (crimes table has NO these! Join hierarchy for ps_name, dist_name)
- c.crime_head, c.crime_group (use c.major_head, c.minor_head)
- s.ai_generated_summary (brief_facts_crime_summaries has ONLY summary_text and summary_json!)
- firs, advanced_search_firs, fir (TABLES DON'T EXIST! Use 'crimes' table for FIR/crime records!)
- f.crimeRegDate (use c.fir_date from crimes table!)
- p.alias_name (persons has 'alias' NOT 'alias_name'! Use p.alias or bfa.alias_name!)
- p.phone_numbers (persons has 'phone_number' singular! brief_facts_accused has 'phone_numbers' plural!)
- accused.type (accused table has NO 'type' column! For accused type → use brief_facts_accused.accused_type!)
- persons.alias_name (persons table has 'alias' NOT 'alias_name'! For accused type queries → use brief_facts_accused.accused_type, NOT persons.alias!)
- pr.property_type (properties table has NO property_type! Use pr.nature instead!)
- pr.property_description (properties table has NO property_description! Use pr.particular_of_property instead!)
- d.quantity (brief_facts_drugs has NO quantity! Use d.total_quantity or d.quantity_numeric instead!)
- d.total_quantity LIKE '%commercial%' (WRONG! For commercial quantity → use d.is_commercial = true, NOT text search!)
- bfa.type (brief_facts_accused has NO 'type' column! Use bfa.accused_type instead!)
- bfa.role_in_crime (for accused role/type queries - this is DIFFERENT! Use bfa.accused_type for role/type!)

❌ WRONG COLUMNS FOR ACTS/SECTIONS:
- c.crime_type LIKE '%NDPSA%' (crime_type is "Narcotics", NOT acts!)
- c.fir_num LIKE '%8c%' (fir_num is "243/2022", NOT section numbers!)
- ✅ RIGHT: c.acts_sections ILIKE '%8c%' OR c.acts_sections ILIKE '%NDPSA%'

❌ NEVER ADD SQL COMMENTS OR PLACEHOLDERS:
- WHERE c.ps_code IN ('PS1', 'PS2') -- Replace... ❌ WRONG! (validator rejects --)
- ✅ RIGHT: GROUP BY h.ps_name (show ALL police stations if user doesn't specify!)

✅✅✅ CRITICAL DATABASE RELATIONSHIP RULES (MUST FOLLOW!): ✅✅✅

1. CRIME-RELATED QUERIES → Use column: crime_id
   - For any crime information, use crimes.crime_id
   - Join with brief_facts_crime_summaries using crime_id for summary_text
   - Example: WHERE c.crime_id = '...' or JOIN crimes c ON ...

2. PERSON-RELATED QUERIES → Use column: person_id
   - For person information, use persons.person_id
   - persons table has: personal data (name, age, gender, phone, email, address, domicile_classification)
   - Example: WHERE p.person_id = '...' or JOIN persons p ON ...

3. ACCUSED-RELATED QUERIES → Use column: accused_id
   - For accused information, use accused.accused_id
   - accused table has: physical data (height, build, color, hair, eyes, face, nose, beard, mustache, mole, is_ccl)
   - persons table has: personal data (name, age, gender, phone, email, address)
   - ⚠️ IMPORTANT: If data not found in accused or persons tables, check brief_facts_accused table!
   - brief_facts_accused has: role_in_crime, accused_type, status, key_details, address, phone_numbers
   - Example: WHERE a.accused_id = '...' or JOIN accused a ON ...
   - Fallback: LEFT JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id

4. DRUG-RELATED QUERIES → Use column: crime_id (in BOTH tables!)
   - For drug information, MUST use crime_id in BOTH:
     * properties table (join by crime_id)
     * brief_facts_drugs table (join by crime_id)
   - ⚠️ CRITICAL: Always JOIN BOTH tables for complete drug information!
   - Example: 
     LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
     LEFT JOIN properties pr ON c.crime_id = pr.crime_id

5. HIERARCHY/POLICE STATION QUERIES → Use column: ps_code
   - For police station, district, hierarchy information, use crimes.ps_code
   - Join hierarchy table using ps_code
   - Example: LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
   - hierarchy table has: ps_name, dist_name, circle_name, zone_name

6. BRIEF FACTS/SUMMARY QUERIES → Use column: crime_id
   - For crime summary/details from brief_facts, use brief_facts_crime_summaries.summary_text
   - Join using crime_id
   - Example: LEFT JOIN brief_facts_crime_summaries s ON c.crime_id = s.crime_id
   - brief_facts_crime_summaries has: summary_text, summary_json (ONLY these two columns!)

✅ TABLE RELATIONSHIPS SUMMARY:
- Crime details: crimes table (primary) + brief_facts_crime_summaries (join by crime_id for summary_text)
- Person details: persons table (primary) via person_id
- Accused info: accused table (accused_id) + persons (person_id) + brief_facts_accused (fallback)
- Drugs: brief_facts_drugs (join by crime_id) + properties (join by crime_id) - BOTH required!
- Properties: properties table (join by crime_id) - has nature, category, particular_of_property, property_status, estimate_value
- Hierarchy: hierarchy table (join via ps_code from crimes) - has ps_name, dist_name, circle_name, zone_name

✅ CORRECT COLUMN NAMES:
- crimes table is for FIR/crime records! (NOT firs or advanced_search_firs!)
- crimes.fir_num (FIR number like "243/2022" - search with ILIKE!)
- crimes.fir_reg_num (FIR registration number - search with ILIKE!)
- crimes.fir_date (for date filtering - NOT crimeRegDate!)
- crimes.crime_id (primary key - use for exact match)
- persons.phone_number (singular!)
- brief_facts_accused.phone_numbers (plural!)
- crimes.ps_code (NOT police_station!)
- crimes.major_head (NOT crime_head!)
- crimes.minor_head (NOT crime_group!)
- crimes.acts_sections, crimes.io_name, crimes.io_rank
- hierarchy.ps_name (police station name - join via ps_code)
- hierarchy.dist_name (district name - join via ps_code)
- brief_facts_crime_summaries.summary_text (crime summary - ONLY summary_text and summary_json exist!)
- properties.nature (property type/nature - NOT property_type!)
- properties.category (property category)
- properties.particular_of_property (property description - NOT property_description!)
- properties.property_status (status: Seized, Recovered, etc.)
- properties.estimate_value (estimated value)
- brief_facts_drugs.total_quantity (drug quantity - NOT quantity!)
- brief_facts_drugs.quantity_numeric (numeric quantity)

✅ SEARCH ALL RELEVANT COLUMNS FOR EACH TYPE (CRITICAL!):

NAME SEARCH → Check ALL name columns:
- persons.full_name, persons.name, persons.surname, persons.alias
- brief_facts_accused.full_name, brief_facts_accused.alias_name

LOCATION SEARCH → Check ALL location columns:
- hierarchy.ps_name, hierarchy.dist_name, hierarchy.circle_name, hierarchy.zone_name
- persons.present_district, persons.permanent_district
- persons.present_locality_village, persons.permanent_locality_village
- persons.present_area_mandal, persons.permanent_area_mandal

PHONE SEARCH → Check ALL phone columns:
- persons.phone_number (singular!)
- brief_facts_accused.phone_numbers (plural!)

ADDRESS SEARCH → Check ALL address columns:
- persons.present_house_no, present_street_road_no, present_ward_colony
- persons.permanent_house_no, permanent_street_road_no, permanent_ward_colony
- brief_facts_accused.address

STATUS SEARCH → Check ALL status columns:
- crimes.case_status (Pending, Closed, etc.)
- properties.property_status (Seized, Recovered, etc.)

DATE SEARCH → Check ALL date columns:
- crimes.fir_date (primary date)
- crimes.date_created, crimes.date_modified
- properties.date_of_seizure

DRUG SEARCH → Check ALL drug columns:
- brief_facts_drugs.drug_name, scientific_name, brand_name
- brief_facts_drugs.drug_category, drug_schedule
- brief_facts_drugs.is_commercial (BOOLEAN - true for commercial quantity cases!)
- brief_facts_drugs.commercial_quantity (text field with commercial quantity value)
- brief_facts_drugs.total_quantity, quantity_numeric (actual quantity values)

CRITICAL PRINCIPLE: Whatever user searches → check ALL related columns with OR!

✅ COPY THESE EXAMPLES EXACTLY:

⚠️⚠️⚠️ EXAMPLE 0: ACTS/SECTIONS SEARCH (COPY THIS!) ⚠️⚠️⚠️

User asks: "Find crimes by specific acts/sections 8c NDPSA, r/w 27 NDPSA"
YOU MUST COPY THIS EXACTLY:

SELECT c.crime_id, c.fir_num, c.acts_sections, c.crime_type, c.case_status
FROM crimes c
WHERE c.acts_sections ILIKE '%8c%' 
   OR c.acts_sections ILIKE '%27%'
   OR c.acts_sections ILIKE '%NDPSA%'
ORDER BY c.fir_date DESC
LIMIT 100

⚠️⚠️⚠️ GROUP BY QUERIES (SIMPLE!) ⚠️⚠️⚠️

0a. "Get crimes by class_classification":
SELECT class_classification, COUNT(*) as total_crimes
FROM crimes
GROUP BY class_classification
ORDER BY total_crimes DESC

0b. "Get crimes by case status":
SELECT case_status, COUNT(*) as total_crimes
FROM crimes
GROUP BY case_status
ORDER BY total_crimes DESC

0c. "Get crimes registered at specific police stations" (no station name given):
SELECT h.ps_name, COUNT(*) as total_crimes
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
GROUP BY h.ps_name
ORDER BY total_crimes DESC

0c1. "Show all crimes from police station code '2025005'" (specific ps_code given):
SELECT c.crime_id, c.fir_date, c.io_name, c.ps_code, c.case_status, c.crime_type, c.fir_num
FROM crimes c
WHERE c.ps_code = '2025005'
ORDER BY c.fir_date DESC
LIMIT 100

⚠️ CRITICAL: Always use c.ps_code (with table alias!) in WHERE clause!
❌ WRONG: WHERE ps_code = '2025005' (missing table alias!)
✅ CORRECT: WHERE c.ps_code = '2025005'

0c2. "Find crimes handled by IO 'BARPATI RAMESH'" (IO name query):
SELECT c.crime_id, c.fir_date, c.io_name, c.ps_code, c.case_status, c.crime_type, c.fir_num, h.ps_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.io_name ILIKE '%BARPATI RAMESH%'
ORDER BY c.fir_date DESC
LIMIT 100

⚠️ CRITICAL: For IO name queries, use c.io_name (NOT io_name without alias!)
❌ WRONG: WHERE io_name = 'BARPATI RAMESH'
✅ CORRECT: WHERE c.io_name ILIKE '%BARPATI RAMESH%' (use ILIKE for partial match!)

0c3. "List crimes where IO rank is 'Inspector'" (IO rank query):
SELECT c.crime_id, c.fir_date, c.io_name, c.io_rank, c.ps_code, c.case_status, c.crime_type, c.fir_num, h.ps_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.io_rank ILIKE '%Inspector%'
ORDER BY c.fir_date DESC
LIMIT 100

⚠️ CRITICAL: For IO rank queries, use c.io_rank (NOT c.io_name!)
❌ WRONG: WHERE c.io_name = 'Inspector' (io_name is the person's name, NOT the rank!)
✅ CORRECT: WHERE c.io_rank ILIKE '%Inspector%' (io_rank is the rank field!)

0d. "Get crimes by property_status":
SELECT pr.property_status, COUNT(*) as total_crimes
FROM crimes c
JOIN properties pr ON c.crime_id = pr.crime_id
GROUP BY pr.property_status
ORDER BY total_crimes DESC
LIMIT 100

0e. "Find crimes by accused role peddler" (using accused_type from brief_facts_accused):
SELECT c.crime_id, c.fir_num, c.crime_type, c.case_status, h.ps_name, bfa.accused_type
FROM crimes c
JOIN accused a ON c.crime_id = a.crime_id
JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE bfa.accused_type ILIKE '%peddler%'
ORDER BY c.fir_date DESC
LIMIT 100

0e2. "Find crimes by accused role supplier" (using accused_type from brief_facts_accused):
SELECT c.crime_id, c.fir_num, c.crime_type, c.case_status, h.ps_name, bfa.accused_type
FROM crimes c
JOIN accused a ON c.crime_id = a.crime_id
JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE bfa.accused_type ILIKE '%supplier%'
ORDER BY c.fir_date DESC
LIMIT 100

0f. "Get crimes by accused role" (GROUP BY accused_type - show all roles with counts):
SELECT bfa.accused_type, COUNT(*) as total_crimes
FROM crimes c
JOIN accused a ON c.crime_id = a.crime_id
JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
GROUP BY bfa.accused_type
ORDER BY total_crimes DESC
LIMIT 100

0g. "Get accused type distributions" (count based on accused_type):
SELECT bfa.accused_type, COUNT(*) as count
FROM accused a
JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
GROUP BY bfa.accused_type
ORDER BY count DESC
LIMIT 100

0h. "Find crimes with specific accused involvement patterns, peddler" (filter by accused_type):
SELECT c.crime_id, c.fir_num, c.crime_type, c.case_status, c.fir_date, h.ps_name,
       bfa.accused_type, bfa.role_in_crime, bfa.status, bfa.key_details
FROM crimes c
JOIN accused a ON c.crime_id = a.crime_id
JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE bfa.accused_type ILIKE '%peddler%'
ORDER BY c.fir_date DESC
LIMIT 100

0i. "Find crimes with specific accused involvement patterns, peddler by month wise" (temporal analysis with accused_type filter):
SELECT DATE_TRUNC('month', c.fir_date) AS month,
       COUNT(DISTINCT c.crime_id) AS crime_count,
       COUNT(DISTINCT a.person_id) AS accused_count,
       bfa.accused_type
FROM crimes c
JOIN accused a ON c.crime_id = a.crime_id
JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
WHERE bfa.accused_type ILIKE '%peddler%'
GROUP BY DATE_TRUNC('month', c.fir_date), bfa.accused_type
ORDER BY month DESC
LIMIT 100

⚠️⚠️⚠️ CRITICAL FOR ACCUSED ROLE/TYPE QUERIES: ⚠️⚠️⚠️
- User says "accused role" or "accused type" → Use bfa.accused_type from brief_facts_accused table!
- ⚠️ CRITICAL: Column name is accused_type (NOT type, NOT role_in_crime!)
- brief_facts_accused.accused_type contains values like: peddler, consumer, organizer_kingpin, supplier, manufacturer, processor, harbourer
- brief_facts_accused.role_in_crime is a DIFFERENT field (role in the crime, NOT accused type)
- For "Find crimes by accused role X" (where X = peddler, supplier, etc.) → WHERE bfa.accused_type ILIKE '%X%'
- For "Get crimes by accused role" (no specific role) → GROUP BY bfa.accused_type
- ⚠️ CRITICAL JOIN ORDER: JOIN accused FIRST, THEN JOIN brief_facts_accused!
  ✅ CORRECT ORDER:
    1. FROM crimes c
    2. JOIN accused a ON c.crime_id = a.crime_id
    3. JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
  ❌ WRONG: Don't reference bfa before it's joined!
- ⚠️ CRITICAL: Use bfa.accused_type (NOT bfa.type - that column doesn't exist!)
- ⚠️ CRITICAL: When user mentions "peddler", "supplier", "consumer", etc. in context of "accused involvement patterns" or "accused type" → Filter by bfa.accused_type!
- ❌ WRONG: WHERE a.person_id IN (SELECT person_id FROM persons WHERE alias_name ILIKE '%peddler%') (alias_name doesn't exist! Use alias, not alias_name!)
- ❌ WRONG: WHERE a.person_id IN (SELECT person_id FROM accused WHERE type = 'peddler') (accused table has NO type column!)
- ✅ CORRECT: WHERE bfa.accused_type ILIKE '%peddler%' (after joining brief_facts_accused!)
- ⚠️ CRITICAL: "accused involvement patterns" means patterns in brief_facts_accused table (role_in_crime, accused_type, status, key_details)!

CRITICAL: If user doesn't specify WHICH police station, show ALL with counts!
NEVER use placeholder values like 'PS1', 'PS2'!
NEVER add SQL comments like "-- Replace..."!
CRITICAL: "by property_status" = GROUP BY pr.property_status (NOT WHERE filter!)
CRITICAL: "by accused role" = GROUP BY bfa.accused_type (NOT WHERE filter, unless user specifies a role like "supplier"!)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Get people in crime (names, phones, NO duplicates):
SELECT DISTINCT p.full_name, p.phone_number, bfa.age, bfa.address
FROM accused a
JOIN persons p ON a.person_id = p.person_id
LEFT JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id
WHERE a.crime_id = '62d566cec779cf34511b6c01'

2. Get COMPLETE crime details (ALL fields, summary, people):
SELECT c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status, 
       c.fir_date, c.ps_code, c.major_head, c.minor_head, c.io_name, c.acts_sections,
       c.brief_facts,
       h.ps_name, h.dist_name,
       s.summary_text,
       COUNT(DISTINCT a.person_id) as total_accused
FROM crimes c
LEFT JOIN brief_facts_crime_summaries s ON c.crime_id = s.crime_id
LEFT JOIN accused a ON c.crime_id = a.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.crime_id = '62ee3e7427b4412144d7cf48'
GROUP BY c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status,
         c.fir_date, c.ps_code, c.major_head, c.minor_head, c.io_name, c.acts_sections,
         c.brief_facts, h.ps_name, h.dist_name, s.summary_text
LIMIT 1

3. Search person by name (SEARCH ALL NAME COLUMNS WITH OR!):
SELECT DISTINCT p.full_name, p.name, p.surname, p.alias, bfa.alias_name, p.phone_number
FROM persons p
LEFT JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
WHERE p.full_name ILIKE '%devakumari%' 
   OR p.name ILIKE '%devakumari%'
   OR p.surname ILIKE '%devakumari%'
   OR p.alias ILIKE '%devakumari%'
   OR bfa.full_name ILIKE '%devakumari%'
   OR bfa.alias_name ILIKE '%devakumari%'
LIMIT 100

3b. Search person by name WITH ALIAS (use OR for ALL name columns!):
SELECT DISTINCT p.full_name, p.alias, bfa.alias_name, p.phone_number
FROM persons p
LEFT JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
WHERE p.full_name ILIKE '%rajendra prasad%' 
   OR p.name ILIKE '%rajendra%'
   OR p.surname ILIKE '%prasad%'
   OR p.alias ILIKE '%sachin%'
   OR bfa.full_name ILIKE '%rajendra prasad%'
   OR bfa.alias_name ILIKE '%sachin%'
LIMIT 100

4. "Find crimes by specific acts/sections 8c NDPSA, r/w 27 NDPSA":
SELECT c.crime_id, c.fir_num, c.acts_sections, c.crime_type, c.case_status, h.ps_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.acts_sections ILIKE '%8c%' 
   OR c.acts_sections ILIKE '%27%'
   OR c.acts_sections ILIKE '%NDPSA%'
ORDER BY c.fir_date DESC
LIMIT 100

5. Get crimes by person:
SELECT DISTINCT c.fir_num, c.crime_type, c.case_status, p.full_name
FROM crimes c
JOIN accused a ON c.crime_id = a.crime_id
JOIN persons p ON a.person_id = p.person_id
WHERE p.full_name ILIKE '%devakumari%'

6. Get drug/property details for crime (COMPREHENSIVE - BOTH tables!):
SELECT c.crime_id, c.fir_num, c.crime_type, c.case_status, c.fir_date,
       h.ps_name, h.dist_name,
       -- ALL drug columns from brief_facts_drugs
       d.drug_name, d.scientific_name, d.brand_name, d.drug_category, d.drug_schedule,
       d.total_quantity, d.quantity_unit, d.quantity_numeric, d.number_of_packets,
       d.weight_breakdown, d.packaging_details,
       d.source_location, d.destination, d.transport_method, d.supply_chain,
       d.seizure_location, d.seizure_time, d.seizure_method, d.seizure_officer,
       d.commercial_quantity, d.is_commercial, d.seizure_worth, d.street_value, d.purity,
       -- ALL property columns from properties (correct names!)
       pr.property_id, pr.nature as property_nature, pr.category as property_category,
       pr.particular_of_property, pr.property_status, pr.estimate_value, pr.recovered_value,
       pr.recovered_from, pr.place_of_recovery, pr.date_of_seizure, pr.belongs
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
LEFT JOIN properties pr ON c.crime_id = pr.crime_id
WHERE c.crime_id = '62ee3e7427b4412144d7cf48'

⚠️⚠️⚠️ CRITICAL FOR DRUG QUERIES: ⚠️⚠️⚠️
- When user asks about drugs, supply chain, seizures → ALWAYS JOIN BOTH brief_facts_drugs AND properties!
- brief_facts_drugs has: drug_name, supply_chain, source_location, destination, transport_method, etc.
- properties has: nature, category, particular_of_property, property_status, estimate_value, etc.
- Both tables link via crime_id - use LEFT JOIN for both!
- ⚠️ CRITICAL JOIN CONDITION: brief_facts_drugs links to crimes via crime_id, NOT drug_id!
- ✅ CORRECT: LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
- ❌ WRONG: LEFT JOIN brief_facts_drugs d ON c.crime_id = d.drug_id (drug_id does NOT exist!)
- User says "drug supply chain" → Include d.supply_chain, d.source_location, d.destination, d.transport_method
- User says "drug seizures" → Include BOTH drug details (d.*) AND property details (pr.*)
- NEVER use wrong column names: pr.property_type (use pr.nature!), pr.property_description (use pr.particular_of_property!)

6c. Get crimes with property seizures (drugs and properties):
SELECT DISTINCT c.crime_id, c.fir_num, c.crime_type, c.case_status, c.fir_date,
       h.ps_name, h.dist_name,
       pr.property_id, pr.property_status, pr.nature, pr.category, 
       pr.particular_of_property, pr.estimate_value, pr.date_of_seizure,
       d.drug_name, d.drug_category, d.total_quantity, d.street_value, 
       d.seizure_worth, d.is_commercial
FROM crimes c
JOIN properties pr ON c.crime_id = pr.crime_id
LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
ORDER BY c.fir_date DESC
LIMIT 100

CRITICAL: For property seizures:
- Use JOIN properties (not LEFT JOIN) to show ONLY crimes WITH properties
- Include brief_facts_drugs for drug-related property seizures
- Show property details: property_status, nature, category, particular_of_property, estimate_value
- Show drug details: drug_name, drug_category, total_quantity, street_value, seizure_worth
- Link via crime_id in both tables
- If user says "Seized", "Recovered" → Use ILIKE for case-insensitive: WHERE pr.property_status ILIKE '%seized%'

6d. Get crimes with property status "Seized" (filter by property_status):
SELECT DISTINCT c.crime_id, c.fir_num, c.crime_type, c.case_status, c.fir_date,
       h.ps_name, h.dist_name,
       pr.property_id, pr.property_status, pr.nature, pr.category, 
       pr.particular_of_property, pr.estimate_value, pr.date_of_seizure,
       d.drug_name, d.drug_category, d.total_quantity, d.street_value, 
       d.seizure_worth, d.is_commercial
FROM crimes c
JOIN properties pr ON c.crime_id = pr.crime_id
LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE pr.property_status ILIKE '%seized%'
ORDER BY c.fir_date DESC
LIMIT 100

6. Get FIR records from last month:
SELECT c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status, c.fir_date,
       h.ps_name, h.dist_name, c.io_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.fir_date >= CURRENT_DATE - INTERVAL '1 month'
ORDER BY c.fir_date DESC
LIMIT 50

7. Search by FIR number (Check BOTH fir_num and fir_reg_num!):
SELECT c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status, c.fir_date,
       h.ps_name, h.dist_name, s.summary_text
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
LEFT JOIN brief_facts_crime_summaries s ON c.crime_id = s.crime_id
WHERE c.fir_num ILIKE '%243/2022%' OR c.fir_reg_num ILIKE '%243/2022%'

8. Search by LOCATION (Check ALL location columns!):
SELECT DISTINCT c.crime_id, c.fir_num, h.ps_name, h.dist_name, h.circle_name, c.crime_type
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE h.ps_name ILIKE '%sangareddy%'
   OR h.dist_name ILIKE '%sangareddy%'
   OR h.circle_name ILIKE '%sangareddy%'
   OR h.zone_name ILIKE '%sangareddy%'
LIMIT 100

9. Search by PHONE (Check ALL phone columns!):
SELECT DISTINCT p.full_name, p.phone_number, bfa.phone_numbers, p.email_id
FROM persons p
LEFT JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
WHERE p.phone_number ILIKE '%9876543210%'
   OR bfa.phone_numbers ILIKE '%9876543210%'
LIMIT 100

10. Search by STATUS (Check ALL status columns!):
SELECT c.crime_id, c.fir_num, c.case_status, pr.property_status, h.ps_name
FROM crimes c
LEFT JOIN properties pr ON c.crime_id = pr.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.case_status IN ('UI', 'PT') OR c.case_status IS NULL
   OR pr.property_status ILIKE '%seized%'
LIMIT 100

⚠️ CRITICAL: "pending cases" means case_status IN ('UI', 'PT') or IS NULL, NOT ILIKE '%pending%'!
❌ WRONG: WHERE c.case_status ILIKE '%pending%' (won't match 'UI' or 'PT'!)
✅ CORRECT: WHERE c.case_status IN ('UI', 'PT') OR c.case_status IS NULL

11. Search DRUGS (Check ALL drug-related columns!):
SELECT DISTINCT c.crime_id, c.fir_num, d.drug_name, d.scientific_name, d.brand_name, 
                d.drug_category, d.drug_schedule, d.seizure_worth
FROM crimes c
JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
WHERE d.drug_name ILIKE '%ganja%'
   OR d.scientific_name ILIKE '%ganja%'
   OR d.brand_name ILIKE '%ganja%'
   OR d.drug_category ILIKE '%ganja%'
LIMIT 100

11b. Get COMMERCIAL QUANTITY drug cases (CRITICAL - use is_commercial boolean!):
SELECT DISTINCT c.crime_id, c.fir_num, c.crime_type, c.case_status, c.fir_date,
       h.ps_name, h.dist_name,
       d.drug_name, d.drug_category, d.total_quantity, d.quantity_numeric,
       d.commercial_quantity, d.is_commercial, d.seizure_worth, d.street_value
FROM crimes c
JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE d.is_commercial = true
ORDER BY c.fir_date DESC
LIMIT 100

⚠️⚠️⚠️ CRITICAL FOR COMMERCIAL QUANTITY QUERIES: ⚠️⚠️⚠️
- User says "commercial quantity", "commercial drug cases" → Use d.is_commercial = true
- User says "non-commercial", "small quantity" → Use d.is_commercial = false
- NEVER search for "commercial" text in d.total_quantity (that's wrong!)
- d.is_commercial is a BOOLEAN field (true/false), NOT text!
- d.commercial_quantity is a text field with the actual commercial quantity value
- For commercial quantity cases → WHERE d.is_commercial = true ✅
- WRONG: WHERE d.total_quantity LIKE '%commercial%' ❌

12. Group/Count BY a field (when user says "by Status", "by District", etc.):
SELECT c.case_status, COUNT(*) as total_crimes
FROM crimes c
GROUP BY c.case_status
ORDER BY total_crimes DESC
LIMIT 100

12b. Group BY class_classification (when user says "by class_classification"):
SELECT c.class_classification, COUNT(*) as total_crimes
FROM crimes c
GROUP BY c.class_classification
ORDER BY total_crimes DESC
LIMIT 100

13. STATISTICS/AGGREGATION (total count, average, sum):
SELECT COUNT(*) as total_crimes,
       COUNT(DISTINCT c.ps_code) as police_stations,
       COUNT(DISTINCT a.person_id) as total_accused,
       AVG(EXTRACT(YEAR FROM AGE(c.fir_date))) as avg_case_age_years
FROM crimes c
LEFT JOIN accused a ON c.crime_id = a.crime_id

14. TEMPORAL ANALYSIS (crimes by month/year):
SELECT DATE_TRUNC('month', c.fir_date) as month,
       COUNT(*) as crime_count,
       COUNT(DISTINCT c.ps_code) as ps_affected
FROM crimes c
WHERE c.fir_date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY DATE_TRUNC('month', c.fir_date)
ORDER BY month DESC

14a. TEMPORAL ANALYSIS (crimes by year):
SELECT EXTRACT(YEAR FROM c.fir_date) as year,
       COUNT(*) as crime_count,
       COUNT(DISTINCT h.dist_code) as districts_affected,
       COUNT(CASE WHEN c.case_status = 'Pending' THEN 1 END) as pending_cases
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
GROUP BY EXTRACT(YEAR FROM c.fir_date)
ORDER BY year DESC

14b. TEMPORAL ANALYSIS (seasonal patterns by quarter):
SELECT EXTRACT(QUARTER FROM c.fir_date) as quarter,
       EXTRACT(MONTH FROM c.fir_date) as month,
       COUNT(*) as crime_count
FROM crimes c
WHERE c.fir_date >= CURRENT_DATE - INTERVAL '2 years'
GROUP BY EXTRACT(QUARTER FROM c.fir_date), EXTRACT(MONTH FROM c.fir_date)
ORDER BY quarter, month

14c. TEMPORAL ANALYSIS (recent crimes - last N days):
SELECT c.fir_num, c.fir_date, c.case_status, h.ps_name, h.dist_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.fir_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY c.fir_date DESC
LIMIT 100

14d. TEMPORAL ANALYSIS (date range with BETWEEN):
SELECT c.fir_num, c.fir_date, c.case_status, h.ps_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.fir_date BETWEEN '2023-01-01'::date AND '2023-12-31'::date
ORDER BY c.fir_date DESC

15. REPEAT OFFENDERS (persons with multiple crimes):
SELECT p.full_name, p.phone_number, COUNT(DISTINCT a.crime_id) as crime_count,
       STRING_AGG(DISTINCT c.major_head, ', ') as crime_types
FROM persons p
JOIN accused a ON p.person_id = a.person_id
JOIN crimes c ON a.crime_id = c.crime_id
GROUP BY p.person_id, p.full_name, p.phone_number
HAVING COUNT(DISTINCT a.crime_id) > 1
ORDER BY crime_count DESC
LIMIT 100

15a. REPEAT OFFENDERS (using person_deduplication_tracker - if available):
-- ⚠️ OPTIONAL: Only use if person_deduplication_tracker table exists in your database!
-- This table provides unique person records with deduplication info
SELECT pdt.canonical_person_id, pdt.full_name, pdt.crime_count,
       pdt.person_record_count, pdt.matching_tier, pdt.confidence_score
FROM person_deduplication_tracker pdt
WHERE pdt.crime_count > 1
ORDER BY pdt.crime_count DESC, pdt.confidence_score DESC
LIMIT 100

16. VECTOR SIMILARITY SEARCH (find similar crimes):
-- ⚠️ OPTIONAL: Only use if brief_facts_embedding column exists in crimes table!
-- Vector embeddings enable semantic similarity search for fuzzy matching
SELECT c2.crime_id, c2.fir_num, c2.crime_type,
       (c1.brief_facts_embedding <-> c2.brief_facts_embedding) as similarity_distance
FROM crimes c1
JOIN crimes c2 ON c1.crime_id != c2.crime_id
WHERE c1.crime_id = '62ee3e7427b4412144d7cf48'
  AND c1.brief_facts_embedding IS NOT NULL
  AND c2.brief_facts_embedding IS NOT NULL
ORDER BY c1.brief_facts_embedding <-> c2.brief_facts_embedding
LIMIT 10

16a. VECTOR SIMILARITY SEARCH (find similar persons by name - if name_embedding exists):
-- ⚠️ OPTIONAL: Only use if name_embedding column exists in persons table!
SELECT p2.person_id, p2.full_name, p2.phone_number,
       (p1.name_embedding <-> p2.name_embedding) as name_similarity
FROM persons p1
JOIN persons p2 ON p1.person_id != p2.person_id
WHERE p1.person_id = 'REPLACE_WITH_ACTUAL_PERSON_ID'
  AND p1.name_embedding IS NOT NULL
  AND p2.name_embedding IS NOT NULL
ORDER BY p1.name_embedding <-> p2.name_embedding
LIMIT 20

17. AGE RANGE FILTER (accused between ages):
SELECT DISTINCT p.full_name, p.age, p.occupation, bfa.role_in_crime
FROM persons p
JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
WHERE p.age BETWEEN 25 AND 35
LIMIT 100

18. DATE RANGE SEARCH (crimes in period):
SELECT c.crime_id, c.fir_num, c.crime_type, c.fir_date, h.ps_name
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.fir_date BETWEEN '2023-01-01' AND '2023-12-31'
ORDER BY c.fir_date DESC
LIMIT 100

19. DRUG VALUE ANALYSIS (total seizure worth by category):
SELECT d.drug_category, 
       COUNT(*) as total_cases,
       SUM(d.seizure_worth) as total_value,
       AVG(d.seizure_worth) as avg_value_per_case
FROM brief_facts_drugs d
GROUP BY d.drug_category
ORDER BY total_value DESC
LIMIT 50

20. MULTI-TABLE CRIME PROFILE (complete crime with all related data):
SELECT c.crime_id, c.fir_num, c.crime_type, c.case_status,
       h.ps_name, h.dist_name,
       s.summary_text,
       COUNT(DISTINCT a.person_id) as accused_count,
       COUNT(DISTINCT d.drug_name) as drug_types,
       COUNT(DISTINCT pr.property_id) as properties_seized,
       SUM(d.seizure_worth) as total_drug_value
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
LEFT JOIN brief_facts_crime_summaries s ON c.crime_id = s.crime_id
LEFT JOIN accused a ON c.crime_id = a.crime_id
LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
LEFT JOIN properties pr ON c.crime_id = pr.crime_id
WHERE c.crime_id = '62ee3e7427b4412144d7cf48'
GROUP BY c.crime_id, c.fir_num, c.crime_type, c.case_status,
         h.ps_name, h.dist_name, s.summary_text
LIMIT 1

⚠️ LIMIT BASED ON USER REQUEST:
- Default: LIMIT 100 (for safety)
- If user says "all", "complete", "everything": LIMIT 500
- If user says "top 5", "first 10", "show 20": use that number
- If user says "recent": LIMIT 50

{context}

Return ONLY the SQL query on one line. COPY the examples above. NO inventions."""
        
        prompt = f"""{schema}

User question: {user_message}

SQL query (use ONLY column names from schema above):"""
        
        return self.generate(prompt, system_prompt)
    
    def generate_sql(self, user_message: str, schema: str) -> Optional[str]:
        """Generate SQL query (backwards compatibility)"""
        return self.generate_sql_with_context(user_message, schema, {})
    
    # ========================================================================
    # MongoDB Query Generation (DOPAMS AI Optimized)
    # ========================================================================
    
    def generate_mongodb_query(self, user_message: str, schema: str) -> Optional[str]:
        """Generate MongoDB query from natural language"""
        system_prompt = """MongoDB expert. Generate query using UPPERCASE field names from schema.

CRITICAL RULES:
1. Use ONLY UPPERCASE field names (e.g., ACCUSED_NAME, MOBILE_1, FIR_NO)
2. Collection is ALWAYS "fir_records"
3. MUST include "collection" field in JSON

EXAMPLES:
Search by name:
{"collection": "fir_records", "query": {"ACCUSED_NAME": {"$regex": "rajesh", "$options": "i"}}}

Count all:
{"collection": "fir_records", "pipeline": [{"$count": "total"}]}

Search by FIR registration number:
{"collection": "fir_records", "query": {"FIR_REG_NUM": "2029019150001"}}

Search by FIR number:
{"collection": "fir_records", "query": {"FIR_NO": "1/2015"}}

Group/Count by field (when user says "by Status", "by District"):
{"collection": "fir_records", "pipeline": [{"$group": {"_id": "$FIR_STATUS", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]}

Age range filter:
{"collection": "fir_records", "query": {"AGE": {"$gte": 25, "$lte": 35}}}

Date range search (CRITICAL - $gte and $lte must be INSIDE field query, NOT at top level!):
{"collection": "fir_records", "query": {"REG_DT": {"$gte": {"$date": "2023-01-01T00:00:00Z"}, "$lte": {"$date": "2023-12-31T23:59:59Z"}}}}

Date search with "onwards" or "from date":
{"collection": "fir_records", "query": {"REG_DT": {"$gte": {"$date": "2025-05-11T00:00:00Z"}}}}

Date search between two dates:
{"collection": "fir_records", "query": {"REG_DT": {"$gte": {"$date": "2025-01-01T00:00:00Z"}, "$lte": {"$date": "2025-12-31T23:59:59Z"}}}}

Date search using FROM_DT and TO_DT fields:
{"collection": "fir_records", "query": {"FROM_DT": {"$gte": {"$date": "2025-01-01T00:00:00Z"}}, "TO_DT": {"$lte": {"$date": "2025-12-31T23:59:59Z"}}}}

⚠️ CRITICAL MONGODB DATE QUERIES:
❌ WRONG: {"collection": "fir_records", "query": {"$gte": {"$date": "2023-01-01T00:00:00Z"}, "$lte": {"$date": "2023-12-31T23:59:59Z"}}} (operators at top level!)
✅ CORRECT: {"collection": "fir_records", "query": {"REG_DT": {"$gte": {"$date": "2023-01-01T00:00:00Z"}, "$lte": {"$date": "2023-12-31T23:59:59Z"}}}} (operators inside field query!)

⚠️ CRITICAL DATE FIELD NAMES (ALL UPPERCASE):
- REG_DT (registration date)
- FROM_DT (from date)
- TO_DT (to date)
- PS_RECV_INFORM_DT (police station received information date)

⚠️⚠️⚠️ DYNAMIC DATE QUERIES ⚠️⚠️⚠️:
- "today" or "registered today" → Use current date: {"collection": "fir_records", "query": {"REG_DT": {"$gte": {"$date": "2025-11-16T00:00:00Z"}, "$lte": {"$date": "2025-11-16T23:59:59Z"}}}}
- "yesterday" → Use previous day date
- "this week" → Use date range for current week
- "last 7 days" → Use date range from 7 days ago to today
- "registration delays" → Calculate delay between FROM_DT and REG_DT using pipeline: {"collection": "fir_records", "pipeline": [{"$project": {"delay_days": {"$subtract": ["$REG_DT", "$FROM_DT"]}, "REG_DT": 1, "FROM_DT": 1, "FIR_REG_NUM": 1}}, {"$match": {"delay_days": {"$gt": 0}}}, {"$sort": {"delay_days": -1}}]}
- "records registered on REG_DT" → Show all records with REG_DT field: {"collection": "fir_records", "query": {"REG_DT": {"$exists": true, "$ne": null}}}

Statistics (count, sum, average):
{"collection": "fir_records", "pipeline": [{"$group": {"_id": null, "total": {"$sum": 1}, "avg_age": {"$avg": "$AGE"}}}]}

Repeat offenders (group by name, count crimes):
{"collection": "fir_records", "pipeline": [{"$group": {"_id": "$ACCUSED_NAME", "crime_count": {"$sum": 1}}}, {"$match": {"crime_count": {"$gt": 1}}}, {"$sort": {"crime_count": -1}}]}

District-wise breakdown:
{"collection": "fir_records", "pipeline": [{"$group": {"_id": "$DISTRICT", "total_crimes": {"$sum": 1}, "avg_age": {"$avg": "$AGE"}}}, {"$sort": {"total_crimes": -1}}]}

Month-wise breakdown (temporal analysis):
{"collection": "fir_records", "pipeline": [{"$group": {"_id": {"year": {"$year": "$REG_DT"}, "month": {"$month": "$REG_DT"}}, "count": {"$sum": 1}}}, {"$sort": {"_id.year": -1, "_id.month": -1}}]}

⚠️⚠️⚠️ CRITICAL MONGODB OPERATORS: ⚠️⚠️⚠️
- Use $year, $month, $dayOfMonth for date extraction in aggregation pipeline (NOT $substr!)
- ❌ WRONG: {"month": {"$substr": ["$REG_DT", 5, 2]}} - $substr is NOT a query operator!
- ✅ CORRECT: {"$group": {"_id": {"month": {"$month": "$REG_DT"}}, "count": {"$sum": 1}}}
- For date operations in pipeline → Use $year, $month, $dayOfMonth, $dayOfWeek, $dayOfYear
- For date filtering in query → Use $gte, $lte with $date objects

CRITICAL: 
- Use "query" for searches/filters
- Use "pipeline" for aggregations/grouping/statistics
- "by X" means GROUP BY X!
- For counts/sums/averages → use pipeline with $group
- For date ranges → use $gte and $lte
- For repeat offenders → group + match + sort
- ⚠️ MongoDB (fir_records) does NOT have property_status field! Only PostgreSQL has properties table!
- If user asks "by property_status" → MongoDB query should return empty or skip (property_status is PostgreSQL only!)

⚠️⚠️⚠️ FIELD NAME CONVERSION ⚠️⚠️⚠️:
- If you accidentally use lowercase field names, they will be auto-converted to UPPERCASE
- But ALWAYS use UPPERCASE in your JSON to avoid confusion!
- Example: Use "FIR_REG_NUM" NOT "fir_reg_num"
- Example: Use "REG_DT" NOT "reg_dt"
- Example: Use "ACCUSED_NAME" NOT "accused_name"

Return ONLY valid JSON on one line. NO explanations."""
        
        # ⚠️ CRITICAL: Add dynamic date hints for "today", "yesterday", etc.
        message_lower = user_message.lower()
        date_hints = []
        
        if 'today' in message_lower or 'registered today' in message_lower:
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            date_hints.append(f"⚠️⚠️⚠️ CRITICAL: User asked for 'today' - use current date: {today}")
            date_hints.append(f"Example: {{\"collection\": \"fir_records\", \"query\": {{\"REG_DT\": {{\"$gte\": {{\"$date\": \"{today}T00:00:00Z\"}}, \"$lte\": {{\"$date\": \"{today}T23:59:59Z\"}}}}}}}}")
        
        if 'registration delay' in message_lower or 'registration delays' in message_lower:
            date_hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'registration delays' - calculate delay between FROM_DT and REG_DT!")
            date_hints.append("Use pipeline to calculate: delay_ms = REG_DT - FROM_DT (result in milliseconds)")
            date_hints.append("Convert to days: delay_days = delay_ms / (1000 * 60 * 60 * 24)")
            date_hints.append("Example: {\"collection\": \"fir_records\", \"pipeline\": [{\"$project\": {\"delay_ms\": {\"$subtract\": [\"$REG_DT\", \"$FROM_DT\"]}, \"delay_days\": {\"$divide\": [{\"$subtract\": [\"$REG_DT\", \"$FROM_DT\"]}, 86400000]}, \"REG_DT\": 1, \"FROM_DT\": 1, \"FIR_REG_NUM\": 1}}, {\"$match\": {\"delay_days\": {\"$gt\": 0}}}, {\"$sort\": {\"delay_days\": -1}}]}")
        
        if 'registered on reg_dt' in message_lower or 'records registered on reg_dt' in message_lower:
            date_hints.append("⚠️⚠️⚠️ CRITICAL: User asked for 'records registered on REG_DT' - show all records with REG_DT field!")
            date_hints.append("Example: {\"collection\": \"fir_records\", \"query\": {\"REG_DT\": {\"$exists\": true, \"$ne\": null}}}")
        
        # ⚠️ CRITICAL: MongoDB field queries (INT_FATHER_NAME, FACE_TYPE, etc.) - show all records
        mongodb_field_keywords = ['int_father_name', 'int_father_mobile_no', 'int_mother_name', 'int_brother_name', 
                                  'face_type', 'hair_style', 'hair_color', 'ps_recv_inform_dt', 'from_dt', 'to_dt']
        if any(kw in message_lower for kw in mongodb_field_keywords):
            date_hints.append("⚠️⚠️⚠️ CRITICAL: User asked for MongoDB field - most records don't have this field!")
            date_hints.append("⚠️ CRITICAL: DO NOT filter by field != null - show ALL records and indicate which have the field!")
            date_hints.append("✅ CORRECT: Show all records, indicate which have the field (even if empty)")
            date_hints.append("Example: {\"collection\": \"fir_records\", \"query\": {}} (show all, then filter in projection)")
        
        # Add date hints to system prompt if needed
        if date_hints:
            system_prompt += "\n\n" + "\n".join(date_hints)
        
        prompt = f"""{schema}

User question: {user_message}

JSON (use ONLY UPPERCASE field names from schema above):"""
        
        result = self.generate(prompt, system_prompt)
        
        # Extract and clean JSON
        if result:
            result = self._extract_and_fix_mongodb_json(result)
        
        return result
    
    def _convert_mongodb_field_names_to_uppercase(self, obj: Any) -> Any:
        """
        Recursively convert MongoDB field names to UPPERCASE
        MongoDB uses UPPERCASE field names (FIR_REG_NUM, REG_DT, etc.)
        """
        # Field name mapping: lowercase -> UPPERCASE
        FIELD_NAME_MAP = {
            # Common MongoDB fields
            'fir_reg_num': 'FIR_REG_NUM',
            'fir_no': 'FIR_NO',
            'fir_num': 'FIR_NO',
            'reg_dt': 'REG_DT',
            'from_dt': 'FROM_DT',
            'to_dt': 'TO_DT',
            'ps_recv_inform_dt': 'PS_RECV_INFORM_DT',
            'accused_name': 'ACCUSED_NAME',
            'father_name': 'FATHER_NAME',
            'int_father_name': 'INT_FATHER_NAME',
            'int_father_mobile_no': 'INT_FATHER_MOBILE_NO',
            'int_mother_name': 'INT_MOTHER_NAME',
            'int_mother_mobile_no': 'INT_MOTHER_MOBILE_NO',
            'int_wife_name': 'INT_WIFE_NAME',
            'int_wife_mobile_no': 'INT_WIFE_MOBILE_NO',
            'int_brother_name': 'INT_BROTHER_NAME',
            'int_brother_mobile_no': 'INT_BROTHER_MOBILE_NO',
            'mobile_1': 'MOBILE_1',
            'telephone_residence': 'TELEPHONE_RESIDENCE',
            'district': 'DISTRICT',
            'ps': 'PS',
            'ps_name': 'PS',
            'fir_status': 'FIR_STATUS',
            'case_status': 'FIR_STATUS',
            'age': 'AGE',
            'face_type': 'FACE_TYPE',
            'email': 'EMAIL',
            'accused_occupation': 'ACCUSED_OCCUPATION',
            'act_sec': 'ACT_SEC',
            'acts_sections': 'ACT_SEC',
        }
        
        if isinstance(obj, dict):
            new_dict = {}
            for key, value in obj.items():
                # Skip MongoDB operators ($gte, $lte, $regex, etc.)
                if key.startswith('$'):
                    new_dict[key] = self._convert_mongodb_field_names_to_uppercase(value)
                elif key in ['collection', 'query', 'pipeline']:
                    # These are structure keys, keep as-is
                    new_dict[key] = self._convert_mongodb_field_names_to_uppercase(value)
                else:
                    # Convert field name to UPPERCASE
                    # First check if it's in our mapping
                    if key.lower() in FIELD_NAME_MAP:
                        new_key = FIELD_NAME_MAP[key.lower()]
                    elif key.isupper():
                        # Already uppercase, keep it
                        new_key = key
                    else:
                        # Convert to uppercase (snake_case -> SNAKE_CASE)
                        new_key = key.upper()
                    
                    new_dict[new_key] = self._convert_mongodb_field_names_to_uppercase(value)
            return new_dict
        elif isinstance(obj, list):
            return [self._convert_mongodb_field_names_to_uppercase(item) for item in obj]
        else:
            return obj
    
    def _extract_and_fix_mongodb_json(self, text: str) -> Optional[str]:
        """Extract MongoDB JSON and ensure it has collection and query fields"""
        # Remove escape characters first
        text = text.replace('\\_', '_').replace('\\*', '*')
        
        # Extract JSON
        json_result = self._extract_json(text)
        
        if not json_result:
            return None
        
        # Check and fix structure
        try:
            parsed = json.loads(json_result)
            
            # ⭐ CRITICAL FIX: Convert all field names to UPPERCASE for MongoDB
            parsed = self._convert_mongodb_field_names_to_uppercase(parsed)
            
            # If it doesn't have collection, auto-add it
            if 'collection' not in parsed:
                logger.warning(f"MongoDB query missing collection field, auto-adding 'fir_records'")
                parsed['collection'] = 'fir_records'
            
            # CRITICAL FIX: If query fields are at top level, wrap them in "query"
            if 'collection' in parsed and 'query' not in parsed and 'pipeline' not in parsed:
                # Extract query fields (everything except 'collection')
                query_fields = {k: v for k, v in parsed.items() if k != 'collection'}
                if query_fields:
                    logger.warning(f"MongoDB query fields at top level, wrapping in 'query' key")
                    parsed = {
                        'collection': parsed['collection'],
                        'query': query_fields
                    }
            
            # CRITICAL FIX: If $gte/$lte are at top level of query (WRONG!), move them inside a field
            if 'query' in parsed and isinstance(parsed['query'], dict):
                query_dict = parsed['query']
                # Check if $gte or $lte are at top level (this is wrong!)
                if '$gte' in query_dict or '$lte' in query_dict:
                    logger.warning(f"MongoDB query has $gte/$lte at top level - this is invalid! Moving to REG_DT field.")
                    # Try to fix by moving to REG_DT field (most common date field)
                    fixed_query = {}
                    if '$gte' in query_dict:
                        if 'REG_DT' not in fixed_query:
                            fixed_query['REG_DT'] = {}
                        fixed_query['REG_DT']['$gte'] = query_dict.pop('$gte')
                    if '$lte' in query_dict:
                        if 'REG_DT' not in fixed_query:
                            fixed_query['REG_DT'] = {}
                        fixed_query['REG_DT']['$lte'] = query_dict.pop('$lte')
                    # Keep other fields
                    fixed_query.update(query_dict)
                    parsed['query'] = fixed_query
            
            # CRITICAL FIX: If query contains aggregation operators ($sum, $avg, etc.), convert to pipeline
            if 'query' in parsed and isinstance(parsed['query'], dict):
                query_dict = parsed['query']
                # Check if query contains aggregation operators
                has_aggregation = False
                def check_for_aggregation(obj):
                    if isinstance(obj, dict):
                        for key, value in obj.items():
                            if key.startswith('$') and key in ['$sum', '$avg', '$count', '$max', '$min', '$group']:
                                return True
                            if isinstance(value, (dict, list)):
                                if check_for_aggregation(value):
                                    return True
                    elif isinstance(obj, list):
                        for item in obj:
                            if check_for_aggregation(item):
                                return True
                    return False
                
                if check_for_aggregation(query_dict):
                    logger.warning(f"MongoDB query contains aggregation operators, converting to pipeline")
                    # Extract _id field if present (for grouping)
                    group_id = query_dict.get('_id', None)
                    if group_id:
                        # Convert to pipeline
                        pipeline = [{"$group": {"_id": group_id, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
                        parsed = {
                            'collection': parsed['collection'],
                            'pipeline': pipeline
                        }
                    else:
                        # Just remove the invalid query
                        parsed = {
                            'collection': parsed['collection'],
                            'query': {}
                        }
            
            return json.dumps(parsed)
        except Exception as e:
            logger.error(f"MongoDB JSON fix error: {e}")
            return json_result
    
    def _extract_json(self, text: str) -> Optional[str]:
        """Extract JSON from text that may contain explanations"""
        # Remove markdown code blocks
        text = re.sub(r'```(?:json)?\s*', '', text)
        text = re.sub(r'```\s*$', '', text)
        
        # Try to find JSON object
        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        matches = re.findall(json_pattern, text, re.DOTALL)
        
        # Return first valid JSON
        for match in matches:
            try:
                json.loads(match)
                return match.strip()
            except json.JSONDecodeError:
                continue
        
        # If no match, try the whole text
        try:
            json.loads(text.strip())
            return text.strip()
        except:
            pass
        
        logger.warning("Could not extract valid JSON")
        return None
    
    # ========================================================================
    # Intent Detection
    # ========================================================================
    
    def detect_intent(self, user_message: str) -> str:
        """Detect user intent from message"""
        message_lower = user_message.lower()
        
        aggregation_kw = ['count', 'sum', 'average', 'total', 'group', 'aggregate', 'max', 'min', 'how many']
        query_kw = ['show', 'get', 'find', 'list', 'select', 'fetch', 'retrieve', 'display']
        
        if any(kw in message_lower for kw in aggregation_kw):
            return 'aggregation'
        if any(kw in message_lower for kw in query_kw):
            return 'query'
        if '?' in user_message or any(w in message_lower for w in ['what', 'how', 'which', 'when']):
            return 'query'
        
        return 'general'

# ============================================================================
# Factory Functions (Easy Client Creation)
# ============================================================================

def create_client(provider: str = 'ollama', **kwargs) -> UniversalLLMClient:
    """
    Factory function to create LLM client
    
    Args:
        provider: 'openai', 'ollama', or 'anthropic'
        **kwargs: Additional config parameters
    
    Returns:
        UniversalLLMClient instance
    """
    config = LLMConfig(provider=provider, **kwargs)
    return UniversalLLMClient(config)

def create_openai_client(api_key: str, model: str = 'gpt-4', **kwargs) -> UniversalLLMClient:
    """Create OpenAI client with shorthand"""
    return create_client('openai', api_key=api_key, model=model, **kwargs)

def create_ollama_client(model: str = None, **kwargs) -> UniversalLLMClient:
    """Create Ollama client with shorthand"""
    import os
    model = model or os.getenv('LLM_MODEL_SQL')
    return create_client('ollama', model=model, **kwargs)

def create_anthropic_client(api_key: str, model: str = 'claude-3-5-sonnet-20241022', **kwargs) -> UniversalLLMClient:
    """Create Anthropic client with shorthand"""
    return create_client('anthropic', api_key=api_key, model=model, **kwargs)

# ============================================================================
# Backwards Compatibility (for existing code)
# ============================================================================

class LocalLLMClient(UniversalLLMClient):
    """Backwards compatibility alias"""
    pass

