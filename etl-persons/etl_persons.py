#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Person Details API
Incrementally fetches person details for PERSON_IDs from recently updated accused records
Only processes person_ids from accused records that were added/updated since last run
"""

import sys
import os
import time
import re
import json
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import colorlog
from typing import Dict, Optional, List, Set, Tuple, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool, compute_safe_workers

from config import DB_CONFIG, API_CONFIG, LOG_CONFIG, TABLE_CONFIG, PERSON_GENDER_CONFIG, PERSON_GENDER_LLM_CONFIG

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))


def parse_iso_date(iso_date_str: str) -> datetime:
    """Parse ISO 8601 or YYYY-MM-DD date string to datetime."""
    if 'T' in iso_date_str or ' ' in iso_date_str:
        return datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
    dt = datetime.strptime(iso_date_str, '%Y-%m-%d')
    return dt.replace(tzinfo=IST_OFFSET)


def get_yesterday_end_ist() -> str:
    """Get yesterday 23:59:59 in IST as ISO-8601 string."""
    now_ist = datetime.now(IST_OFFSET)
    yesterday = now_ist - timedelta(days=1)
    return yesterday.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()

# Get table names from config (with fallback to defaults)
ACCUSED_TABLE = TABLE_CONFIG.get('accused', 'accused')
PERSONS_TABLE = TABLE_CONFIG.get('persons', 'persons')

# Setup logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    LOG_CONFIG['format'],
    datefmt=LOG_CONFIG['date_format'],
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(LOG_CONFIG['level'])


class PersonsETL:
    def __init__(self):
        self.db_pool = None
        self.person_gender_infer_on_unknown = bool(PERSON_GENDER_CONFIG.get('infer_on_unknown', True))
        self.person_gender_dry_run = bool(PERSON_GENDER_CONFIG.get('dry_run', False))
        self.person_gender_preserve_valid_api = bool(PERSON_GENDER_CONFIG.get('preserve_valid_api', True))
        # Per-pass minimum qualifying confidence — a pass must meet its own bar
        # or the cascade continues to the next pass.
        self.threshold_prefix = float(PERSON_GENDER_CONFIG.get('threshold_prefix', 0.85))
        self.threshold_rule   = float(PERSON_GENDER_CONFIG.get('threshold_rule',   0.80))
        self.threshold_suffix = float(PERSON_GENDER_CONFIG.get('threshold_suffix', 0.65))
        self.threshold_llm    = float(PERSON_GENDER_CONFIG.get('threshold_llm',    0.70))
        self.llm_gender_enabled = bool(PERSON_GENDER_LLM_CONFIG.get('enabled', True))
        self.llm_gender_url = str(PERSON_GENDER_LLM_CONFIG.get('url', 'http://192.168.103.106:11434')).rstrip('/')
        self.llm_gender_model = str(PERSON_GENDER_LLM_CONFIG.get('model', 'llama3.1:8b'))
        self.llm_gender_timeout = int(PERSON_GENDER_LLM_CONFIG.get('timeout', 20))
        self.llm_gender_batch_size = int(PERSON_GENDER_LLM_CONFIG.get('batch_size', 20))
        self._llm_gender_cache: Dict[str, Tuple[str, float]] = {}
        self.stats = {
            'person_ids': 0,
            'api_calls': 0,
            'failed_api_calls': 0,
            'inserted': 0,
            'updated': 0,
            'no_change': 0,  # Records that exist but no changes needed
            'no_data': 0,   # API returned 404 / no data (dead person_id)
            'failed': 0,  # Records that failed to process
            'errors': 0,
            'dry_run_changes': 0,
            'dry_run_no_change': 0,
            'dry_run_inserts': 0,
            'llm_skipped': 0,  # Records queued but not resolved by LLM
            'llm_resolved': 0  # Records successfully resolved by LLM
        }
        self.stats_lock = threading.Lock()
        self.schema_lock = threading.Lock()
        self.dry_run_lock = threading.Lock()
        self.llm_retry_queue = []  # Track records that failed LLM for retry
        self.llm_queue_lock = threading.Lock()

        self.dry_run_log_file = None
        if self.person_gender_dry_run:
            os.makedirs('logs', exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.dry_run_log_file = f"logs/person_gender_dry_run_{ts}.jsonl"
            logger.warning(f"⚠️  PERSON_GENDER_DRY_RUN enabled. No DB writes will be made. Dry-run log: {self.dry_run_log_file}")

        self.invalid_name_exact = {
            'absconding', 'unknown', 'unknown person', 'unknown persons',
            'not known', 'name not known', 'unidentified', 'dead body',
            'na', 'n a', 'n/a', 'nil', 'none', 'not available',
            'no name', 'accused', 'suspect', 'person'
        }
        self.placeholder_tokens = {
            'absconding', 'unknown', 'unidentified', 'na', 'n/a',
            'nil', 'none', 'dead', 'body', 'accused', 'suspect', 'person'
        }
        self.gender_map = {
            'male': 'Male',
            'm': 'Male',
            'man': 'Male',
            'boy': 'Male',
            'female': 'Female',
            'f': 'Female',
            'woman': 'Female',
            'girl': 'Female',
            'transgender': 'Transgender',
            'trans gender': 'Transgender',
            'third gender': 'Transgender',
            'trans': 'Transgender',
            'tg': 'Transgender',
            'unknown': 'Unknown',
            'unk': 'Unknown',
            'n/a': 'Unknown',
            'na': 'Unknown',
            'not known': 'Unknown',
            'not available': 'Unknown',
            '': 'Unknown'
        }
        # Expanded rule map — unambiguous Indian given names (consulted against
        # Telugu / Kannada / Hindi / Urdu / Muslim naming conventions).
        # Keys must be lowercase. Values are canonical gender strings.
        self.name_gender_rule_map = {
            # --- Male Telugu/Kannada given names ---
            'ramesh': 'Male', 'rajesh': 'Male', 'suresh': 'Male', 'mahesh': 'Male',
            'rahul': 'Male', 'vijay': 'Male', 'arun': 'Male', 'ashok': 'Male',
            'ajith': 'Male', 'ajay': 'Male', 'ravi': 'Male', 'siva': 'Male',
            'shiva': 'Male', 'surya': 'Male', 'sravan': 'Male', 'sravanthi': 'Female',
            'shekar': 'Male', 'sekhar': 'Male', 'shankar': 'Male', 'sankar': 'Male',
            'srikanth': 'Male', 'srikar': 'Male', 'venkat': 'Male', 'venkateswara': 'Male',
            'venkateswarlu': 'Male', 'venkataramana': 'Male', 'nagesh': 'Male',
            'naresh': 'Male', 'ganesh': 'Male', 'dinesh': 'Male', 'rakesh': 'Male',
            'lokesh': 'Male', 'mukesh': 'Male', 'satish': 'Male', 'girish': 'Male',
            'harish': 'Male', 'manish': 'Male', 'umesh': 'Male', 'yogesh': 'Male',
            'praveen': 'Male', 'naveen': 'Male', 'sandeep': 'Male', 'pradeep': 'Male',
            'deepak': 'Male', 'karthik': 'Male', 'kartik': 'Male', 'aakash': 'Male',
            'akash': 'Male', 'arjun': 'Male', 'abhishek': 'Male', 'anand': 'Male',
            'anurag': 'Male', 'arvind': 'Male', 'ashwin': 'Male', 'balaji': 'Male',
            'bhaskar': 'Male', 'charan': 'Male', 'chandra': 'Male', 'dhanush': 'Male',
            'gopal': 'Male', 'govind': 'Male', 'hari': 'Male', 'harsha': 'Male',
            'jagadeesh': 'Male', 'jagadish': 'Male', 'jagan': 'Male', 'jagath': 'Male',
            'kalyan': 'Male', 'kamal': 'Male', 'kishore': 'Male', 'krishna': 'Male',
            'madhu': 'Male', 'manoj': 'Male', 'mohan': 'Male', 'murali': 'Male',
            'nagaraju': 'Male', 'nataraju': 'Male', 'nataraj': 'Male', 'nikhil': 'Male',
            'niranjan': 'Male', 'pavan': 'Male', 'prabhu': 'Male', 'prasad': 'Male',
            'prashanth': 'Male', 'prashant': 'Male', 'praveen': 'Male', 'prem': 'Male',
            'raghav': 'Male', 'raghavendra': 'Male', 'raghu': 'Male', 'rajiv': 'Male',
            'rajkumar': 'Male', 'raju': 'Male', 'rakesh': 'Male', 'ram': 'Male',
            'ramakrishna': 'Male', 'raman': 'Male', 'ramana': 'Male', 'rambabu': 'Male',
            'ranga': 'Male', 'ranjith': 'Male', 'rishikesh': 'Male', 'ritesh': 'Male',
            'rohith': 'Male', 'rohit': 'Male', 'sampath': 'Male', 'santhosh': 'Male',
            'santosh': 'Male', 'sashidhar': 'Male', 'satya': 'Male', 'satyam': 'Male',
            'siddarth': 'Male', 'siddharth': 'Male', 'srinivas': 'Male', 'sriram': 'Male',
            'subash': 'Male', 'subhash': 'Male', 'sunil': 'Male', 'suraj': 'Male',
            'suryanarayana': 'Male', 'swamy': 'Male', 'tejas': 'Male', 'tilak': 'Male',
            'uday': 'Male', 'upendra': 'Male', 'varun': 'Male', 'venkatesh': 'Male',
            'vikram': 'Male', 'vikrant': 'Male', 'vikky': 'Male', 'vicky': 'Male',
            'vinay': 'Male', 'vinod': 'Male', 'vishnu': 'Male', 'vivek': 'Male',
            'yashwanth': 'Male', 'yashwant': 'Male', 'yash': 'Male', 'anoop': 'Male',
            'anup': 'Male', 'kevin': 'Male', 'tejavardhan': 'Male', 'harivardhan': 'Male',
            # --- Male Hindi/North Indian ---
            'abhijeet': 'Male', 'aditya': 'Male', 'amitabh': 'Male', 'amrit': 'Male',
            'ankur': 'Male', 'ankit': 'Male', 'atul': 'Male', 'gaurav': 'Male',
            'himanshu': 'Male', 'karan': 'Male', 'lalit': 'Male', 'manuj': 'Male',
            'neeraj': 'Male', 'niraj': 'Male', 'pankaj': 'Male', 'pardeep': 'Male',
            'rajendra': 'Male', 'rajesh': 'Male', 'ranjeet': 'Male', 'ranjit': 'Male',
            'ravindra': 'Male', 'sanjay': 'Male', 'sukhdev': 'Male', 'vikas': 'Male',
            'vineet': 'Male',
            # --- Male Muslim/Urdu names ---
            'areef': 'Male', 'arif': 'Male', 'amer': 'Male', 'ameer': 'Male',
            'amir': 'Male', 'baig': 'Male', 'faiz': 'Male', 'faizal': 'Male',
            'farhan': 'Male', 'faisal': 'Male', 'imran': 'Male', 'irfan': 'Male',
            'ishtiaq': 'Male', 'junaid': 'Male', 'khalid': 'Male', 'khaled': 'Male',
            'khaleem': 'Male', 'nayeemuddin': 'Male', 'shabbir': 'Male', 'asif': 'Male',
            'mohd': 'Male', 'mohammad': 'Male', 'mohammed': 'Male', 'mukhtar': 'Male',
            'mustafa': 'Male', 'nadeem': 'Male', 'naseer': 'Male', 'naveed': 'Male',
            'qayum': 'Male', 'riyaz': 'Male', 'rizwan': 'Male', 'salman': 'Male',
            'shahid': 'Male', 'shakeel': 'Male', 'shafiq': 'Male', 'sharif': 'Male',
            'shoaib': 'Male', 'sohail': 'Male', 'tariq': 'Male', 'usman': 'Male',
            'waseem': 'Male', 'zafar': 'Male', 'zaheer': 'Male', 'zubair': 'Male',
            # --- Specific names seen in audit data ---
            'gajanandh': 'Male', 'gowrav': 'Male', 'jyothiram': 'Male',
            'narsimma': 'Male', 'narsimha': 'Male', 'narasimha': 'Male',
            'rajashekar': 'Male', 'rajashekhar': 'Male', 'ramulu': 'Male',
            'tuntun': 'Male', 'abilash': 'Male', 'hemanth': 'Male', 'hemant': 'Male',
            'vamshi': 'Male', 'nitish': 'Male', 'pawan': 'Male', 'aman': 'Male',
            # --- Muslim/Urdu male names (additional) ---
            'abbu': 'Male',
            # --- Female Indian names ---
            'sita': 'Female', 'laxmi': 'Female', 'lakshmi': 'Female', 'kavitha': 'Female',
            'kavita': 'Female', 'sunita': 'Female', 'anjali': 'Female', 'pooja': 'Female',
            'puja': 'Female', 'anita': 'Female', 'archana': 'Female', 'asha': 'Female',
            'bhavana': 'Female', 'deepa': 'Female', 'deepika': 'Female', 'divya': 'Female',
            'geeta': 'Female', 'geetha': 'Female', 'hema': 'Female', 'indira': 'Female',
            'jyothi': 'Female', 'jyoti': 'Female', 'kamala': 'Female', 'kiran': 'Female',
            'komala': 'Female', 'krishnaveni': 'Female', 'kumari': 'Female',
            'madhavi': 'Female', 'mamatha': 'Female', 'manasa': 'Female',
            'meena': 'Female', 'meenakshi': 'Female', 'nandini': 'Female',
            'padma': 'Female', 'padmavathi': 'Female', 'parvathi': 'Female',
            'pavani': 'Female', 'priya': 'Female', 'priyanka': 'Female',
            'radha': 'Female', 'rajani': 'Female', 'rajitha': 'Female',
            'ramadevi': 'Female', 'ramya': 'Female', 'rani': 'Female',
            'ratna': 'Female', 'rekha': 'Female', 'revathi': 'Female',
            'rohini': 'Female', 'sarada': 'Female', 'saraswathi': 'Female',
            'savithri': 'Female', 'shantha': 'Female', 'shanthi': 'Female',
            'shobha': 'Female', 'sirisha': 'Female', 'sneha': 'Female',
            'soujanya': 'Female', 'sowmya': 'Female', 'sravanthi': 'Female',
            'sridevi': 'Female', 'subhadra': 'Female', 'sudha': 'Female',
            'sukanya': 'Female', 'suma': 'Female', 'sumathi': 'Female',
            'sunitha': 'Female', 'swapna': 'Female', 'swathi': 'Female',
            'usha': 'Female', 'vasantha': 'Female', 'vasavi': 'Female',
            'vidya': 'Female', 'vijaya': 'Female', 'vijayalaxmi': 'Female',
            'vimala': 'Female', 'yamini': 'Female',
            # --- Ambiguous / context-dependent — deliberately omitted ---
            # 'durga': omitted (male devotee names exist; context needed)
            # 'jagan': Male above; 'jagadamba' would be Female — distinct enough
        }

        # Male honorific prefixes: if any token matches, force Male regardless of suffix.
        # Handles names like "Mirza Amiar Baig", "Md Irfan", "Sheikh Rahman".
        self.male_prefix_tokens = {
            'mirza', 'mohammad', 'mohammed', 'md', 'mohd', 'sheikh', 'shaikh',
            'sheik', 'shaik', 'shiak',                   # common Hyderabad spellings
            'syed', 'sayyad', 'sayyed', 'mir', 'maulana', 'maulvi', 'hafiz',
            'chaudhry', 'chaudhary', 'khan', 'malik',
        }

        # Telugu/Kannada/South-Indian surname suffixes that should NOT trigger
        # the female heuristic. These are clan/caste/place endings that are
        # gender-neutral (or predominantly male in criminal records context).
        # Used to suppress the broad -a/-i suffix rule when the token is a surname.
        self.neutral_surname_suffixes = (
            'pati', 'pathi', 'kurthi', 'reddy', 'raju', 'naidu', 'gari', 'wari',
            'ouri', 'uri', 'ari', 'puri', 'vari', 'neni', 'sani', 'gari',
            'dhar', 'kar', 'war', 'pur',
        )

    def _normalize_space(self, value: str) -> str:
        return ' '.join(value.strip().split())

    def _canonical_name_for_match(self, value: str) -> str:
        lowered = value.lower().strip()
        lowered = re.sub(r'[^a-z0-9\s]+', ' ', lowered)
        return self._normalize_space(lowered)

    def _normalize_person_name(self, raw_name: Optional[str], personal: Dict) -> Optional[str]:
        if raw_name is None:
            name_part = self._normalize_space(str(personal.get('NAME') or ''))
            surname_part = self._normalize_space(str(personal.get('SURNAME') or ''))
            joined = self._normalize_space(f"{name_part} {surname_part}".strip())
            return joined if joined else None
        text = self._normalize_space(str(raw_name))
        if not text:
            return None
        return text

    def _build_inference_name(self, personal: Dict) -> Optional[str]:
        """
        Return the best single name string for gender inference.

        Uses FULL_NAME as the canonical, complete source.  Falls back to NAME
        only when FULL_NAME is absent.

        We deliberately do NOT concatenate NAME + SURNAME because treating a
        surname as if it were a given name is the primary source of false-gender
        assignments (e.g. NAME="Ravela" + SURNAME="Sanjay Kumar" should infer
        from the full string "Ravela Sanjay Kumar", not a fabricated concatenation
        that might put the surname first).

        Does NOT affect what is written to the DB — only used as input to
        _resolve_gender().
        """
        for field in ('FULL_NAME', 'NAME'):
            raw = personal.get(field)
            if not raw:
                continue
            text = self._normalize_space(str(raw).strip())
            # Strip alias markers like "@DJ Rahul"
            text = self._normalize_space(re.split(r'@', text)[0].strip())
            if text:
                return text
        return None

    def _is_valid_person_name(self, clean_name: Optional[str]) -> bool:
        if clean_name is None:
            return False
        lowered = clean_name.lower().strip()
        if not lowered:
            return False

        canonical = self._canonical_name_for_match(lowered)
        if canonical in self.invalid_name_exact:
            return False

        alpha_chars = [ch for ch in lowered if ch.isalpha()]
        if len(alpha_chars) < 2:
            return False
        alpha_ratio = len(alpha_chars) / max(len(lowered), 1)
        if alpha_ratio < 0.35:
            return False

        tokens = [t for t in re.split(r'[^a-z]+', canonical) if t]
        if tokens and all(token in self.placeholder_tokens for token in tokens):
            return False
        if re.search(r'\b(name\s+not\s+known|unknown\s+person(s)?|absconding\s+accused|dead\s+body|unidentified)\b', canonical):
            return False
        return True

    def _normalize_api_gender(self, raw_gender: Optional[str]) -> Optional[str]:
        if raw_gender is None:
            return 'Unknown'
        value = self._normalize_space(str(raw_gender)).strip().lower()
        mapped = self.gender_map.get(value)
        if mapped:
            return mapped
        compact = self._normalize_space(value.replace('-', ' '))
        mapped = self.gender_map.get(compact)
        if mapped:
            return mapped
        return None

    def _load_learned_gender_rules(self) -> None:
        """
        Mine the persons table for rows where the API provided a validated gender
        (Male or Female, gender_source='api') and extend the static rule_map and
        male_prefix_tokens with patterns observed in those full_name values.

        Design rules:
        • Only 'Male' / 'Female' are learned — Transgender is intentionally excluded
          because it cannot be reliably inferred from name tokens alone.
        • Learned entries AUGMENT the static maps; they never override existing entries.
        • A token is added to rule_map only when it appears in ≥ MIN_COUNT names AND
          ≥ MIN_CONFIDENCE fraction of those names share the same gender.
        • A token is added to male_prefix_tokens (first-position only) with a stricter
          threshold of 0.95.
        • Uses full_name exclusively — no NAME/SURNAME concatenation.
        """
        min_count = int(os.environ.get('GENDER_LEARN_MIN_COUNT', '3'))
        min_conf = float(os.environ.get('GENDER_LEARN_MIN_CONFIDENCE', '0.90'))
        prefix_min_conf = 0.95

        try:
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT full_name, gender
                    FROM {PERSONS_TABLE}
                    WHERE gender IN ('Male', 'Female')
                      AND gender_source = 'api'
                      AND full_name IS NOT NULL
                      AND TRIM(full_name) != ''
                """)
                rows = cursor.fetchall()
        except Exception as exc:
            logger.warning(f'⚠️  Could not load learned gender rules from DB: {exc}')
            return

        logger.info(f'🎓 Learning name patterns from {len(rows)} API-validated records …')

        # token → {'Male': N, 'Female': N}
        token_counts: Dict[str, Dict[str, int]] = {}
        # first-position token → {'Male': N, 'Female': N}
        first_token_counts: Dict[str, Dict[str, int]] = {}

        for full_name, gender in rows:
            tokens = self._tokenize_name_for_inference(full_name)
            if not tokens:
                continue
            first_token_counts.setdefault(tokens[0], {'Male': 0, 'Female': 0})
            first_token_counts[tokens[0]][gender] = first_token_counts[tokens[0]].get(gender, 0) + 1
            for token in tokens:
                token_counts.setdefault(token, {'Male': 0, 'Female': 0})
                token_counts[token][gender] = token_counts[token].get(gender, 0) + 1

        new_rules = new_prefixes = 0

        for token, counts in token_counts.items():
            if token in self.name_gender_rule_map:
                continue  # static rule wins
            total = counts.get('Male', 0) + counts.get('Female', 0)
            if total < min_count:
                continue
            for gender_label in ('Male', 'Female'):
                if counts.get(gender_label, 0) / total >= min_conf:
                    self.name_gender_rule_map[token] = gender_label
                    new_rules += 1
                    break

        for token, counts in first_token_counts.items():
            if token in self.male_prefix_tokens:
                continue  # static prefix wins
            if token in self.name_gender_rule_map:
                continue  # already covered by rule_map
            total = counts.get('Male', 0) + counts.get('Female', 0)
            if total < min_count:
                continue
            if counts.get('Male', 0) / total >= prefix_min_conf:
                self.male_prefix_tokens.add(token)
                new_prefixes += 1

        logger.info(
            f'   ✅ Learned {new_rules} rule-map entries, {new_prefixes} prefix tokens '
            f'from {len(rows)} API records (min_count={min_count}, min_conf={min_conf:.0%})'
        )

    def _tokenize_name_for_inference(self, clean_name: Optional[str]) -> List[str]:
        """Tokenize a name string into lowercase alpha tokens (len > 1), stripping alias markers."""
        if not clean_name:
            return []
        name_part = re.split(r'@', clean_name)[0].strip()
        return [t for t in re.findall(r'[A-Za-z]+', name_part.lower()) if len(t) > 1]

    # ── Pass 1: Male honorific / prefix detection ────────────────────────────────
    # Confidence: 0.95  |  Min qualifying: threshold_prefix (default 0.85)
    def _infer_pass_prefix(self, tokens: List[str]) -> Tuple[Optional[str], float, str]:
        """Return Male at 0.95 if any token is a recognised male honorific prefix."""
        for token in tokens:
            if token in self.male_prefix_tokens:
                return 'Male', 0.95, 'prefix'
        return None, 0.0, 'prefix'

    # ── Pass 2: Rule-map lookup ───────────────────────────────────────────────────
    # Confidence: 0.90  |  Min qualifying: threshold_rule (default 0.80)
    def _infer_pass_rule(self, tokens: List[str]) -> Tuple[Optional[str], float, str]:
        """
        Scan tokens in reverse (given-name-last order for South Indian names).
        Returns the first unambiguous rule_map hit.
        """
        for token in reversed(tokens):
            match = self.name_gender_rule_map.get(token)
            if match:
                return match, 0.90, 'rule'
        return None, 0.0, 'rule'

    # ── Pass 3: Suffix heuristic ─────────────────────────────────────────────────
    # Confidence: 0.65  |  Min qualifying: threshold_suffix (default 0.65)
    def _infer_pass_suffix(self, tokens: List[str]) -> Tuple[Optional[str], float, str]:
        """
        Check the last token against known gender-indicating suffixes.
        Suppressed for neutral South Indian surname suffixes (Reddy, Raju, etc.)
        to reduce false positives.
        """
        if not tokens:
            return None, 0.0, 'heuristic'
        last = tokens[-1]
        if any(last.endswith(sfx) for sfx in self.neutral_surname_suffixes):
            return None, 0.0, 'heuristic'
        male_suffixes = ('esh', 'endra', 'kumar', 'raj', 'veer', 'wanth', 'kanth',
                         'nath', 'deep', 'jeet', 'preet', 'arth')
        if last.endswith(male_suffixes):
            return 'Male', 0.65, 'heuristic'
        female_suffixes = ('ya', 'ika', 'itha', 'ita', 'ini', 'avani', 'avathi',
                           'aveni', 'rani', 'devi', 'veni', 'vathi')
        if last.endswith(female_suffixes):
            return 'Female', 0.65, 'heuristic'
        return None, 0.0, 'heuristic'

    def _infer_gender_from_name(self, clean_name: Optional[str]) -> Tuple[Optional[str], float, str]:
        """
        Convenience wrapper — runs all 3 rule-based passes in order and returns
        the first non-None result.  Callers that need threshold-gated cascading
        should call _resolve_gender instead.
        """
        tokens = self._tokenize_name_for_inference(clean_name)
        if not tokens:
            return None, 0.0, 'heuristic'
        for pass_fn in (self._infer_pass_prefix, self._infer_pass_rule, self._infer_pass_suffix):
            gender, conf, source = pass_fn(tokens)
            if gender:
                return gender, conf, source
        return None, 0.0, 'heuristic'

    def _infer_gender_llm_batch(self, names: List[str]) -> Dict[str, Tuple[str, float]]:
        """
        Call Ollama LLM to infer gender for a batch of Indian person names.
        Used only when rule-based inference returns Unknown with confidence=0.
        Returns {name: (gender, confidence)} — missing entries mean LLM also failed.
        Results are cached to avoid duplicate API calls within a run.
        """
        if not self.llm_gender_enabled or not names:
            return {}

        uncached = [n for n in names if n not in self._llm_gender_cache]
        if not uncached:
            return {n: self._llm_gender_cache[n] for n in names if n in self._llm_gender_cache}

        numbered = '\n'.join(f'{i+1}. {name}' for i, name in enumerate(uncached))
        prompt = (
            'You classify gender of Indian person names (Telugu, Kannada, Hindi, Urdu, Muslim).\n'
            'Rules: Shaik/Syed/Md/Mohammad/Khan/Mirza prefix → Male. '
            '"Bai" alone is ambiguous — check full name context.\n'
            'IMPORTANT: only return "Male" or "Female" or "Unknown". Never return "Transgender".\n'
            'Return ONLY a valid JSON object with key "results" containing an array.\n'
            'Each element: {"name": "<original>", "gender": "Male"|"Female"|"Unknown", "confidence": 0.0-1.0}\n\n'
            f'Names:\n{numbered}\n\n'
            f'Expected: {{"results": [{{"name": "{uncached[0]}", "gender": "Male", "confidence": 0.9}}, ...]}}\n\nJSON:'
        )

        try:
            resp = requests.post(
                f'{self.llm_gender_url}/api/generate',
                json={
                    'model': self.llm_gender_model,
                    'prompt': prompt,
                    'stream': False,
                    'format': 'json',
                    'options': {'temperature': 0.0, 'num_predict': 600},
                },
                timeout=self.llm_gender_timeout,
            )
            if resp.status_code != 200:
                logger.warning(f'LLM gender inference: HTTP {resp.status_code}')
                return {}

            raw = resp.json().get('response', '')
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            # Unwrap {"results": [...]} or accept bare list
            if isinstance(parsed, dict):
                parsed = next((v for v in parsed.values() if isinstance(v, list)), [])
            if not isinstance(parsed, list):
                logger.warning('LLM gender inference: unexpected response format')
                return {}

            results: Dict[str, Tuple[str, float]] = {}
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                name = item.get('name', '').strip()
                gender = item.get('gender', 'Unknown')
                confidence = min(1.0, float(item.get('confidence', 0.75)))
                # Transgender is never inferred — API source only
                if gender in ('Male', 'Female') and name:
                    results[name] = (gender, confidence)
                    self._llm_gender_cache[name] = (gender, confidence)

            # Fuzzy match for names the model slightly reformatted
            for orig in uncached:
                if orig not in results:
                    for k, v in results.items():
                        if orig.lower().strip() == k.lower().strip():
                            self._llm_gender_cache[orig] = v
                            break

            logger.debug(f'LLM gender: resolved {len(results)}/{len(uncached)} names')
            return {n: self._llm_gender_cache[n] for n in names if n in self._llm_gender_cache}

        except Exception as exc:
            logger.warning(f'LLM gender inference failed: {exc}')
            return {}

    def _normalize_phone_numbers(self, raw_phone: Any) -> List[str]:
        """Normalize phone payloads into a de-duplicated list preserving source order."""
        collected: List[str] = []

        def collect(value: Any):
            if value is None:
                return
            if isinstance(value, dict):
                for item in value.values():
                    collect(item)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    collect(item)
                return

            text = self._normalize_space(str(value))
            if not text:
                return
            for chunk in re.split(r'[\n,;|/]+', text):
                normalized = self._normalize_space(chunk)
                if normalized:
                    collected.append(normalized)

        collect(raw_phone)

        invalid_tokens = {'na', 'n/a', 'none', 'null', 'not available', 'unknown', '-'}
        deduped: List[str] = []
        seen: Set[str] = set()
        for value in collected:
            lowered = value.lower()
            if lowered in invalid_tokens:
                continue
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped

    def _resolve_gender(self, clean_name: Optional[str], api_gender_raw: Optional[str]) -> Tuple[str, float, str]:
        """
        Multi-pass gender resolution with per-pass minimum qualifying confidence (MQC).

        Cascade order:
          Pass 0  API          — conf 1.0  — always accepted when valid
          Pass 1  Prefix       — conf 0.95 — MQC: threshold_prefix  (default 0.85)
          Pass 2  Rule-map     — conf 0.90 — MQC: threshold_rule    (default 0.80)
          Pass 3  Suffix       — conf 0.65 — MQC: threshold_suffix  (default 0.65)
          Pass 4  LLM          — conf varies — MQC: threshold_llm   (default 0.70)
          Fallback → 'Unknown'

        A pass is skipped entirely when its result is None OR its confidence falls
        below the pass-specific MQC; control then falls to the next pass.
        If every pass fails (or returns below its MQC), the result is 'Unknown'.
        """
        # ── Pass 0: API-provided gender ──────────────────────────────────────────
        normalized_api = self._normalize_api_gender(api_gender_raw)
        if self.person_gender_preserve_valid_api and normalized_api in ('Male', 'Female', 'Transgender'):
            return normalized_api, 1.0, 'api'

        if not self._is_valid_person_name(clean_name):
            return 'Unknown', 0.0, 'invalid_name'

        if normalized_api in ('Male', 'Female', 'Transgender'):
            return normalized_api, 1.0, 'api'

        # API is Unknown/null — skip inference passes if caller opted out
        if normalized_api == 'Unknown' and not self.person_gender_infer_on_unknown:
            return 'Unknown', 1.0, 'api'

        # ── Tokenise once for all rule-based passes ──────────────────────────────
        tokens = self._tokenize_name_for_inference(clean_name)
        if not tokens:
            return 'Unknown', 0.0, 'invalid_name'

        # ── Pass 1: Male honorific prefix ────────────────────────────────────────
        gender, conf, source = self._infer_pass_prefix(tokens)
        if gender and conf >= self.threshold_prefix:
            return gender, conf, source

        # ── Pass 2: Rule-map lookup ───────────────────────────────────────────────
        gender, conf, source = self._infer_pass_rule(tokens)
        if gender and conf >= self.threshold_rule:
            return gender, conf, source

        # ── Pass 3: Suffix heuristic ─────────────────────────────────────────────
        gender, conf, source = self._infer_pass_suffix(tokens)
        if gender and conf >= self.threshold_suffix:
            return gender, conf, source

        # ── Pass 4: LLM fallback ─────────────────────────────────────────────────
        if self.llm_gender_enabled:
            llm_result = self._infer_gender_llm_batch([clean_name])
            if clean_name in llm_result:
                llm_gender, llm_conf = llm_result[clean_name]
                # Transgender is never assigned by inference — API source only.
                if llm_gender in ('Male', 'Female') and llm_conf >= self.threshold_llm:
                    return llm_gender, llm_conf, 'llm'

        # ── All passes failed ────────────────────────────────────────────────────
        return 'Unknown', 0.0, 'unknown'

    def connect_db(self):
        try:
            max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
            self.db_pool = PostgreSQLConnectionPool(
                min_conn=1,
                max_conn=max_workers + 5, 
                **DB_CONFIG
            )
            logger.info(f"✅ Connected to database: {DB_CONFIG['dbname']} (Pool max: {self.db_pool.max_conn})")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def close_db(self):
        if self.db_pool:
            self.db_pool.close_all()
        logger.info("Database connection pool closed")

    def ensure_run_state_table(self):
        """Ensure ETL run-state table exists."""
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_run_state (
                    module_name TEXT PRIMARY KEY,
                    last_successful_end TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def get_run_checkpoint(self, module_name: str) -> Optional[datetime]:
        """Read last successful checkpoint for a module."""
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
                (module_name,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def update_run_checkpoint(self, module_name: str, end_date_iso: str):
        """Persist successful run end boundary for resume safety."""
        end_dt = parse_iso_date(end_date_iso)
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO etl_run_state (module_name, last_successful_end, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (module_name) DO UPDATE SET
                    last_successful_end = EXCLUDED.last_successful_end,
                    updated_at = CURRENT_TIMESTAMP
            """, (module_name, end_dt))
            conn.commit()
    
    def get_table_columns(self, table_name: str) -> Set[str]:
        """Get all column names from a table."""
        try:
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            return set()
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in table.
        Returns dict mapping API field name to database column name (snake_case).
        """
        new_fields = {}
        
        # Map API field names to database column names
        # Personal Details mapping
        personal_fields = {
            'NAME': 'name',
            'SURNAME': 'surname',
            'ALIAS': 'alias',
            'FULL_NAME': 'full_name',
            'RELATION_TYPE': 'relation_type',
            'RELATIVE_NAME': 'relative_name',
            'GENDER': 'gender',
            'IS_DIED': 'is_died',
            'DATE_OF_BIRTH': 'date_of_birth',
            'AGE': 'age',
            'OCCUPATION': 'occupation',
            'EDUCATION_QUALIFICATION': 'education_qualification',
            'CASTE': 'caste',
            'SUB_CASTE': 'sub_caste',
            'RELIGION': 'religion',
            'NATIONALITY': 'nationality',
            'DESIGNATION': 'designation',
            'PLACE_OF_WORK': 'place_of_work'
        }
        
        # Present Address mapping
        present_address_fields = {
            'HOUSE_NO': 'present_house_no',
            'STREET_ROAD_NO': 'present_street_road_no',
            'WARD_COLONY': 'present_ward_colony',
            'LANDMARK_MILESTONE': 'present_landmark_milestone',
            'LOCALITY_VILLAGE': 'present_locality_village',
            'AREA_MANDAL': 'present_area_mandal',
            'DISTRICT': 'present_district',
            'STATE_UT': 'present_state_ut',
            'COUNTRY': 'present_country',
            'RESIDENCY_TYPE': 'present_residency_type',
            'PIN_CODE': 'present_pin_code',
            'JURISDICTION_PS': 'present_jurisdiction_ps'
        }
        
        # Permanent Address mapping
        permanent_address_fields = {
            'HOUSE_NO': 'permanent_house_no',
            'STREET_ROAD_NO': 'permanent_street_road_no',
            'WARD_COLONY': 'permanent_ward_colony',
            'LANDMARK_MILESTONE': 'permanent_landmark_milestone',
            'LOCALITY_VILLAGE': 'permanent_locality_village',
            'AREA_MANDAL': 'permanent_area_mandal',
            'DISTRICT': 'permanent_district',
            'STATE_UT': 'permanent_state_ut',
            'COUNTRY': 'permanent_country',
            'RESIDENCY_TYPE': 'permanent_residency_type',
            'PIN_CODE': 'permanent_pin_code',
            'JURISDICTION_PS': 'permanent_jurisdiction_ps'
        }
        
        # Contact Details mapping
        contact_fields = {
            'PHONE_NUMBER': 'phone_number',
            'COUNTRY_CODE': 'country_code',
            'EMAIL_ID': 'email_id'
        }
        
        # Top-level fields
        top_level_fields = {
            'PERSON_ID': 'person_id',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        # Check top-level fields
        for api_field, db_column in top_level_fields.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        # Check Personal Details
        personal = api_record.get('PERSONAL_DETAILS', {})
        if isinstance(personal, dict):
            for api_field, db_column in personal_fields.items():
                if api_field in personal and db_column not in table_columns:
                    new_fields[f'PERSONAL_DETAILS.{api_field}'] = db_column
        
        # Check Present Address
        present = api_record.get('PRESENT_ADDRESS', {})
        if isinstance(present, dict):
            for api_field, db_column in present_address_fields.items():
                if api_field in present and db_column not in table_columns:
                    new_fields[f'PRESENT_ADDRESS.{api_field}'] = db_column
        
        # Check Permanent Address
        permanent = api_record.get('PERMANENT_ADDRESS', {})
        if isinstance(permanent, dict):
            for api_field, db_column in permanent_address_fields.items():
                if api_field in permanent and db_column not in table_columns:
                    new_fields[f'PERMANENT_ADDRESS.{api_field}'] = db_column
        
        # Check Contact Details
        contact = api_record.get('CONTACT_DETAILS', {})
        if isinstance(contact, dict):
            for api_field, db_column in contact_fields.items():
                if api_field in contact and db_column not in table_columns:
                    new_fields[f'CONTACT_DETAILS.{api_field}'] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the persons table."""
        with self.schema_lock:
            try:
                explicit_type = (column_type or '').strip().upper()
                if explicit_type and explicit_type != 'TEXT':
                    column_type = explicit_type
                else:
                    # Determine column type based on field name
                    if 'date' in column_name.lower():
                        column_type = 'DATE'
                    elif column_name == 'age':
                        column_type = 'INTEGER'
                    elif column_name in ('is_died',):
                        column_type = 'BOOLEAN'
                    elif column_name in ('name', 'surname', 'alias', 'full_name', 'raw_full_name', 'occupation', 
                                        'education_qualification', 'designation', 'place_of_work'):
                        column_type = 'VARCHAR(500)'
                    elif column_name in ('relation_type', 'gender', 'caste', 'sub_caste', 'religion', 
                                        'nationality', 'residency_type', 'country_code', 'gender_source'):
                        column_type = 'VARCHAR(100)'
                    elif column_name == 'gender_confidence':
                        column_type = 'NUMERIC(4,3)'
                    elif 'pin_code' in column_name.lower() or 'jurisdiction_ps' in column_name.lower():
                        column_type = 'VARCHAR(20)'
                    elif 'phone_number' in column_name.lower() or 'email_id' in column_name.lower():
                        column_type = 'VARCHAR(255)'
                    elif 'house_no' in column_name.lower() or 'street' in column_name.lower() or \
                         'ward' in column_name.lower() or 'landmark' in column_name.lower() or \
                         'locality' in column_name.lower() or 'area' in column_name.lower() or \
                         'district' in column_name.lower() or 'state' in column_name.lower() or \
                         'country' in column_name.lower():
                        column_type = 'VARCHAR(255)'
                    else:
                        column_type = 'VARCHAR(255)'
                
                with self.db_pool.get_connection_context() as conn:
                    cursor = conn.cursor()
                    alter_sql = f"ALTER TABLE {PERSONS_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    cursor.execute(alter_sql)
                    conn.commit()
                logger.info(f"✅ Added column {column_name} ({column_type}) to {PERSONS_TABLE}")
                return True
            except Exception as e:
                logger.error(f"❌ Error adding column {column_name}: {e}")
                return False

    def ensure_person_enrichment_columns(self, table_columns: Set[str]) -> Set[str]:
        """Ensure required enrichment columns exist in persons table."""
        required_columns = {
            'raw_full_name': 'VARCHAR(500)',
            'gender_confidence': 'NUMERIC(4,3)',
            'gender_source': 'VARCHAR(20)',
            'phone_numbers': 'TEXT'
        }
        for column_name, column_type in required_columns.items():
            if column_name not in table_columns:
                if self.add_column_to_table(column_name, column_type):
                    table_columns.add(column_name)
        return table_columns

    def apply_person_enrichment(self, person_id: str, raw_full_name: Optional[str],
                                gender_confidence: Optional[float], gender_source: Optional[str],
                                phone_numbers: Optional[str],
                                table_columns: Set[str], cursor):
        """Apply enrichment fields that are not part of the base API mapping."""
        updates = []
        values = []

        if 'raw_full_name' in table_columns:
            updates.append('raw_full_name = COALESCE(%s, raw_full_name)')
            values.append(raw_full_name)

        if 'gender_confidence' in table_columns:
            updates.append('gender_confidence = COALESCE(%s, gender_confidence)')
            values.append(gender_confidence)

        if 'gender_source' in table_columns:
            updates.append('gender_source = COALESCE(%s, gender_source)')
            values.append(gender_source)

        if 'phone_numbers' in table_columns:
            updates.append('phone_numbers = COALESCE(%s, phone_numbers)')
            values.append(phone_numbers)

        if not updates:
            return

        values.append(person_id)
        cursor.execute(
            f"""
                UPDATE {PERSONS_TABLE}
                SET {', '.join(updates)}
                WHERE person_id = %s
            """,
            tuple(values)
        )

    def _normalize_confidence(self, value: Optional[Any]) -> Optional[float]:
        if value is None:
            return None
        try:
            return round(float(value), 3)
        except Exception:
            return None

    def log_dry_run_change(self, person_id: str, operation: str, changes: Dict[str, Dict[str, Any]],
                           preview: Dict[str, Any]):
        if not self.dry_run_log_file:
            return
        payload = {
            'timestamp': datetime.now(tz=IST_OFFSET).isoformat(),
            'person_id': person_id,
            'operation': operation,
            'changes': changes,
            'preview': preview,
        }
        with self.dry_run_lock:
            with open(self.dry_run_log_file, 'a', encoding='utf-8') as handle:
                handle.write(json.dumps(payload, ensure_ascii=True) + '\n')

    def handle_dry_run(self, person_id: str, clean_full_name: Optional[str], raw_full_name_value: Optional[str],
                       resolved_gender: Optional[str], gender_confidence: Optional[float],
                       gender_source: Optional[str], phone_numbers_value: Optional[str],
                       table_columns: Set[str], cursor):
        select_columns = ['full_name', 'gender']
        if 'raw_full_name' in table_columns:
            select_columns.append('raw_full_name')
        if 'gender_confidence' in table_columns:
            select_columns.append('gender_confidence')
        if 'gender_source' in table_columns:
            select_columns.append('gender_source')
        if 'phone_numbers' in table_columns:
            select_columns.append('phone_numbers')

        cursor.execute(
            f"""
                SELECT {', '.join(select_columns)}
                FROM {PERSONS_TABLE}
                WHERE person_id = %s
            """,
            (person_id,)
        )
        row = cursor.fetchone()

        new_conf = self._normalize_confidence(gender_confidence)
        if row is None:
            preview = {
                'full_name': clean_full_name,
                'raw_full_name': raw_full_name_value,
                'gender': resolved_gender,
                'gender_confidence': new_conf,
                'gender_source': gender_source,
                'phone_numbers': phone_numbers_value,
            }
            self.log_dry_run_change(person_id, 'insert', {}, preview)
            with self.stats_lock:
                self.stats['dry_run_inserts'] += 1
            return

        idx = {name: i for i, name in enumerate(select_columns)}
        old_full_name = row[idx['full_name']] if 'full_name' in idx else None
        old_gender = row[idx['gender']] if 'gender' in idx else None
        old_raw_full_name = row[idx['raw_full_name']] if 'raw_full_name' in idx else None
        old_conf = row[idx['gender_confidence']] if 'gender_confidence' in idx else None
        old_source = row[idx['gender_source']] if 'gender_source' in idx else None
        old_phone_numbers = row[idx['phone_numbers']] if 'phone_numbers' in idx else None
        old_conf = self._normalize_confidence(old_conf)
        changes: Dict[str, Dict[str, Any]] = {}

        if clean_full_name is not None and clean_full_name != old_full_name:
            changes['full_name'] = {'old': old_full_name, 'new': clean_full_name}
        if raw_full_name_value is not None and raw_full_name_value != old_raw_full_name:
            changes['raw_full_name'] = {'old': old_raw_full_name, 'new': raw_full_name_value}
        if resolved_gender is not None and resolved_gender != old_gender:
            changes['gender'] = {'old': old_gender, 'new': resolved_gender}
        if new_conf is not None and new_conf != old_conf:
            changes['gender_confidence'] = {'old': old_conf, 'new': new_conf}
        if gender_source is not None and gender_source != old_source:
            changes['gender_source'] = {'old': old_source, 'new': gender_source}
        if phone_numbers_value is not None and phone_numbers_value != old_phone_numbers:
            changes['phone_numbers'] = {'old': old_phone_numbers, 'new': phone_numbers_value}

        if changes:
            preview = {
                'full_name': clean_full_name,
                'raw_full_name': raw_full_name_value,
                'gender': resolved_gender,
                'gender_confidence': new_conf,
                'gender_source': gender_source,
                'phone_numbers': phone_numbers_value,
            }
            self.log_dry_run_change(person_id, 'update', changes, preview)
            with self.stats_lock:
                self.stats['dry_run_changes'] += 1
        else:
            with self.stats_lock:
                self.stats['dry_run_no_change'] += 1
    
    def update_existing_records_with_new_fields(self, new_fields: Dict[str, str]):
        """
        Update existing records with new fields.
        For new fields, set to NULL (they will be updated when those records are processed).
        """
        if not new_fields:
            return
        
        try:
            logger.info(f"📝 New fields detected: {list(new_fields.keys())}")
            logger.info(f"   Note: Existing records will be updated when processed in future ETL runs")
            logger.info(f"   New fields are set to NULL for existing records until they are reprocessed")
        except Exception as e:
            logger.error(f"❌ Error updating existing records: {e}")
    
    def update_new_fields(self, person_id: str, p: Dict, personal: Dict, present: Dict, 
                         permanent: Dict, contact: Dict, table_columns: Set[str], cursor):
        """
        Update any new fields that were added via schema evolution.
        This handles fields that exist in table_columns but are not in the standard field list.
        """
        if not table_columns:
            return
        
        # Standard fields that are already handled in the main UPDATE/INSERT
        standard_fields = {
            'person_id', 'name', 'surname', 'alias', 'full_name', 'relation_type', 'relative_name',
            'gender', 'is_died', 'date_of_birth', 'age', 'occupation', 'education_qualification',
            'caste', 'sub_caste', 'religion', 'nationality', 'designation', 'place_of_work',
            'raw_full_name', 'gender_confidence', 'gender_source',
            'present_house_no', 'present_street_road_no', 'present_ward_colony',
            'present_landmark_milestone', 'present_locality_village', 'present_area_mandal',
            'present_district', 'present_state_ut', 'present_country', 'present_residency_type',
            'present_pin_code', 'present_jurisdiction_ps',
            'permanent_house_no', 'permanent_street_road_no', 'permanent_ward_colony',
            'permanent_landmark_milestone', 'permanent_locality_village', 'permanent_area_mandal',
            'permanent_district', 'permanent_state_ut', 'permanent_country', 'permanent_residency_type',
            'permanent_pin_code', 'permanent_jurisdiction_ps',
            'phone_number', 'phone_numbers', 'country_code', 'email_id', 'date_created', 'date_modified'
        }
        
        # Find new fields (exist in table but not in standard fields)
        new_fields_to_update = {}
        
        # Check for new fields in API response that exist in table_columns
        # Map API field paths to database column names
        field_mapping = {
            'PERSON_ID': ('person_id', p.get('PERSON_ID')),
            'DATE_CREATED': ('date_created', p.get('DATE_CREATED')),
            'DATE_MODIFIED': ('date_modified', p.get('DATE_MODIFIED')),
        }
        
        # Personal details
        personal_mapping = {
            'NAME': 'name', 'SURNAME': 'surname', 'ALIAS': 'alias', 'FULL_NAME': 'full_name',
            'RELATION_TYPE': 'relation_type', 'RELATIVE_NAME': 'relative_name', 'GENDER': 'gender',
            'IS_DIED': 'is_died', 'DATE_OF_BIRTH': 'date_of_birth', 'AGE': 'age',
            'OCCUPATION': 'occupation', 'EDUCATION_QUALIFICATION': 'education_qualification',
            'CASTE': 'caste', 'SUB_CASTE': 'sub_caste', 'RELIGION': 'religion',
            'NATIONALITY': 'nationality', 'DESIGNATION': 'designation', 'PLACE_OF_WORK': 'place_of_work'
        }
        
        for api_field, db_column in personal_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = personal.get(api_field)
                if api_field == 'AGE':
                    # Handle age normalization
                    try:
                        if value is None:
                            value = None
                        elif isinstance(value, (int, float)):
                            value = int(value)
                        elif isinstance(value, str):
                            s = value.strip()
                            value = int(s) if s.isdigit() else None
                    except Exception:
                        value = None
                elif api_field == 'DATE_OF_BIRTH':
                    value = value  # Keep as is, will be handled by NULLIF in SQL if needed
                elif api_field in ('NAME', 'SURNAME', 'ALIAS', 'FULL_NAME', 'OCCUPATION', 
                                  'EDUCATION_QUALIFICATION', 'DESIGNATION', 'PLACE_OF_WORK'):
                    value = self.truncate_string(value, 500, db_column)
                elif api_field in ('RELATION_TYPE', 'GENDER', 'CASTE', 'SUB_CASTE', 'RELIGION', 
                                  'NATIONALITY'):
                    value = self.truncate_string(value, 100, db_column)
                else:
                    value = self.truncate_string(value, 255, db_column)
                new_fields_to_update[db_column] = value
        
        # Present address fields
        present_mapping = {
            'HOUSE_NO': 'present_house_no', 'STREET_ROAD_NO': 'present_street_road_no',
            'WARD_COLONY': 'present_ward_colony', 'LANDMARK_MILESTONE': 'present_landmark_milestone',
            'LOCALITY_VILLAGE': 'present_locality_village', 'AREA_MANDAL': 'present_area_mandal',
            'DISTRICT': 'present_district', 'STATE_UT': 'present_state_ut', 'COUNTRY': 'present_country',
            'RESIDENCY_TYPE': 'present_residency_type', 'PIN_CODE': 'present_pin_code',
            'JURISDICTION_PS': 'present_jurisdiction_ps'
        }
        
        for api_field, db_column in present_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = present.get(api_field)
                if 'pin_code' in db_column or 'jurisdiction_ps' in db_column:
                    value = self.truncate_string(value, 20, db_column)
                else:
                    value = self.truncate_string(value, 255, db_column)
                new_fields_to_update[db_column] = value
        
        # Permanent address fields
        permanent_mapping = {
            'HOUSE_NO': 'permanent_house_no', 'STREET_ROAD_NO': 'permanent_street_road_no',
            'WARD_COLONY': 'permanent_ward_colony', 'LANDMARK_MILESTONE': 'permanent_landmark_milestone',
            'LOCALITY_VILLAGE': 'permanent_locality_village', 'AREA_MANDAL': 'permanent_area_mandal',
            'DISTRICT': 'permanent_district', 'STATE_UT': 'permanent_state_ut', 'COUNTRY': 'permanent_country',
            'RESIDENCY_TYPE': 'permanent_residency_type', 'PIN_CODE': 'permanent_pin_code',
            'JURISDICTION_PS': 'permanent_jurisdiction_ps'
        }
        
        for api_field, db_column in permanent_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = permanent.get(api_field)
                if 'pin_code' in db_column or 'jurisdiction_ps' in db_column:
                    value = self.truncate_string(value, 20, db_column)
                else:
                    value = self.truncate_string(value, 255, db_column)
                new_fields_to_update[db_column] = value
        
        # Contact fields
        contact_mapping = {
            'PHONE_NUMBER': 'phone_number', 'COUNTRY_CODE': 'country_code', 'EMAIL_ID': 'email_id'
        }
        
        for api_field, db_column in contact_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = contact.get(api_field)
                if db_column == 'phone_number' or db_column == 'email_id':
                    value = self.truncate_string(value, 255, db_column)
                else:
                    value = self.truncate_string(value, 10, db_column)
                new_fields_to_update[db_column] = value
        
        # If there are new fields to update, execute an UPDATE statement
        if new_fields_to_update:
            try:
                set_clauses = [f"{col} = COALESCE(%s, {col})" for col in new_fields_to_update.keys()]
                update_values_list = list(new_fields_to_update.values()) + [person_id]
                
                update_query = f"""
                    UPDATE {PERSONS_TABLE} SET {', '.join(set_clauses)}
                    WHERE person_id = %s
                """
                cursor.execute(update_query, update_values_list)
                logger.debug(f"Updated new fields for person {person_id}: {list(new_fields_to_update.keys())}")
            except Exception as e:
                logger.warning(f"⚠️  Error updating new fields for person {person_id}: {e}")
                # Don't fail the whole operation, just log the warning

    def correct_heuristic_gender_records(self, dry_run: Optional[bool] = None) -> int:
        """
        Re-evaluate all persons rows where gender was set by the heuristic/rule engine
        (gender_source IN ('heuristic', 'rule', 'prefix')) and correct any that are
        now classified differently under the updated inference logic.

        This is a targeted remediation pass for records already written to the DB
        before the suffix-bug fix.  It is safe to run multiple times (idempotent).

        Pass dry_run=True to log changes without writing; defaults to
        self.person_gender_dry_run if not specified.

        Returns the number of rows corrected (or would-be corrected in dry-run).
        """
        effective_dry_run = self.person_gender_dry_run if dry_run is None else dry_run
        corrected = 0

        logger.info("🔍 Starting heuristic gender correction pass …")
        if effective_dry_run:
            logger.warning("⚠️  Dry-run mode — no DB writes will be made")

        # Force inference on during this pass regardless of the runtime flag —
        # we explicitly want to re-evaluate every row that was previously guessed.
        old_infer_flag = self.person_gender_infer_on_unknown
        self.person_gender_infer_on_unknown = True

        try:
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                # Fetch both wrong-gender rows AND previously-Unknown rows that may now resolve
                cursor.execute(f"""
                    SELECT person_id, full_name, raw_full_name, name, surname, alias,
                           gender, gender_confidence, gender_source
                    FROM {PERSONS_TABLE}
                    WHERE (
                        -- Re-evaluate rows written by old heuristic/rule logic
                        (gender_source IN ('heuristic', 'rule', 'prefix')
                         AND gender IN ('Male', 'Female', 'Transgender'))
                        OR
                        -- Also attempt to resolve previously-Unknown rows (source not api)
                        (gender = 'Unknown'
                         AND (gender_source IS NULL OR gender_source NOT IN ('api', 'invalid_name')))
                    )
                """)
                rows = cursor.fetchall()

            logger.info(f"   Found {len(rows)} rows to re-evaluate (heuristic corrections + Unknown backfill)")

            fix_batch: List[Tuple[str, str, float, str, str]] = []

            # Collect names that may need LLM batch processing (confidence=0 after rule pass)
            pending_llm: List[Tuple[str, str, str]] = []  # (person_id, inference_name, old_gender)

            for person_id, full_name, raw_full_name, db_name, db_surname, db_alias, old_gender, old_conf, old_source in rows:
                personal_mirror = {
                    'FULL_NAME': full_name or raw_full_name,
                    'NAME': db_name,
                    'SURNAME': db_surname,
                    'ALIAS': db_alias,
                }
                inference_name = self._build_inference_name(personal_mirror)
                new_gender, new_conf, new_source = self._resolve_gender(
                    clean_name=inference_name,
                    api_gender_raw=None,
                )

                if new_gender == old_gender or new_gender == 'Unknown':
                    # If still Unknown after rules, queue for LLM batch
                    if new_gender == 'Unknown' and new_conf == 0.0 and inference_name and self.llm_gender_enabled:
                        pending_llm.append((person_id, inference_name, old_gender))
                    continue

                corrected += 1
                logger.info(
                    f"   ✏️  {person_id} | {full_name!r} → "
                    f"{old_gender} ({old_source} {old_conf}) → {new_gender} ({new_source} {new_conf:.3f})"
                )
                if not effective_dry_run:
                    fix_batch.append((new_gender, new_conf, new_source, person_id, old_gender))

            # LLM batch pass for names that rule engine could not resolve
            llm_failed_records = []
            if pending_llm:
                logger.info(f"   🤖 LLM batch pass for {len(pending_llm)} unresolved names …")
                bs = self.llm_gender_batch_size
                for chunk_start in range(0, len(pending_llm), bs):
                    chunk = pending_llm[chunk_start: chunk_start + bs]
                    name_list = [inf_name for _, inf_name, _ in chunk]
                    llm_results = self._infer_gender_llm_batch(name_list)
                    for person_id, inf_name, old_gender in chunk:
                        if inf_name not in llm_results:
                            llm_failed_records.append((person_id, inf_name, old_gender))
                            with self.stats_lock:
                                self.stats['llm_skipped'] += 1
                            continue
                        llm_gender, llm_conf = llm_results[inf_name]
                        if llm_gender == old_gender or llm_gender == 'Unknown':
                            llm_failed_records.append((person_id, inf_name, old_gender))
                            with self.stats_lock:
                                self.stats['llm_skipped'] += 1
                            continue
                        corrected += 1
                        with self.stats_lock:
                            self.stats['llm_resolved'] += 1
                        logger.info(
                            f"   🤖 {person_id} | {inf_name!r} → "
                            f"{old_gender} → {llm_gender} (llm {llm_conf:.3f})"
                        )
                        if not effective_dry_run:
                            fix_batch.append((llm_gender, llm_conf, 'llm', person_id, old_gender))

                # Queue failed records for retry with available parallel workers
                if llm_failed_records:
                    with self.llm_queue_lock:
                        self.llm_retry_queue.extend(llm_failed_records)
                    logger.warning(f"   ⏳ Queued {len(llm_failed_records)} records for LLM retry in next batch")

            if fix_batch and not effective_dry_run:
                with self.db_pool.get_connection_context() as conn:
                    cursor = conn.cursor()
                    execute_batch(
                        cursor,
                        f"""
                        UPDATE {PERSONS_TABLE}
                        SET gender = %s,
                            gender_confidence = %s,
                            gender_source = %s
                        WHERE person_id = %s
                          AND gender = %s
                          AND (gender_source IS NULL
                               OR gender_source NOT IN ('api', 'invalid_name'))
                        """,
                        fix_batch,
                        page_size=500,
                    )
                    conn.commit()
                logger.info(f"✅ Corrected {corrected} records in DB")
            elif effective_dry_run:
                logger.info(f"🧪 Dry-run: would correct {corrected} records")
            else:
                logger.info("✅ No corrections needed")

        finally:
            self.person_gender_infer_on_unknown = old_infer_flag

        return corrected

    def get_last_processed_date(self) -> Optional[datetime]:
        """
        Get the last processed date from persons table.
        Returns max(date_created, date_modified) or None if table is empty.
        """
        try:
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                # Check if table has any data
                cursor.execute(f"SELECT COUNT(*) FROM {PERSONS_TABLE}")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    # New database, return None to process all
                    logger.info("📊 Persons table is empty, will process all person_ids from accused table")
                    return None
                
                # Table has data, get max of date_created and date_modified
                # Only consider dates >= 2022-01-01 to avoid processing very old data
                MIN_START_DATE = datetime(2022, 1, 1, tzinfo=IST_OFFSET)
                
                cursor.execute(f"""
                    SELECT GREATEST(
                        COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                        COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                    ) as max_date
                    FROM {PERSONS_TABLE}
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    max_date = result[0]
                    # Convert to IST timezone if needed
                    if isinstance(max_date, datetime):
                        if max_date.tzinfo is None:
                            max_date = max_date.replace(tzinfo=IST_OFFSET)
                        else:
                            max_date = max_date.astimezone(IST_OFFSET)
                        
                        # Ensure we never go before 2022-01-01
                        if max_date < MIN_START_DATE:
                            logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before 2022-01-01, using 2022-01-01")
                            return MIN_START_DATE
                        
                        logger.info(f"📊 Persons table has data, last processed date: {max_date.isoformat()}")
                        return max_date
                
                # Fallback to start date
                logger.warning("⚠️  Could not determine max date, using 2022-01-01")
                return MIN_START_DATE
            
        except Exception as e:
            logger.error(f"❌ Error getting last processed date: {e}")
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return datetime(2022, 1, 1, tzinfo=IST_OFFSET)
    
    def get_person_ids(self) -> List[str]:
        """
        Get person IDs from accused records that were recently added/updated.
        Only processes person_ids from accused records where date_created or date_modified
        is after the last processed date from persons table.
        """
        # Get last processed date from persons table
        last_date = self.get_last_processed_date()
        
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            if last_date is None:
                # First run - process all person_ids from accused table
                logger.info("🔄 First run: Processing all person_ids from accused table")
                cursor.execute(f"""
                    SELECT DISTINCT person_id 
                    FROM {ACCUSED_TABLE} 
                    WHERE person_id IS NOT NULL
                """)
            else:
                # Incremental run - process person_ids from:
                #   1. Accused records updated after last_date (incremental)
                #   2. Stub persons (name IS NULL) — created by accused ETL but never populated,
                #      regardless of accused.date_created/date_modified (which may be NULL or old)
                logger.info(f"🔄 Incremental run: Processing person_ids from accused records updated after {last_date.isoformat()} + any unpopulated stubs")
                cursor.execute(f"""
                    SELECT DISTINCT a.person_id 
                    FROM {ACCUSED_TABLE} a
                    LEFT JOIN {PERSONS_TABLE} p ON a.person_id = p.person_id
                    WHERE a.person_id IS NOT NULL
                    AND (
                        -- Stub person: row exists but core identity fields are all NULL
                        (p.person_id IS NOT NULL AND p.name IS NULL AND p.gender IS NULL
                         AND p.date_of_birth IS NULL AND p.nationality IS NULL)
                        -- Person row doesn't exist at all yet
                        OR p.person_id IS NULL
                        -- Accused record updated after last persons run
                        OR a.date_created >= %s 
                        OR a.date_modified >= %s
                    )
                """, (last_date, last_date))
            
            rows = [r[0] for r in cursor.fetchall()]
        
        with self.stats_lock:
            self.stats['person_ids'] = len(rows)
        
        if last_date is None:
            logger.info(f"📊 Found {len(rows)} person_ids to process (first run - all records)")
        else:
            logger.info(f"📊 Found {len(rows)} person_ids to process (incremental - updated after {last_date.isoformat()} + stubs)")
        
        return rows

    def get_person_ids_for_window(self, from_date: str, to_date: str) -> List[str]:
        """
        Get distinct person IDs from accused records changed in a date window.
        Date window is inclusive and uses accused.date_created/date_modified.
        """
        from_dt = parse_iso_date(from_date)
        to_dt = parse_iso_date(to_date)

        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT DISTINCT a.person_id
                FROM {ACCUSED_TABLE} a
                WHERE a.person_id IS NOT NULL
                  AND (
                    (a.date_created IS NOT NULL AND a.date_created >= %s AND a.date_created <= %s)
                    OR
                    (a.date_modified IS NOT NULL AND a.date_modified >= %s AND a.date_modified <= %s)
                  )
            """, (from_dt, to_dt, from_dt, to_dt))

            return [r[0] for r in cursor.fetchall()]

    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """Generate YYYY-MM-DD window ranges in chunks with overlap."""
        date_ranges = []
        current_date = parse_iso_date(start_date).date()
        end = parse_iso_date(end_date).date()

        while current_date <= end:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            if chunk_end > end:
                chunk_end = end

            date_ranges.append((
                current_date.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))

            next_start = chunk_end - timedelta(days=overlap_days - 1)
            if chunk_end >= end:
                break
            current_date = next_start

        return date_ranges

    def fetch_person_api(self, person_id: str, from_date: str, to_date: str) -> Optional[Dict]:
        url = f"{API_CONFIG['base_url']}/person-details/{person_id}"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {'x-api-key': API_CONFIG['api_key']}
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching person details for {person_id} (Attempt {attempt + 1})")
                resp = requests.get(url, params=params, headers=headers, timeout=API_CONFIG['timeout'])
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('status') and data.get('data'):
                        with self.stats_lock:
                            self.stats['api_calls'] += 1
                        return data['data']
                    else:
                        # API returned 200 but no valid data
                        logger.warning(f"API returned 200 but no valid data for person {person_id}")
                        with self.stats_lock:
                            self.stats['no_data'] += 1
                        return None
                elif resp.status_code == 404:
                    # Person not found - don't retry
                    logger.debug(f"Person {person_id} not found (404)")
                    with self.stats_lock:
                        self.stats['no_data'] += 1
                    return None
                elif resp.status_code == 400:
                    # Specific logic for 400: Bad Request (Likely dead/expunged ID) - skip instantly
                    logger.error(f"API rejected person_id {person_id} (400 Bad Request). Skipping.")
                    with self.stats_lock:
                        self.stats['no_data'] += 1
                    return None
                else:
                    # Other status codes - retry
                    logger.warning(f"API returned status code {resp.status_code}... waiting 60s before retrying")
                    if attempt < API_CONFIG['max_retries'] - 1:
                        time.sleep(60)
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout for person {person_id}, retrying... (Attempt {attempt + 1}/{API_CONFIG['max_retries']})")
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(3)
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"API connection error for person {person_id}, retrying... (Attempt {attempt + 1}/{API_CONFIG['max_retries']}): {e}")
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(3)
            except Exception as e:
                logger.error(f"API error for person {person_id}: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    with self.stats_lock:
                        self.stats['failed_api_calls'] += 1
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch person {person_id} after {API_CONFIG['max_retries']} attempts")
        with self.stats_lock:
            self.stats['failed_api_calls'] += 1
        return None

    def truncate_string(self, value: Optional[str], max_length: int = 100, field_name: str = "") -> Optional[str]:
        """Truncate string to max_length, logging if truncation occurs. Empty strings become None."""
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == '':
            return None
        if not isinstance(value, str):
            return str(value)[:max_length] if len(str(value)) > max_length else str(value)
        if len(value) > max_length:
            logger.warning(f"⚠️  Truncating {field_name} from {len(value)} to {max_length} chars for person {getattr(self, '_current_person_id', 'unknown')}")
            return value[:max_length]
        return value

    def upsert_person(self, d: Dict, table_columns: Set[str], conn, cursor):
        p = d or {}
        personal = p.get('PERSONAL_DETAILS') or {}
        present = p.get('PRESENT_ADDRESS') or {}
        permanent = p.get('PERMANENT_ADDRESS') or {}
        contact = p.get('CONTACT_DETAILS') or {}
        
        # Store person_id for truncation logging
        person_id = p.get('PERSON_ID')
        self._current_person_id = person_id
        
        # Extract date fields from API (API dates only, NULL if not provided - no current timestamps)
        date_created = p.get('DATE_CREATED') or None
        date_modified = p.get('DATE_MODIFIED') or None

        # Normalize age to integer or None so we never cast strings like '25' with NULLIF
        raw_age = personal.get('AGE')
        age_value = None
        try:
            if raw_age is None:
                age_value = None
            elif isinstance(raw_age, (int, float)):
                age_value = int(raw_age)
            elif isinstance(raw_age, str):
                s = raw_age.strip()
                age_value = int(s) if s.isdigit() else None
            else:
                age_value = None
        except Exception:
            age_value = None

        api_full_name = personal.get('FULL_NAME')
        raw_full_name_value = self.truncate_string(api_full_name, 500, 'raw_full_name')
        clean_full_name = self._normalize_person_name(api_full_name, personal)
        clean_full_name = self.truncate_string(clean_full_name, 500, 'full_name')

        # Build the richest possible name string for gender inference — this may
        # be wider than clean_full_name (which only stores FULL_NAME / NAME+SURNAME).
        # It is NOT written to the DB; it is only used as input to _resolve_gender.
        inference_name = self._build_inference_name(personal)

        resolved_gender, gender_confidence, gender_source = self._resolve_gender(inference_name, personal.get('GENDER'))
        resolved_gender = self.truncate_string(resolved_gender, 20, 'gender')
        gender_source = self.truncate_string(gender_source, 20, 'gender_source')
        normalized_phone_numbers = self._normalize_phone_numbers(contact.get('PHONE_NUMBER'))
        primary_phone = self.truncate_string(normalized_phone_numbers[0], 20, 'phone_number') if normalized_phone_numbers else None
        phone_numbers_value = self.truncate_string(' | '.join(normalized_phone_numbers), 500, 'phone_numbers') if normalized_phone_numbers else None

        try:
            if self.person_gender_dry_run:
                self.handle_dry_run(
                    person_id=person_id,
                    clean_full_name=clean_full_name,
                    raw_full_name_value=raw_full_name_value,
                    resolved_gender=resolved_gender,
                    gender_confidence=gender_confidence,
                    gender_source=gender_source,
                    phone_numbers_value=phone_numbers_value,
                    table_columns=table_columns,
                    cursor=cursor
                )
                return

            cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (p.get('PERSON_ID'),))
            exists = cursor.fetchone() is not None

            if exists:
                # Use COALESCE so existing non-NULL values are never overwritten by API nulls
                cursor.execute(
                    f"""
                    UPDATE {PERSONS_TABLE} SET
                        name=COALESCE(%s, name),
                        surname=COALESCE(%s, surname),
                        alias=COALESCE(%s, alias),
                        full_name=COALESCE(%s, full_name),
                        relation_type=COALESCE(%s, relation_type),
                        relative_name=COALESCE(%s, relative_name),
                        gender=COALESCE(%s, gender),
                        is_died=COALESCE(%s, is_died),
                        date_of_birth=COALESCE(NULLIF(%s,'')::date, date_of_birth),
                        age=COALESCE(%s, age),
                        occupation=COALESCE(%s, occupation),
                        education_qualification=COALESCE(%s, education_qualification),
                        caste=COALESCE(%s, caste),
                        sub_caste=COALESCE(%s, sub_caste),
                        religion=COALESCE(%s, religion),
                        nationality=COALESCE(%s, nationality),
                        designation=COALESCE(%s, designation),
                        place_of_work=COALESCE(%s, place_of_work),
                        present_house_no=COALESCE(%s, present_house_no),
                        present_street_road_no=COALESCE(%s, present_street_road_no),
                        present_ward_colony=COALESCE(%s, present_ward_colony),
                        present_landmark_milestone=COALESCE(%s, present_landmark_milestone),
                        present_locality_village=COALESCE(%s, present_locality_village),
                        present_area_mandal=CASE
                            WHEN NULLIF(TRIM(present_area_mandal), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), present_area_mandal)
                            ELSE present_area_mandal
                        END,
                        present_district=CASE
                            WHEN NULLIF(TRIM(present_district), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), present_district)
                            ELSE present_district
                        END,
                        present_state_ut=CASE
                            WHEN NULLIF(TRIM(present_state_ut), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), present_state_ut)
                            ELSE present_state_ut
                        END,
                        present_country=CASE
                            WHEN NULLIF(TRIM(present_country), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), present_country)
                            ELSE present_country
                        END,
                        present_residency_type=COALESCE(%s, present_residency_type),
                        present_pin_code=COALESCE(%s, present_pin_code),
                        present_jurisdiction_ps=COALESCE(%s, present_jurisdiction_ps),
                        permanent_house_no=COALESCE(%s, permanent_house_no),
                        permanent_street_road_no=COALESCE(%s, permanent_street_road_no),
                        permanent_ward_colony=COALESCE(%s, permanent_ward_colony),
                        permanent_landmark_milestone=COALESCE(%s, permanent_landmark_milestone),
                        permanent_locality_village=COALESCE(%s, permanent_locality_village),
                        permanent_area_mandal=CASE
                            WHEN NULLIF(TRIM(permanent_area_mandal), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), permanent_area_mandal)
                            ELSE permanent_area_mandal
                        END,
                        permanent_district=CASE
                            WHEN NULLIF(TRIM(permanent_district), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), permanent_district)
                            ELSE permanent_district
                        END,
                        permanent_state_ut=CASE
                            WHEN NULLIF(TRIM(permanent_state_ut), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), permanent_state_ut)
                            ELSE permanent_state_ut
                        END,
                        permanent_country=CASE
                            WHEN NULLIF(TRIM(permanent_country), '') IS NULL
                                THEN COALESCE(NULLIF(TRIM(%s), ''), permanent_country)
                            ELSE permanent_country
                        END,
                        permanent_residency_type=COALESCE(%s, permanent_residency_type),
                        permanent_pin_code=COALESCE(%s, permanent_pin_code),
                        permanent_jurisdiction_ps=COALESCE(%s, permanent_jurisdiction_ps),
                        phone_number=COALESCE(%s, phone_number),
                        country_code=COALESCE(%s, country_code),
                        email_id=COALESCE(%s, email_id),
                        date_created=COALESCE(%s, date_created),
                        date_modified=COALESCE(%s, date_modified)
                    WHERE person_id=%s
                    """,
                    (
                        self.truncate_string(personal.get('NAME'), 255, 'name'),
                        self.truncate_string(personal.get('SURNAME'), 255, 'surname'),
                        self.truncate_string(personal.get('ALIAS'), 255, 'alias'),
                        clean_full_name,
                        self.truncate_string(personal.get('RELATION_TYPE'), 50, 'relation_type'),
                        self.truncate_string(personal.get('RELATIVE_NAME'), 255, 'relative_name'),
                        resolved_gender,
                        personal.get('IS_DIED'),
                        personal.get('DATE_OF_BIRTH'), age_value,
                        self.truncate_string(personal.get('OCCUPATION'), 255, 'occupation'),
                        self.truncate_string(personal.get('EDUCATION_QUALIFICATION'), 255, 'education_qualification'),
                        self.truncate_string(personal.get('CASTE'), 100, 'caste'),
                        self.truncate_string(personal.get('SUB_CASTE'), 100, 'sub_caste'),
                        self.truncate_string(personal.get('RELIGION'), 100, 'religion'),
                        self.truncate_string(personal.get('NATIONALITY'), 100, 'nationality'),
                        self.truncate_string(personal.get('DESIGNATION'), 255, 'designation'),
                        self.truncate_string(personal.get('PLACE_OF_WORK'), 500, 'place_of_work'),
                        self.truncate_string(present.get('HOUSE_NO'), 255, 'present_house_no'),
                        self.truncate_string(present.get('STREET_ROAD_NO'), 255, 'present_street_road_no'),
                        self.truncate_string(present.get('WARD_COLONY'), 255, 'present_ward_colony'),
                        self.truncate_string(present.get('LANDMARK_MILESTONE'), 255, 'present_landmark_milestone'),
                        self.truncate_string(present.get('LOCALITY_VILLAGE'), 255, 'present_locality_village'),
                        self.truncate_string(present.get('AREA_MANDAL'), 255, 'present_area_mandal'),
                        self.truncate_string(present.get('DISTRICT'), 255, 'present_district'),
                        self.truncate_string(present.get('STATE_UT'), 255, 'present_state_ut'),
                        self.truncate_string(present.get('COUNTRY'), 255, 'present_country'),
                        self.truncate_string(present.get('RESIDENCY_TYPE'), 100, 'present_residency_type'),
                        self.truncate_string(present.get('PIN_CODE'), 20, 'present_pin_code'),
                        self.truncate_string(present.get('JURISDICTION_PS'), 20, 'present_jurisdiction_ps'),
                        self.truncate_string(permanent.get('HOUSE_NO'), 255, 'permanent_house_no'),
                        self.truncate_string(permanent.get('STREET_ROAD_NO'), 255, 'permanent_street_road_no'),
                        self.truncate_string(permanent.get('WARD_COLONY'), 255, 'permanent_ward_colony'),
                        self.truncate_string(permanent.get('LANDMARK_MILESTONE'), 255, 'permanent_landmark_milestone'),
                        self.truncate_string(permanent.get('LOCALITY_VILLAGE'), 255, 'permanent_locality_village'),
                        self.truncate_string(permanent.get('AREA_MANDAL'), 255, 'permanent_area_mandal'),
                        self.truncate_string(permanent.get('DISTRICT'), 255, 'permanent_district'),
                        self.truncate_string(permanent.get('STATE_UT'), 255, 'permanent_state_ut'),
                        self.truncate_string(permanent.get('COUNTRY'), 255, 'permanent_country'),
                        self.truncate_string(permanent.get('RESIDENCY_TYPE'), 100, 'permanent_residency_type'),
                        self.truncate_string(permanent.get('PIN_CODE'), 20, 'permanent_pin_code'),
                        self.truncate_string(permanent.get('JURISDICTION_PS'), 20, 'permanent_jurisdiction_ps'),
                        primary_phone,
                        self.truncate_string(contact.get('COUNTRY_CODE'), 10, 'country_code'),
                        self.truncate_string(contact.get('EMAIL_ID'), 255, 'email_id'),
                        date_created, date_modified,
                        person_id
                    )
                )

                self.apply_person_enrichment(
                    person_id=person_id,
                    raw_full_name=raw_full_name_value,
                    gender_confidence=gender_confidence,
                    gender_source=gender_source,
                    phone_numbers=phone_numbers_value,
                    table_columns=table_columns,
                    cursor=cursor
                )
                with self.stats_lock:
                    self.stats['updated'] += 1
                
                # Update any new fields that were added via schema evolution
                if table_columns:
                    self.update_new_fields(person_id, p, personal, present, permanent, contact, table_columns, cursor)
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {PERSONS_TABLE} (
                        person_id, name, surname, alias, full_name,
                        relation_type, relative_name, gender, is_died,
                        date_of_birth, age, occupation,
                        education_qualification, caste, sub_caste, religion,
                        nationality, designation, place_of_work,
                        present_house_no, present_street_road_no, present_ward_colony,
                        present_landmark_milestone, present_locality_village, present_area_mandal,
                        present_district, present_state_ut, present_country, present_residency_type,
                        present_pin_code, present_jurisdiction_ps,
                        permanent_house_no, permanent_street_road_no, permanent_ward_colony,
                        permanent_landmark_milestone, permanent_locality_village, permanent_area_mandal,
                        permanent_district, permanent_state_ut, permanent_country, permanent_residency_type,
                        permanent_pin_code, permanent_jurisdiction_ps,
                        phone_number, country_code, email_id,
                        date_created, date_modified
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        NULLIF(%s,'')::date,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s, %s
                    )
                    """,
                    (
                        person_id,
                        self.truncate_string(personal.get('NAME'), 255, 'name'),
                        self.truncate_string(personal.get('SURNAME'), 255, 'surname'),
                        self.truncate_string(personal.get('ALIAS'), 255, 'alias'),
                        clean_full_name,
                        self.truncate_string(personal.get('RELATION_TYPE'), 50, 'relation_type'),
                        self.truncate_string(personal.get('RELATIVE_NAME'), 255, 'relative_name'),
                        resolved_gender,
                        personal.get('IS_DIED'),
                        personal.get('DATE_OF_BIRTH'), age_value,
                        self.truncate_string(personal.get('OCCUPATION'), 255, 'occupation'),
                        self.truncate_string(personal.get('EDUCATION_QUALIFICATION'), 255, 'education_qualification'),
                        self.truncate_string(personal.get('CASTE'), 100, 'caste'),
                        self.truncate_string(personal.get('SUB_CASTE'), 100, 'sub_caste'),
                        self.truncate_string(personal.get('RELIGION'), 100, 'religion'),
                        self.truncate_string(personal.get('NATIONALITY'), 100, 'nationality'),
                        self.truncate_string(personal.get('DESIGNATION'), 255, 'designation'),
                        self.truncate_string(personal.get('PLACE_OF_WORK'), 500, 'place_of_work'),
                        self.truncate_string(present.get('HOUSE_NO'), 255, 'present_house_no'),
                        self.truncate_string(present.get('STREET_ROAD_NO'), 255, 'present_street_road_no'),
                        self.truncate_string(present.get('WARD_COLONY'), 255, 'present_ward_colony'),
                        self.truncate_string(present.get('LANDMARK_MILESTONE'), 255, 'present_landmark_milestone'),
                        self.truncate_string(present.get('LOCALITY_VILLAGE'), 255, 'present_locality_village'),
                        self.truncate_string(present.get('AREA_MANDAL'), 255, 'present_area_mandal'),
                        self.truncate_string(present.get('DISTRICT'), 255, 'present_district'),
                        self.truncate_string(present.get('STATE_UT'), 255, 'present_state_ut'),
                        self.truncate_string(present.get('COUNTRY'), 255, 'present_country'),
                        self.truncate_string(present.get('RESIDENCY_TYPE'), 100, 'present_residency_type'),
                        self.truncate_string(present.get('PIN_CODE'), 20, 'present_pin_code'),
                        self.truncate_string(present.get('JURISDICTION_PS'), 20, 'present_jurisdiction_ps'),
                        self.truncate_string(permanent.get('HOUSE_NO'), 255, 'permanent_house_no'),
                        self.truncate_string(permanent.get('STREET_ROAD_NO'), 255, 'permanent_street_road_no'),
                        self.truncate_string(permanent.get('WARD_COLONY'), 255, 'permanent_ward_colony'),
                        self.truncate_string(permanent.get('LANDMARK_MILESTONE'), 255, 'permanent_landmark_milestone'),
                        self.truncate_string(permanent.get('LOCALITY_VILLAGE'), 255, 'permanent_locality_village'),
                        self.truncate_string(permanent.get('AREA_MANDAL'), 255, 'permanent_area_mandal'),
                        self.truncate_string(permanent.get('DISTRICT'), 255, 'permanent_district'),
                        self.truncate_string(permanent.get('STATE_UT'), 255, 'permanent_state_ut'),
                        self.truncate_string(permanent.get('COUNTRY'), 255, 'permanent_country'),
                        self.truncate_string(permanent.get('RESIDENCY_TYPE'), 100, 'permanent_residency_type'),
                        self.truncate_string(permanent.get('PIN_CODE'), 20, 'permanent_pin_code'),
                        self.truncate_string(permanent.get('JURISDICTION_PS'), 20, 'permanent_jurisdiction_ps'),
                        primary_phone,
                        self.truncate_string(contact.get('COUNTRY_CODE'), 10, 'country_code'),
                        self.truncate_string(contact.get('EMAIL_ID'), 255, 'email_id'),
                        date_created, date_modified
                    )
                )

                self.apply_person_enrichment(
                    person_id=person_id,
                    raw_full_name=raw_full_name_value,
                    gender_confidence=gender_confidence,
                    gender_source=gender_source,
                    phone_numbers=phone_numbers_value,
                    table_columns=table_columns,
                    cursor=cursor
                )
                with self.stats_lock:
                    self.stats['inserted'] += 1
                
                # Update any new fields that were added via schema evolution (for new inserts, new fields will be NULL initially)
                # They'll be updated when the person is reprocessed in future runs
                if table_columns:
                    self.update_new_fields(person_id, p, personal, present, permanent, contact, table_columns, cursor)

            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error upserting person {p.get('PERSON_ID')}: {e}")
            with self.stats_lock:
                self.stats['failed'] += 1
                self.stats['errors'] += 1

    def run(self):
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Person Details API")
        logger.info("=" * 80)
        logger.info("ℹ️  This process incrementally fetches person details for PERSON_IDs")
        logger.info("ℹ️  Only processes person_ids from recently updated accused records")
        if self.person_gender_dry_run:
            logger.warning("⚠️  DRY RUN MODE ENABLED: writes are disabled and would-change rows are logged only")
            if self.dry_run_log_file:
                logger.warning(f"⚠️  Dry-run change log: {self.dry_run_log_file}")
        logger.info("=" * 80)
        
        if not self.connect_db():
            return False
        try:
            self.ensure_run_state_table()

            # Get table columns for schema evolution
            table_columns = self.get_table_columns(PERSONS_TABLE)
            if self.person_gender_dry_run:
                missing = [c for c in ('raw_full_name', 'gender_confidence', 'gender_source', 'phone_numbers') if c not in table_columns]
                if missing:
                    logger.warning(
                        f"⚠️  Dry-run detected missing enrichment columns {missing}. "
                        "Preview output will omit comparisons for missing columns."
                    )
            else:
                table_columns = self.ensure_person_enrichment_columns(table_columns)
            logger.debug(f"Existing table columns: {sorted(table_columns)}")

            # ── Learn from existing API-validated data ─────────────────────────
            # Extends rule_map and prefix_tokens with patterns mined from names
            # that the API already resolved.  Must run BEFORE the correction pass
            # so the learned patterns are available during re-evaluation.
            self._load_learned_gender_rules()
            # ──────────────────────────────────────────────────────────────────

            # ── One-time remediation pass ──────────────────────────────────────
            # Re-evaluate any rows previously written with the old heuristic and
            # correct gender misclassifications. Idempotent — safe every run.
            if not self.person_gender_dry_run:
                self.correct_heuristic_gender_records()
            else:
                self.correct_heuristic_gender_records(dry_run=True)
            # ──────────────────────────────────────────────────────────────────
            
            last_date = self.get_last_processed_date()
            checkpoint_date = self.get_run_checkpoint('persons')
            resume_boundary = last_date
            if checkpoint_date:
                checkpoint_dt = parse_iso_date(checkpoint_date) if isinstance(checkpoint_date, str) else checkpoint_date
                if resume_boundary is None or checkpoint_dt > resume_boundary:
                    resume_boundary = checkpoint_dt

            effective_start_date = resume_boundary.isoformat() if resume_boundary else '2022-01-01T00:00:00+05:30'
            calculated_end_date = get_yesterday_end_ist()

            chunk_days = int(os.environ.get('CHUNK_DAYS', '5'))
            overlap_days = int(os.environ.get('CHUNK_OVERLAP_DAYS', '1'))

            date_ranges = self.generate_date_ranges(
                effective_start_date,
                calculated_end_date,
                chunk_days,
                overlap_days
            )

            logger.info(f"Date Range: {effective_start_date} to {calculated_end_date}")
            logger.info(f"Chunk Size: {chunk_days} days (overlap: {overlap_days} day(s))")

            if not date_ranges:
                logger.info("ℹ️  No date ranges to process. Nothing to do.")
                return True

            processed_person_ids = set()
            
            batch_size = 100  # Log batch stats every 100 records
            first_record_processed = False
            
            def process_person(pid, table_columns, from_date, to_date):
                nonlocal first_record_processed
                data = self.fetch_person_api(pid, from_date, to_date)
                if data:
                    # Check for schema evolution on first record with minimal lock contention
                    if not first_record_processed and table_columns is not None and not self.person_gender_dry_run:
                        new_fields = self.detect_new_fields(data, table_columns)
                        if new_fields:
                            with self.schema_lock:
                                if not first_record_processed:
                                    logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                                    for api_field, db_column in new_fields.items():
                                        if self.add_column_to_table(db_column):
                                            table_columns.add(db_column)
                                    self.update_existing_records_with_new_fields(new_fields)
                                    first_record_processed = True
                        else:
                            first_record_processed = True
                    elif not first_record_processed:
                        first_record_processed = True
                    
                    db_retry_attempts = int(os.environ.get('DB_WRITE_MAX_RETRIES', '3'))
                    for db_attempt in range(db_retry_attempts):
                        try:
                            with self.db_pool.get_connection_context() as conn:
                                cursor = conn.cursor()
                                self.upsert_person(data, table_columns, conn, cursor)
                            break
                        except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                            if db_attempt == db_retry_attempts - 1:
                                raise
                            logger.warning(
                                f"Transient DB error for person {pid}, retrying "
                                f"({db_attempt + 1}/{db_retry_attempts}): {db_err}"
                            )
                            time.sleep(2 ** db_attempt)
                else:
                    with self.stats_lock:
                        self.stats['no_data'] += 1

            requested_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
            max_workers = compute_safe_workers(self.db_pool, requested_workers)

            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                window_person_ids = self.get_person_ids_for_window(from_date, to_date)
                window_person_ids = [pid for pid in window_person_ids if pid not in processed_person_ids]

                if not window_person_ids:
                    continue

                for pid in window_person_ids:
                    processed_person_ids.add(pid)

                with self.stats_lock:
                    self.stats['person_ids'] += len(window_person_ids)

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(process_person, pid, table_columns, from_date, to_date): pid
                        for pid in window_person_ids
                    }

                    with tqdm(total=len(window_person_ids), desc=f"Persons {from_date} to {to_date}", unit="person", leave=False) as pbar:
                        for idx, future in enumerate(as_completed(futures), 1):
                            pid = futures[future]
                            try:
                                future.result()
                            except Exception as e:
                                logger.error(f"Error processing person {pid}: {e}")
                                with self.stats_lock:
                                    self.stats['failed'] += 1
                                    self.stats['errors'] += 1

                            pbar.update(1)
                            if idx % batch_size == 0:
                                with self.stats_lock:
                                    logger.info(
                                        f"   📊 Progress ({from_date} to {to_date}): {idx}/{len(window_person_ids)} - "
                                        f"Inserted: {self.stats['inserted']}, Updated: {self.stats['updated']}, "
                                        f"Failed: {self.stats['failed']}"
                                    )

            # Process queued LLM retry records with available parallel workers
            with self.llm_queue_lock:
                retry_queue = self.llm_retry_queue.copy()
                self.llm_retry_queue.clear()

            if retry_queue and self.llm_gender_enabled:
                logger.info("")
                logger.info(f"🔄 Processing {len(retry_queue)} queued LLM retry records …")

                retry_fix_batch = []
                retry_bs = self.llm_gender_batch_size
                for chunk_start in range(0, len(retry_queue), retry_bs):
                    chunk = retry_queue[chunk_start: chunk_start + retry_bs]
                    name_list = [inf_name for _, inf_name, _ in chunk]
                    llm_results = self._infer_gender_llm_batch(name_list)
                    for person_id, inf_name, old_gender in chunk:
                        if inf_name not in llm_results:
                            logger.debug(f"   ⏭️  {person_id} | {inf_name!r} — LLM still unable to resolve")
                            continue
                        llm_gender, llm_conf = llm_results[inf_name]
                        if llm_gender == old_gender or llm_gender == 'Unknown':
                            logger.debug(f"   ⏭️  {person_id} | {inf_name!r} — no gender change {old_gender} → {llm_gender}")
                            continue
                        logger.info(
                            f"   ✅ {person_id} | {inf_name!r} → "
                            f"{old_gender} → {llm_gender} (retry, llm {llm_conf:.3f})"
                        )
                        retry_fix_batch.append((llm_gender, llm_conf, 'llm', person_id, old_gender))
                        with self.stats_lock:
                            self.stats['llm_resolved'] += 1

                if retry_fix_batch:
                    with self.db_pool.get_connection_context() as conn:
                        cursor = conn.cursor()
                        execute_batch(
                            cursor,
                            f"""
                            UPDATE {PERSONS_TABLE}
                            SET gender = %s,
                                gender_confidence = %s,
                                gender_source = %s
                            WHERE person_id = %s
                              AND gender = %s
                              AND (gender_source IS NULL
                                   OR gender_source NOT IN ('api', 'invalid_name'))
                            """,
                            retry_fix_batch,
                            page_size=500,
                        )
                        conn.commit()
                    logger.info(f"✅ Resolved {len(retry_fix_batch)} retried records via LLM")

            # Get database counts
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {PERSONS_TABLE}")
                db_persons_count = cursor.fetchone()[0]
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"📡 API CALLS:")
            logger.info(f"  Total API Calls:         {self.stats['api_calls']}")
            logger.info(f"  Failed API Calls:         {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM DATABASE (Person IDs from Accused):")
            logger.info(f"  Person IDs to Process:    {self.stats['person_ids']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New):     {self.stats['inserted']}")
            logger.info(f"  Total Updated:            {self.stats['updated']}")
            logger.info(f"  Total No Change:          {self.stats['no_change']}")
            logger.info(f"  Total No Data (404/dead): {self.stats['no_data']}")
            logger.info(f"  Total Failed:              {self.stats['failed']}")
            logger.info(f"  Total in DB:               {db_persons_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['person_ids'] > 0:
                coverage = ((self.stats['inserted'] + self.stats['updated']) / self.stats['person_ids']) * 100
                logger.info(f"  Processed → DB Coverage: {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"🤖 LLM GENDER INFERENCE:")
            logger.info(f"  Resolved by LLM:          {self.stats['llm_resolved']}")
            logger.info(f"  Queued (unable to resolve): {self.stats['llm_skipped']}")
            logger.info(f"")
            logger.info(f"❌ Errors:                  {self.stats['errors']}")
            if self.person_gender_dry_run:
                logger.info("")
                logger.info("🧪 DRY-RUN RESULTS:")
                logger.info(f"  Would Insert:             {self.stats['dry_run_inserts']}")
                logger.info(f"  Would Update:             {self.stats['dry_run_changes']}")
                logger.info(f"  No Change:                {self.stats['dry_run_no_change']}")
            else:
                self.update_run_checkpoint('persons', calculated_end_date)
            logger.info("=" * 80)
            logger.info("✅ ETL Pipeline completed successfully!")
            return True
        except KeyboardInterrupt:
            logger.warning("\n⚠️  ETL interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ ETL failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.close_db()


def main():
    from db_pooling import PostgreSQLConnectionPool
    pool = PostgreSQLConnectionPool()
    pool.reset()

    etl = PersonsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()