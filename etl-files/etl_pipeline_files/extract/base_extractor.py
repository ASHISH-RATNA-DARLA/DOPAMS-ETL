"""
Base extractor class for API data extraction
"""
import requests
import time
from typing import Dict, List, Any
import logging


class BaseExtractor:
    """Base class for API extractors"""
    
    def __init__(self, api_config, logger=None):
        """
        Initialize extractor.
        
        Args:
            api_config: APIConfig instance
            logger: Logger instance
        """
        self.api_config = api_config
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update(api_config.get_headers())
    
    def fetch_data(self, url: str, max_retries: int = 3, retry_delay: int = 5) -> Dict[str, Any]:
        """
        Fetch data from API with retry logic.
        
        Args:
            url: API URL
            max_retries: Maximum number of retries
            retry_delay: Delay between retries (seconds)
        
        Returns:
            dict: API response data
        
        Raises:
            Exception: If all retries fail
        """
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Fetching: {url} (attempt {attempt + 1}/{max_retries})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.HTTPError as e:
                # Check if it's a client error (4xx) - don't retry these
                if e.response is not None and 400 <= e.response.status_code < 500:
                    # 400-499 are client errors (bad request, not found, etc.) - don't retry
                    self.logger.debug(f"Client error (HTTP {e.response.status_code}): {url} - skipping retry")
                    raise
                # 5xx server errors - retry
                if attempt < max_retries - 1:
                    self.logger.warning(f"Server error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"Failed to fetch data after {max_retries} attempts: {e}")
                    raise
            
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                # Network errors - retry
                if attempt < max_retries - 1:
                    self.logger.warning(f"Network error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"Failed to fetch data after {max_retries} attempts: {e}")
                    raise
            
            except requests.exceptions.RequestException as e:
                # Other request exceptions - retry
                if attempt < max_retries - 1:
                    self.logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"Failed to fetch data after {max_retries} attempts: {e}")
                    raise
        
        raise Exception(f"Failed to fetch data from {url} after {max_retries} attempts")
    
    def extract_files(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract file records from API response.
        Must be implemented by subclasses.
        
        Args:
            data: API response data
        
        Returns:
            List of file records
        """
        raise NotImplementedError("Subclasses must implement extract_files method")


