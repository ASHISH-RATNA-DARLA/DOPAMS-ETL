"""
API configuration parser from api-ref.txt
"""
import re
import os
from pathlib import Path


class APIConfig:
    """Parse and store API configuration from api-ref.txt"""
    
    def __init__(self, config_file='api-ref.txt'):
        """
        Initialize API configuration.
        
        Args:
            config_file: Path to api-ref.txt file
        """
        self.config_file = Path(config_file)
        self.base_url = None  # API1 (port 3000) - default for backward compatibility
        self.api2_base_url = None  # API2 (port 3001) - for new APIs
        self.api_key = None
        self.endpoints = {}
        self.endpoint_api_map = {}  # Maps endpoint to API version (1 or 2)
        self._parse_config()
    
    def _parse_config(self):
        """Load configuration primarily from .env and use api-ref.txt as a fallback mapping"""
        # Ensure .env is loaded (traverse up if necessary)
        try:
            from dotenv import load_dotenv, find_dotenv
            load_dotenv(find_dotenv(), override=True)
        except ImportError:
            pass

        # Primary Configuration: Environment Variables
        self.api_key = os.getenv('DOPAMAS_API_KEY') or os.getenv('API_KEY')
        
        # Load Base URLs from env (fallback to default ports on localhost if completely missing)
        env_api1 = os.getenv('DOPAMAS_API_URL')
        self.base_url = env_api1.rstrip('/') if env_api1 else "http://YOUR_API_HOST:3000/api/DOPAMS"
        
        api2_host = os.getenv('API2_URL')
        api2_port = os.getenv('API2_PORT')
        if api2_host and api2_port:
            self.api2_base_url = f"http://{api2_host}:{api2_port}/api/DOPAMS"
        else:
            self.api2_base_url = "http://YOUR_API_HOST:3001/api/DOPAMS"
            
        # Hardcode the known endpoints since they are standard
        self.endpoints = {
            'crimes': '/crimes',
            'persons': '/person-details',
            'property': '/property-details',
            'interrogation': '/interrogation-reports/v1/',
            'mo_seizures': '/mo-seizures',
            'chargesheets': '/chargesheets',
            'fsl_case_property': '/case-property'
        }
        
        self.endpoint_api_map = {
            'crimes': 1,
            'persons': 1,
            'property': 1,
            'interrogation': 1,
            'mo_seizures': 2,
            'chargesheets': 2,
            'fsl_case_property': 2
        }

        # Try to extract hosts from api-ref.txt if environment variables were NOT set
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # If the env var was empty, try api-ref.txt
                if not os.getenv('DOPAMAS_API_URL'):
                    api1_match = re.search(r"http://([\w\.\-]+):3000/api/DOPAMS", content)
                    if api1_match:
                        self.base_url = f"http://{api1_match.group(1)}:3000/api/DOPAMS"

                if not (api2_host and api2_port):
                    api2_match = re.search(r"http://([\w\.\-]+):3001/api/DOPAMS", content)
                    if api2_match:
                        self.api2_base_url = f"http://{api2_match.group(1)}:3001/api/DOPAMS"

                if not self.api_key:
                    api_key_match = re.search(r"x-api-key:\s*([\w\-]+)", content)
                    if api_key_match and api_key_match.group(1) != 'YOUR_API_KEY_HERE':
                        self.api_key = api_key_match.group(1)
            except Exception:
                pass
                
        # Final validation
        if not self.api_key:
            raise ValueError("Could not find DOPAMAS_API_KEY in .env file or api-ref.txt")
    
    def get_url(self, endpoint_name, **params):
        """
        Build full API URL with parameters.
        
        Args:
            endpoint_name: Name of endpoint (crimes, persons, property, interrogation, mo_seizures, chargesheets, fsl_case_property)
            **params: URL parameters (e.g., fromDate, toDate, person_id)
        
        Returns:
            str: Full API URL
        """
        if endpoint_name not in self.endpoints:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")
        
        endpoint = self.endpoints[endpoint_name]
        
        # Determine which API base URL to use
        api_version = self.endpoint_api_map.get(endpoint_name, 1)
        base_url = self.api2_base_url if api_version == 2 else self.base_url
        
        # Build URL
        if endpoint_name == 'persons':
            # Persons API: /person-details/{person_id}
            person_id = params.get('person_id')
            if not person_id:
                raise ValueError("person_id required for persons endpoint")
            url = f"{base_url}{endpoint}/{person_id}"
        else:
            # Date-based APIs: /endpoint?fromDate=...&toDate=...
            url = f"{base_url}{endpoint}"
            query_params = []
            if 'fromDate' in params:
                query_params.append(f"fromDate={params['fromDate']}")
            if 'toDate' in params:
                query_params.append(f"toDate={params['toDate']}")
            if query_params:
                url += "?" + "&".join(query_params)
        
        return url
    
    def get_headers(self):
        """Get API request headers"""
        return {
            'x-api-key': self.api_key
        }


