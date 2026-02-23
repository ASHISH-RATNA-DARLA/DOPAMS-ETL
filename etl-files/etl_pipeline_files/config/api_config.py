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
        """Parse api-ref.txt file"""
        if not self.config_file.exists():
            raise FileNotFoundError(f"API config file not found: {self.config_file}")
        
        with open(self.config_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract API1 base URL (port 3000) - from crimes/persons/property/interrogation APIs
        api1_pattern = r"http://([\d\.]+):3000/api/DOPAMS"
        api1_match = re.search(api1_pattern, content)
        if api1_match:
            host = api1_match.group(1)
            self.base_url = f"http://{host}:3000/api/DOPAMS"
        
        # Extract API2 base URL (port 3001) - from mo_seizures/chargesheets/fsl_case_property APIs
        api2_pattern = r"http://([\d\.]+):3001/api/DOPAMS"
        api2_match = re.search(api2_pattern, content)
        if api2_match:
            host = api2_match.group(1)
            self.api2_base_url = f"http://{host}:3001/api/DOPAMS"
        else:
            # Fallback to env vars if not found in file
            api2_host = os.getenv('API2_URL', '103.164.200.184')
            api2_port = os.getenv('API2_PORT', '3001')
            self.api2_base_url = f"http://{api2_host}:{api2_port}/api/DOPAMS"
        
        # Extract API key
        api_key_pattern = r"x-api-key:\s*([a-f0-9\-]+)"
        api_key_match = re.search(api_key_pattern, content)
        if api_key_match:
            self.api_key = api_key_match.group(1)
        
        # Extract endpoints - API1 (port 3000)
        # Crimes API
        if 'crimes api:' in content.lower():
            self.endpoints['crimes'] = '/crimes'
            self.endpoint_api_map['crimes'] = 1
        
        # Persons API
        if 'persons api:' in content.lower() or 'person-details' in content:
            self.endpoints['persons'] = '/person-details'
            self.endpoint_api_map['persons'] = 1
        
        # Property API
        if 'property api:' in content.lower() or 'property-details' in content:
            self.endpoints['property'] = '/property-details'
            self.endpoint_api_map['property'] = 1
        
        # Interrogation API
        if 'interrogation api:' in content.lower() or 'interrogation-reports' in content:
            self.endpoints['interrogation'] = '/interrogation-reports/v1/'
            self.endpoint_api_map['interrogation'] = 1
        
        # Extract endpoints - API2 (port 3001)
        # MO Seizures API
        if 'mo_seizures api:' in content.lower() or 'mo-seizures' in content:
            self.endpoints['mo_seizures'] = '/mo-seizures'
            self.endpoint_api_map['mo_seizures'] = 2
        
        # Chargesheets API
        if 'chargesheets' in content.lower():
            self.endpoints['chargesheets'] = '/chargesheets'
            self.endpoint_api_map['chargesheets'] = 2
        
        # FSL Case Property API
        if 'fsl_case_property' in content.lower() or 'case-property' in content:
            self.endpoints['fsl_case_property'] = '/case-property'
            self.endpoint_api_map['fsl_case_property'] = 2
        
        # Validate
        if not self.base_url:
            raise ValueError("Could not extract API1 base URL from api-ref.txt")
        if not self.api2_base_url:
            raise ValueError("Could not extract API2 base URL from api-ref.txt or env vars")
        if not self.api_key:
            raise ValueError("Could not extract API key from api-ref.txt")
        if not self.endpoints:
            raise ValueError("Could not extract API endpoints from api-ref.txt")
    
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


