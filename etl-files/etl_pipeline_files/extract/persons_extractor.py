"""
Persons API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class PersonsExtractor(BaseExtractor):
    """Extract file references from Persons API"""
    
    def extract_files(self, data: Dict[str, Any], person_id: str, api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract IDENTITY_DETAILS and MEDIA from person API response.
        
        Args:
            data: API response data
            person_id: Person ID
            api_date: Date (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning(f"Invalid API response format for person {person_id}")
            return files
        
        person_data = data.get('data', {})
        if not isinstance(person_data, dict):
            self.logger.warning(f"Person {person_id}: data is not a dict")
            return files
        
        # Extract date from record (prefer DATE_CREATED, fallback to DATE_MODIFIED, then api_date)
        # IMPORTANT: Always ensure record_date is not None - use api_date as final fallback
        record_date = None
        if person_data.get('DATE_CREATED'):
            record_date = person_data.get('DATE_CREATED')
        elif person_data.get('DATE_MODIFIED'):
            record_date = person_data.get('DATE_MODIFIED')
        elif person_data.get('CREATED_DATE'):
            record_date = person_data.get('CREATED_DATE')
        elif person_data.get('MODIFIED_DATE'):
            record_date = person_data.get('MODIFIED_DATE')
        
        # CRITICAL: Always use api_date as fallback, even if None (will be handled by loader)
        # But prefer api_date over None if available
        if not record_date:
            record_date = api_date
        
        # If still None, log warning but continue - loader will use current time as last resort
        if not record_date:
            self.logger.warning(f"⚠️ Person {person_id}: No date found in record and api_date is None - will use current time")
        
        # Extract IDENTITY_DETAILS
        identity_details = person_data.get('IDENTITY_DETAILS', None)
        if identity_details is None:
            self.logger.debug(f"Person {person_id}: IDENTITY_DETAILS field not present")
        elif isinstance(identity_details, list):
            if len(identity_details) == 0:
                # Empty array - insert record with file_id=NULL
                files.append({
                    'source_type': 'person',
                    'source_field': 'IDENTITY_DETAILS',
                    'parent_id': person_id,
                    'file_id': None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                self.logger.debug(f"Person {person_id}: IDENTITY_DETAILS is empty array")
            else:
                # Process each identity detail
                for idx, identity in enumerate(identity_details):
                    if isinstance(identity, dict):
                        file_id = identity.get('FILE_ID')
                        identity_type = identity.get('TYPE')
                        identity_number = identity.get('NUMBER')
                        
                        files.append({
                            'source_type': 'person',
                            'source_field': 'IDENTITY_DETAILS',
                            'parent_id': person_id,
                            'file_id': file_id if file_id else None,
                            'file_index': idx,
                            'identity_type': identity_type,
                            'identity_number': identity_number,
                            'api_date': record_date
                        })
                        
                        if not file_id:
                            self.logger.debug(f"Person {person_id}: IDENTITY_DETAILS[{idx}] has no FILE_ID")
        
        # Extract MEDIA
        media = person_data.get('MEDIA', None)
        if media is None:
            self.logger.debug(f"Person {person_id}: MEDIA field not present")
        elif isinstance(media, list):
            if len(media) == 0:
                # Empty array - insert record with file_id=NULL
                files.append({
                    'source_type': 'person',
                    'source_field': 'MEDIA',
                    'parent_id': person_id,
                    'file_id': None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                self.logger.debug(f"Person {person_id}: MEDIA is empty array")
            else:
                # Process each media item
                for idx, media_id in enumerate(media):
                    files.append({
                        'source_type': 'person',
                        'source_field': 'MEDIA',
                        'parent_id': person_id,
                        'file_id': media_id if media_id else None,
                        'file_index': idx,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    
                    if not media_id:
                        self.logger.debug(f"Person {person_id}: MEDIA[{idx}] is null/empty")
        
        return files


