"""
MO Seizures API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class MoSeizuresExtractor(BaseExtractor):
    """Extract file references from MO Seizures API"""
    
    def extract_files(self, data: Dict[str, Any], api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract MO_MEDIA_FILE_ID from mo_seizures API response.
        
        Args:
            data: API response data
            api_date: Date from API query (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning("Invalid API response format for mo_seizures")
            return files
        
        seizures = data.get('data', [])
        if not isinstance(seizures, list):
            seizures = [seizures]
        
        for seizure in seizures:
            if not isinstance(seizure, dict):
                continue
            
            mo_seizure_id = seizure.get('MO_SEIZURE_ID')
            if not mo_seizure_id:
                self.logger.warning("MO Seizure record missing MO_SEIZURE_ID")
                continue
            
            # Extract date from record (prefer DATE_CREATED, fallback to DATE_MODIFIED, then api_date)
            # CRITICAL: Always ensure record_date is set - use api_date as final fallback
            record_date = None
            if seizure.get('DATE_CREATED'):
                record_date = seizure.get('DATE_CREATED')
            elif seizure.get('DATE_MODIFIED'):
                record_date = seizure.get('DATE_MODIFIED')
            elif seizure.get('CREATED_DATE'):
                record_date = seizure.get('CREATED_DATE')
            elif seizure.get('MODIFIED_DATE'):
                record_date = seizure.get('MODIFIED_DATE')
            
            # CRITICAL: Always use api_date as fallback (from_date from query)
            if not record_date and api_date:
                record_date = api_date
            
            # If still None, log warning - loader will handle fallback
            if not record_date:
                self.logger.warning(f"⚠️ MO Seizure {mo_seizure_id}: No date found in record and api_date is None - will use current time")
            
            mo_media_file_id = seizure.get('MO_MEDIA_FILE_ID', None)
            
            if mo_media_file_id is None:
                # Field not present or null - insert record with file_id=NULL
                files.append({
                    'source_type': 'mo_seizures',
                    'source_field': 'MO_MEDIA',
                    'parent_id': mo_seizure_id,
                    'file_id': None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                self.logger.debug(f"MO Seizure {mo_seizure_id}: MO_MEDIA_FILE_ID is null/empty")
            elif isinstance(mo_media_file_id, list):
                if len(mo_media_file_id) == 0:
                    # Empty array - insert record with file_id=NULL
                    files.append({
                        'source_type': 'mo_seizures',
                        'source_field': 'MO_MEDIA',
                        'parent_id': mo_seizure_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None
                    })
                    self.logger.debug(f"MO Seizure {mo_seizure_id}: MO_MEDIA_FILE_ID is empty array")
                else:
                    # Process each media file ID
                    for idx, file_id in enumerate(mo_media_file_id):
                        files.append({
                            'source_type': 'mo_seizures',
                            'source_field': 'MO_MEDIA',
                            'parent_id': mo_seizure_id,
                            'file_id': file_id if file_id else None,
                            'file_index': idx,
                            'identity_type': None,
                            'identity_number': None,
                            'api_date': record_date
                        })
                        
                        if not file_id:
                            self.logger.debug(f"MO Seizure {mo_seizure_id}: MO_MEDIA_FILE_ID[{idx}] is null/empty")
            else:
                # Single value (string or number) - treat as single item
                files.append({
                    'source_type': 'mo_seizures',
                    'source_field': 'MO_MEDIA',
                    'parent_id': mo_seizure_id,
                    'file_id': mo_media_file_id if mo_media_file_id else None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                
                if not mo_media_file_id:
                    self.logger.debug(f"MO Seizure {mo_seizure_id}: MO_MEDIA_FILE_ID is null/empty")
        
        return files

