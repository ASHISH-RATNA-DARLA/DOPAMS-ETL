"""
Crimes API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class CrimesExtractor(BaseExtractor):
    """Extract file references from Crimes API"""
    
    def extract_files(self, data: Dict[str, Any], api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract FIR_COPY from crimes API response.
        
        Args:
            data: API response data
            api_date: Date from API query (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning("Invalid API response format for crimes")
            return files
        
        crimes = data.get('data', [])
        if not isinstance(crimes, list):
            crimes = [crimes]
        
        for crime in crimes:
            if not isinstance(crime, dict):
                continue
            
            crime_id = crime.get('CRIME_ID')
            if not crime_id:
                self.logger.warning("Crime record missing CRIME_ID")
                continue
            
            fir_copy = crime.get('FIR_COPY')
            
            # Extract date from record (prefer DATE_CREATED, fallback to DATE_MODIFIED, then api_date)
            # CRITICAL: Always ensure record_date is set - use api_date as final fallback
            record_date = None
            if crime.get('DATE_CREATED'):
                record_date = crime.get('DATE_CREATED')
            elif crime.get('DATE_MODIFIED'):
                record_date = crime.get('DATE_MODIFIED')
            elif crime.get('CREATED_DATE'):
                record_date = crime.get('CREATED_DATE')
            elif crime.get('MODIFIED_DATE'):
                record_date = crime.get('MODIFIED_DATE')
            
            # CRITICAL: Always use api_date as fallback (from_date from query)
            # This ensures we always have a date, even if record doesn't have date fields
            if not record_date and api_date:
                record_date = api_date
            
            # If still None, log warning - loader will handle fallback
            if not record_date:
                self.logger.warning(f"⚠️ Crime {crime_id}: No date found in record and api_date is None - will use current time")
            
            # Always create a record, even if fir_copy is null/empty
            file_record = {
                'source_type': 'crime',
                'source_field': 'FIR_COPY',
                'parent_id': crime_id,
                'file_id': fir_copy if fir_copy else None,
                'file_index': 0,
                'identity_type': None,
                'identity_number': None,
                'api_date': record_date  # API date for created_at field
            }
            
            files.append(file_record)
            
            if not fir_copy:
                self.logger.debug(f"Crime {crime_id}: FIR_COPY is empty/null")
        
        return files


