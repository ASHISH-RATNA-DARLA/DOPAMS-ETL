"""
Property API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class PropertyExtractor(BaseExtractor):
    """Extract file references from Property API"""
    
    def extract_files(self, data: Dict[str, Any], api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract MEDIA from property API response.
        
        Args:
            data: API response data
            api_date: Date from API query (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning("Invalid API response format for property")
            return files
        
        properties = data.get('data', [])
        if not isinstance(properties, list):
            properties = [properties]
        
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            
            property_id = prop.get('PROPERTY_ID')
            if not property_id:
                self.logger.warning("Property record missing PROPERTY_ID")
                continue
            
            # Extract date from record (prefer DATE_CREATED, fallback to DATE_MODIFIED, then api_date)
            # CRITICAL: Always ensure record_date is set - use api_date as final fallback
            record_date = None
            if prop.get('DATE_CREATED'):
                record_date = prop.get('DATE_CREATED')
            elif prop.get('DATE_MODIFIED'):
                record_date = prop.get('DATE_MODIFIED')
            elif prop.get('CREATED_DATE'):
                record_date = prop.get('CREATED_DATE')
            elif prop.get('MODIFIED_DATE'):
                record_date = prop.get('MODIFIED_DATE')
            
            # CRITICAL: Always use api_date as fallback (from_date from query)
            if not record_date and api_date:
                record_date = api_date
            
            # If still None, log warning - loader will handle fallback
            if not record_date:
                self.logger.warning(f"⚠️ Property {property_id}: No date found in record and api_date is None - will use current time")
            
            media = prop.get('MEDIA', None)
            
            if media is None:
                self.logger.debug(f"Property {property_id}: MEDIA field not present")
            elif isinstance(media, list):
                if len(media) == 0:
                    # Empty array - insert record with file_id=NULL
                    files.append({
                        'source_type': 'property',
                        'source_field': 'MEDIA',
                        'parent_id': property_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    self.logger.debug(f"Property {property_id}: MEDIA is empty array")
                else:
                    # Process each media item
                    for idx, media_id in enumerate(media):
                        files.append({
                            'source_type': 'property',
                            'source_field': 'MEDIA',
                            'parent_id': property_id,
                            'file_id': media_id if media_id else None,
                            'file_index': idx,
                            'identity_type': None,
                            'identity_number': None,
                            'api_date': record_date
                        })
                        
                        if not media_id:
                            self.logger.debug(f"Property {property_id}: MEDIA[{idx}] is null/empty")
        
        return files


