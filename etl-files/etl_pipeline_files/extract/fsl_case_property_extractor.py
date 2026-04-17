"""
FSL Case Property API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class FslCasePropertyExtractor(BaseExtractor):
    """Extract file references from FSL Case Property API"""
    
    def extract_files(self, data: Dict[str, Any], api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract MEDIA.fileId from fsl_case_property API response.
        
        Args:
            data: API response data
            api_date: Date from API query (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning("Invalid API response format for fsl_case_property")
            return files
        
        case_properties = data.get('data', [])
        if not isinstance(case_properties, list):
            case_properties = [case_properties]
        
        for case_prop in case_properties:
            if not isinstance(case_prop, dict):
                continue
            
            case_property_id = case_prop.get('CASE_PROPERTY_ID')
            if not case_property_id:
                self.logger.warning("FSL Case Property record missing CASE_PROPERTY_ID")
                continue
            
            # Extract dates from API response
            # Use DATE_CREATED for new records (this is what we store in created_at)
            # DATE_MODIFIED can be used later to detect if record changed
            date_created = case_prop.get('DATE_CREATED') or case_prop.get('CREATED_DATE')
            date_modified = case_prop.get('DATE_MODIFIED') or case_prop.get('MODIFIED_DATE')
            
            # Use DATE_CREATED if available (for new records), otherwise DATE_MODIFIED, otherwise api_date
            # CRITICAL: Always ensure record_date is set - use api_date as final fallback
            record_date = date_created or date_modified
            
            # CRITICAL: Always use api_date as fallback (from_date from query)
            if not record_date and api_date:
                record_date = api_date
            
            # If still None, log warning - loader will handle fallback
            if not record_date:
                case_property_id = case_prop.get('CASE_PROPERTY_ID', 'unknown')
                self.logger.warning(f"⚠️ FSL Case Property {case_property_id}: No date found in record and api_date is None - will use current time")
            
            media = case_prop.get('MEDIA', None)
            
            if media is None:
                # Field not present or null - insert record with file_id=NULL
                files.append({
                    'source_type': 'case_property',
                    'source_field': 'MEDIA',
                    'parent_id': case_property_id,
                    'file_id': None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                self.logger.debug(f"FSL Case Property {case_property_id}: MEDIA field not present")
            elif isinstance(media, dict):
                # Single object with fileId or FILE_ID field (API uses both formats)
                file_id = media.get('fileId') or media.get('FILE_ID') or media.get('file_id')
                files.append({
                    'source_type': 'case_property',
                    'source_field': 'MEDIA',
                    'parent_id': case_property_id,
                    'file_id': file_id if file_id else None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                
                if not file_id:
                    self.logger.debug(f"FSL Case Property {case_property_id}: MEDIA.fileId/FILE_ID is null/empty")
            elif isinstance(media, list):
                if len(media) == 0:
                    # Empty array - insert record with file_id=NULL
                    files.append({
                        'source_type': 'case_property',
                        'source_field': 'MEDIA',
                        'parent_id': case_property_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    self.logger.debug(f"FSL Case Property {case_property_id}: MEDIA is empty array")
                else:
                    # Process each media item
                    for idx, item in enumerate(media):
                        if isinstance(item, dict):
                            # API may use fileId, FILE_ID, or file_id
                            file_id = item.get('fileId') or item.get('FILE_ID') or item.get('file_id')
                        else:
                            # If it's a string/number directly, use it as file_id
                            file_id = item
                        
                        files.append({
                            'source_type': 'case_property',
                            'source_field': 'MEDIA',
                            'parent_id': case_property_id,
                            'file_id': file_id if file_id else None,
                            'file_index': idx,
                            'identity_type': None,
                            'identity_number': None,
                            'api_date': record_date
                        })
                        
                        if not file_id:
                            self.logger.debug(f"FSL Case Property {case_property_id}: MEDIA[{idx}] has no fileId/FILE_ID")
            else:
                # Single value (string or number) - treat as single file_id
                files.append({
                    'source_type': 'case_property',
                    'source_field': 'MEDIA',
                    'parent_id': case_property_id,
                    'file_id': media if media else None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                
                if not media:
                    self.logger.debug(f"FSL Case Property {case_property_id}: MEDIA is null/empty")
        
        return files

