"""
Interrogation API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class InterrogationExtractor(BaseExtractor):
    """Extract file references from Interrogation API"""
    
    def extract_files(self, data: Dict[str, Any], api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract MEDIA, INTERROGATION_REPORT, and DOPAMS_DATA from interrogation API response.
        
        Args:
            data: API response data
            api_date: Date from API query (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning("Invalid API response format for interrogation")
            return files
        
        interrogations = data.get('data', [])
        if not isinstance(interrogations, list):
            interrogations = [interrogations]
        
        for interrogation in interrogations:
            if not isinstance(interrogation, dict):
                continue
            
            interrogation_id = interrogation.get('INTERROGATION_REPORT_ID')
            if not interrogation_id:
                self.logger.warning("Interrogation record missing INTERROGATION_REPORT_ID")
                continue
            
            # Extract date from record (prefer DATE_CREATED, fallback to DATE_MODIFIED, then api_date)
            # CRITICAL: Always ensure record_date is set - use api_date as final fallback
            record_date = None
            if interrogation.get('DATE_CREATED'):
                record_date = interrogation.get('DATE_CREATED')
            elif interrogation.get('DATE_MODIFIED'):
                record_date = interrogation.get('DATE_MODIFIED')
            elif interrogation.get('CREATED_DATE'):
                record_date = interrogation.get('CREATED_DATE')
            elif interrogation.get('MODIFIED_DATE'):
                record_date = interrogation.get('MODIFIED_DATE')
            
            # CRITICAL: Always use api_date as fallback (from_date from query)
            if not record_date and api_date:
                record_date = api_date
            
            # If still None, log warning - loader will handle fallback
            if not record_date:
                self.logger.warning(f"⚠️ Interrogation {interrogation_id}: No date found in record and api_date is None - will use current time")
            
            # Extract MEDIA
            media = interrogation.get('MEDIA', None)
            if media is None:
                self.logger.debug(f"Interrogation {interrogation_id}: MEDIA field not present")
            elif isinstance(media, list):
                if len(media) == 0:
                    files.append({
                        'source_type': 'interrogation',
                        'source_field': 'MEDIA',
                        'parent_id': interrogation_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    self.logger.debug(f"Interrogation {interrogation_id}: MEDIA is empty array")
                else:
                    for idx, media_id in enumerate(media):
                        files.append({
                            'source_type': 'interrogation',
                            'source_field': 'MEDIA',
                            'parent_id': interrogation_id,
                            'file_id': media_id if media_id else None,
                            'file_index': idx,
                            'identity_type': None,
                            'identity_number': None,
                            'api_date': record_date
                        })
            
            # Extract INTERROGATION_REPORT
            interrogation_report = interrogation.get('INTERROGATION_REPORT', None)
            if interrogation_report is None:
                self.logger.debug(f"Interrogation {interrogation_id}: INTERROGATION_REPORT field not present")
            elif isinstance(interrogation_report, list):
                if len(interrogation_report) == 0:
                    files.append({
                        'source_type': 'interrogation',
                        'source_field': 'INTERROGATION_REPORT',
                        'parent_id': interrogation_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    self.logger.debug(f"Interrogation {interrogation_id}: INTERROGATION_REPORT is empty array")
                else:
                    for idx, report_id in enumerate(interrogation_report):
                        files.append({
                            'source_type': 'interrogation',
                            'source_field': 'INTERROGATION_REPORT',
                            'parent_id': interrogation_id,
                            'file_id': report_id if report_id else None,
                            'file_index': idx,
                            'identity_type': None,
                            'identity_number': None,
                            'api_date': record_date
                        })
            
            # Extract DOPAMS_DATA from DOPAMS_LINKS
            dopams_links = interrogation.get('DOPAMS_LINKS', None)
            if dopams_links is None:
                self.logger.debug(f"Interrogation {interrogation_id}: DOPAMS_LINKS field not present")
            elif isinstance(dopams_links, list):
                if len(dopams_links) == 0:
                    files.append({
                        'source_type': 'interrogation',
                        'source_field': 'DOPAMS_DATA',
                        'parent_id': interrogation_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    self.logger.debug(f"Interrogation {interrogation_id}: DOPAMS_LINKS is empty array")
                else:
                    for link in dopams_links:
                        if isinstance(link, dict):
                            dopams_data = link.get('DOPAMS_DATA', None)
                            if dopams_data is None:
                                self.logger.debug(f"Interrogation {interrogation_id}: DOPAMS_DATA field not present in link")
                            elif isinstance(dopams_data, list):
                                if len(dopams_data) == 0:
                                    files.append({
                                        'source_type': 'interrogation',
                                        'source_field': 'DOPAMS_DATA',
                                        'parent_id': interrogation_id,
                                        'file_id': None,
                                        'file_index': 0,
                                        'identity_type': None,
                                        'identity_number': None
                                    })
                                    self.logger.debug(f"Interrogation {interrogation_id}: DOPAMS_DATA is empty array")
                                else:
                                    for idx, data_id in enumerate(dopams_data):
                                        files.append({
                                            'source_type': 'interrogation',
                                            'source_field': 'DOPAMS_DATA',
                                            'parent_id': interrogation_id,
                                            'file_id': data_id if data_id else None,
                                            'file_index': idx,
                                            'identity_type': None,
                                            'identity_number': None,
                                            'api_date': record_date
                                        })
        
        return files


