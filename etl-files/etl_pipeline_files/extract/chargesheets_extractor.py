"""
Chargesheets API extractor
"""
from typing import Dict, List, Any
from .base_extractor import BaseExtractor


class ChargesheetsExtractor(BaseExtractor):
    """Extract file references from Chargesheets API"""
    
    def extract_files(self, data: Dict[str, Any], api_date: str = None) -> List[Dict[str, Any]]:
        """
        Extract uploadChargeSheet.fileId from chargesheets API response.
        
        Args:
            data: API response data
            api_date: Date from API query (YYYY-MM-DD format) - used as fallback if record date not available
        
        Returns:
            List of file records
        """
        files = []
        
        if not isinstance(data, dict) or 'data' not in data:
            self.logger.warning("Invalid API response format for chargesheets")
            return files
        
        chargesheets = data.get('data', [])
        if not isinstance(chargesheets, list):
            chargesheets = [chargesheets]
        
        for chargesheet in chargesheets:
            if not isinstance(chargesheet, dict):
                continue
            
            charge_sheet_id = chargesheet.get('chargeSheetId')
            if not charge_sheet_id:
                self.logger.warning("Chargesheet record missing chargeSheetId")
                continue
            
            # Extract date from record (prefer DATE_CREATED, fallback to DATE_MODIFIED, then api_date)
            # CRITICAL: Always ensure record_date is set - use api_date as final fallback
            record_date = None
            if chargesheet.get('DATE_CREATED'):
                record_date = chargesheet.get('DATE_CREATED')
            elif chargesheet.get('DATE_MODIFIED'):
                record_date = chargesheet.get('DATE_MODIFIED')
            elif chargesheet.get('CREATED_DATE'):
                record_date = chargesheet.get('CREATED_DATE')
            elif chargesheet.get('MODIFIED_DATE'):
                record_date = chargesheet.get('MODIFIED_DATE')
            elif chargesheet.get('dateCreated'):
                record_date = chargesheet.get('dateCreated')
            elif chargesheet.get('dateModified'):
                record_date = chargesheet.get('dateModified')
            
            # CRITICAL: Always use api_date as fallback (from_date from query)
            if not record_date and api_date:
                record_date = api_date
            
            # If still None, log warning - loader will handle fallback
            if not record_date:
                charge_sheet_id = chargesheet.get('chargeSheetId', 'unknown')
                self.logger.warning(f"⚠️ Chargesheet {charge_sheet_id}: No date found in record and api_date is None - will use current time")
            
            upload_charge_sheet = chargesheet.get('uploadChargeSheet', None)
            
            if upload_charge_sheet is None:
                # Field not present or null - insert record with file_id=NULL
                files.append({
                    'source_type': 'chargesheets',
                    'source_field': 'uploadChargeSheet',
                    'parent_id': charge_sheet_id,
                    'file_id': None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                self.logger.debug(f"Chargesheet {charge_sheet_id}: uploadChargeSheet is null/empty")
            elif isinstance(upload_charge_sheet, dict):
                # Single object with fileId field
                file_id = upload_charge_sheet.get('fileId')
                files.append({
                    'source_type': 'chargesheets',
                    'source_field': 'uploadChargeSheet',
                    'parent_id': charge_sheet_id,
                    'file_id': file_id if file_id else None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                
                if not file_id:
                    self.logger.debug(f"Chargesheet {charge_sheet_id}: uploadChargeSheet.fileId is null/empty")
            elif isinstance(upload_charge_sheet, list):
                if len(upload_charge_sheet) == 0:
                    # Empty array - insert record with file_id=NULL
                    files.append({
                        'source_type': 'chargesheets',
                        'source_field': 'uploadChargeSheet',
                        'parent_id': charge_sheet_id,
                        'file_id': None,
                        'file_index': 0,
                        'identity_type': None,
                        'identity_number': None,
                        'api_date': record_date
                    })
                    self.logger.debug(f"Chargesheet {charge_sheet_id}: uploadChargeSheet is empty array")
                else:
                    # Process each uploadChargeSheet item
                    for idx, item in enumerate(upload_charge_sheet):
                        if isinstance(item, dict):
                            file_id = item.get('fileId')
                        else:
                            # If it's a string/number directly, use it as file_id
                            file_id = item
                        
                        files.append({
                            'source_type': 'chargesheets',
                            'source_field': 'uploadChargeSheet',
                            'parent_id': charge_sheet_id,
                            'file_id': file_id if file_id else None,
                            'file_index': idx,
                            'identity_type': None,
                            'identity_number': None,
                            'api_date': record_date
                        })
                        
                        if not file_id:
                            self.logger.debug(f"Chargesheet {charge_sheet_id}: uploadChargeSheet[{idx}] has no fileId")
            else:
                # Single value (string or number) - treat as single file_id
                files.append({
                    'source_type': 'chargesheets',
                    'source_field': 'uploadChargeSheet',
                    'parent_id': charge_sheet_id,
                    'file_id': upload_charge_sheet if upload_charge_sheet else None,
                    'file_index': 0,
                    'identity_type': None,
                    'identity_number': None,
                    'api_date': record_date
                })
                
                if not upload_charge_sheet:
                    self.logger.debug(f"Chargesheet {charge_sheet_id}: uploadChargeSheet is null/empty")
        
        return files

