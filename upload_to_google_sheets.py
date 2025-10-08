#!/usr/bin/env python3
"""
GOOGLE SHEETS UPLOADER FOR COLLIN COUNTY FORECLOSURE DATA
Combines data fr            # Recording Information
            'recorded_date': safe_get(parsed, 'deed_of_trust_recording_date') or safe_get(original, 'recorded_date'),
            'recorded_time': safe_get(original, 'recorded_time'),
            'document_id': safe_get(original, 'document_id'),
            'document_type': safe_get(original, 'document_type') or 'Foreclosure Notice',
            
            # Legal Information
            'legal_description': safe_get(parsed, 'legal_description') or safe_get(original, 'legal_description'),
            
            # URLs
            'url_to_lead': safe_get(original, 'url_to_lead') or safe_get(original, 'detail_url'),
            'pdf_url': safe_get(original, 'pdf_url'),
            
            # Deed of Trust Information - prefer parsed data, fallback to original
            'deed_of_trust_number': safe_get(parsed, 'deed_of_trust_number') or safe_get(original, 'deed_of_trust_number'),losures.json and parsed_foreclosure_data.json
and uploads to Google Sheets using service account credentials
"""

import os
import json
import time
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import gspread
from google.oauth2.service_account import Credentials

class ForeClosureDataCombiner:
    """Combines data from both JSON files for comprehensive records"""
    
    def __init__(self, original_data_file: str, parsed_data_file: str):
        self.original_data_file = original_data_file
        self.parsed_data_file = parsed_data_file
        
    def load_original_data(self) -> List[Dict]:
        """Load original collin_foreclosures.json data"""
        try:
            with open(self.original_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error loading original data: {e}")
            return []
    
    def load_parsed_data(self) -> List[Dict]:
        """Load parsed foreclosure data"""
        try:
            with open(self.parsed_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error loading parsed data: {e}")
            return []
    
    def combine_records(self) -> List[Dict]:
        """Combine original and parsed data into comprehensive records"""
        original_data = self.load_original_data()
        parsed_data = self.load_parsed_data()
        
        # Create lookup dict for parsed data by detail_id
        parsed_lookup = {}
        for record in parsed_data:
            detail_id = record.get('detail_id')
            if detail_id:
                parsed_lookup[detail_id] = record
        
        combined_records = []
        
        for original in original_data:
            detail_id = original.get('detail_id')
            parsed = parsed_lookup.get(detail_id, {}) if detail_id else {}
            
            # Create combined record with all available information
            combined = self._merge_record_data(original, parsed)
            combined_records.append(combined)
        
        print(f"✅ Combined {len(combined_records)} records")
        return combined_records
    
    def _merge_record_data(self, original: Dict, parsed: Dict) -> Dict:
        """Merge original and parsed data into a single comprehensive record"""
        
        # Helper function to safely get string values
        def safe_get(data_dict, key, default=''):
            value = data_dict.get(key, default)
            if value is None:
                return default
            return str(value).strip() if value else default
        
        combined = {
            # Identifiers
            'detail_id': safe_get(original, 'detail_id'),
            'detail_url': safe_get(original, 'detail_url'),
            
            # Basic Information
            'file_date': safe_get(original, 'file_date'),
            'sale_date': safe_get(original, 'sale_date'),
            'property_type': safe_get(original, 'property_type'),
            
            # Address Information - prefer parsed data when available
            'full_address': self._get_best_address(original, parsed),
            'county': 'Collin',  # Always Collin County
            'street_address': safe_get(original, 'street_address') or self._extract_street_from_address(safe_get(parsed, 'property_address')),
            'city': safe_get(original, 'city') or self._extract_city_from_address(safe_get(parsed, 'property_address')),
            'state': safe_get(original, 'state') or self._extract_state_from_address(safe_get(parsed, 'property_address')) or 'TX',
            'zip': safe_get(original, 'zip') or self._extract_zip_from_address(safe_get(parsed, 'property_address')),
            
            # Owner Information - prefer parsed data
            'owner_1_first_name': safe_get(parsed, 'owner_1_first_name') or safe_get(original, 'owner_1_first_name'),
            'owner_1_last_name': safe_get(parsed, 'owner_1_last_name') or safe_get(original, 'owner_1_last_name'),
            'owner_2_first_name': safe_get(parsed, 'owner_2_first_name') or safe_get(original, 'owner_2_first_name'),
            'owner_2_last_name': safe_get(parsed, 'owner_2_last_name') or safe_get(original, 'owner_2_last_name'),
            'list_name': safe_get(original, 'list_name'),
            
            # Sale Information
            'sale_time': safe_get(parsed, 'sale_time') or safe_get(original, 'sale_time'),
            
            # Recording Information
            'recorded_date': safe_get(parsed, 'deed_of_trust_recording_date') or safe_get(original, 'recorded_date'),
            'recorded_time': safe_get(original, 'recorded_time'),
            'document_id': safe_get(original, 'document_id'),  # Keep original document ID only
            'document_type': safe_get(original, 'document_type') or 'Foreclosure Notice',
            
            # Legal Information
            'legal_description': safe_get(parsed, 'legal_description') or safe_get(original, 'legal_description'),
            
            # URLs
            'url_to_lead': safe_get(original, 'url_to_lead') or safe_get(original, 'detail_url'),
            'pdf_url': safe_get(original, 'pdf_url'),
            
            # Deed of Trust Information
            'deed_of_trust_number': safe_get(parsed, 'deed_of_trust_number'),
            
            # Additional parsed data
            'case_number': safe_get(parsed, 'case_number'),
            'plaintiff': safe_get(parsed, 'plaintiff'),
            'defendant': safe_get(parsed, 'defendant'),
            'trustee': safe_get(parsed, 'trustee'),
            'original_loan_amount': safe_get(parsed, 'original_loan_amount'),
            'unpaid_balance': safe_get(parsed, 'unpaid_balance'),
            'total_debt': safe_get(parsed, 'total_debt'),
            'lender_name': safe_get(parsed, 'lender_name'),
            'sale_location': safe_get(parsed, 'sale_location'),
            'ai_confidence': safe_get(parsed, 'ai_confidence'),
            'pdf_parsed': bool(parsed),
            'extraction_timestamp': safe_get(parsed, 'extraction_timestamp')
        }
        
        return combined
    
    def _get_best_address(self, original: Dict, parsed: Dict) -> str:
        """Get the best available full address"""
        # First priority: Parsed property address if available and meaningful
        parsed_address = parsed.get('property_address') or ''
        if isinstance(parsed_address, str):
            parsed_address = parsed_address.strip()
        else:
            parsed_address = str(parsed_address).strip() if parsed_address else ''
            
        # Check if parsed address is a real address (not a legal description or status)
        if (parsed_address and 
            len(parsed_address) > 10 and 
            'FILED' not in parsed_address.upper() and
            not parsed_address.upper().startswith('BEING ') and  # Skip legal descriptions
            not parsed_address.upper().startswith('LOT ') and    # Skip lot descriptions
            ',' in parsed_address and  # Likely has city/state
            any(char.isdigit() for char in parsed_address[:20])):  # Has street number
            return parsed_address
        
        # Second priority: Original address if not a FILED status
        original_address = original.get('address') or ''
        if isinstance(original_address, str):
            original_address = original_address.strip()
        else:
            original_address = str(original_address).strip() if original_address else ''
            
        if (original_address and 
            'FILED' not in original_address.upper() and
            len(original_address) > 10 and
            any(char.isdigit() for char in original_address[:20])):  # Has street number
            return original_address
        
        # Third priority: Try to construct from original parts
        parts = []
        street = original.get('street_address')
        if street and str(street).strip():
            parts.append(str(street).strip())
        
        city = original.get('city')
        if city and str(city).strip():
            parts.append(str(city).strip())
            
        state = original.get('state')
        if state and str(state).strip():
            parts.append(str(state).strip())
            
        zip_code = original.get('zip')
        if zip_code and str(zip_code).strip():
            parts.append(str(zip_code).strip())
        
        if len(parts) >= 2:  # At least street and city
            return ', '.join(parts)
        
        # Fourth priority: Return parsed address even if it might be a legal description
        if parsed_address and len(parsed_address) > 20:
            return parsed_address
        
        # Last resort: Return original address even if it's a FILED status, but clean it up
        if original_address:
            return original_address
            
        return ''
    
    def _extract_street_from_address(self, full_address: str) -> str:
        """Extract street address from full address"""
        if not full_address or not isinstance(full_address, str):
            return ''
        
        try:
            # Split by comma and take first part (usually street)
            parts = full_address.split(',')
            if parts:
                return parts[0].strip()
        except:
            pass
        
        return ''
    
    def _extract_city_from_address(self, full_address: str) -> str:
        """Extract city from full address"""
        if not full_address or not isinstance(full_address, str):
            return ''
        
        try:
            # Look for pattern: CITY, STATE ZIP
            match = re.search(r',\s*([^,]+),\s*[A-Z]{2}\s+\d{5}', full_address)
            if match:
                return match.group(1).strip()
            
            # Fallback: second part after comma
            parts = full_address.split(',')
            if len(parts) >= 2:
                city_part = parts[1].strip()
                # Remove state and zip if present
                city_part = re.sub(r'\s+[A-Z]{2}\s+\d{5}.*', '', city_part)
                return city_part
        except:
            pass
        
        return ''
    
    def _extract_state_from_address(self, full_address: str) -> str:
        """Extract state from full address"""
        if not full_address or not isinstance(full_address, str):
            return ''
        
        try:
            # Look for TX, TEXAS, or 2-letter state codes
            match = re.search(r'\b(TX|TEXAS|[A-Z]{2})\b', full_address, re.IGNORECASE)
            if match:
                state = match.group(1).upper()
                if state == 'TEXAS':
                    return 'TX'
                return state
        except:
            pass
        
        return ''
    
    def _extract_zip_from_address(self, full_address: str) -> str:
        """Extract ZIP code from full address"""
        if not full_address or not isinstance(full_address, str):
            return ''
        
        try:
            # Look for 5 or 9 digit ZIP codes
            match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', full_address)
            if match:
                return match.group(1)
        except:
            pass
        
        return ''

class GoogleSheetsUploader:
    """Uploads foreclosure data to Google Sheets"""
    
    def __init__(self, credentials_file: str, spreadsheet_url: str, checkpoint_file: str):
        self.credentials_file = credentials_file
        self.spreadsheet_url = spreadsheet_url
        self.checkpoint_file = checkpoint_file
        self.processed_records = self.load_checkpoints()
        
        # Initialize Google Sheets client
        self.client = self._initialize_client()
        self.worksheet = self._get_worksheet()
        
        # Define column mapping
        self.column_mapping = {
            'C': 'full_address',        # Full Address
            'D': 'county',              # County
            'E': 'list_name',           # List Name
            'F': 'street_address',      # Street Address
            'G': 'city',                # City
            'H': 'state',               # State
            'I': 'zip',                 # Zip
            'J': 'owner_1_first_name',  # Owner 1 First name
            'K': 'owner_1_last_name',   # Owner 1 Last Name
            'L': 'owner_2_first_name',  # Owner 2 First name
            'M': 'owner_2_last_name',   # Owner 2 Last Name
            'N': 'sale_date',           # Sale Date
            'O': 'sale_time',           # Sale Time
            'P': 'recorded_date',       # Recorded Date
            'Q': 'recorded_time',       # Recorded Time
            'R': 'document_id',         # Document ID
            'S': 'document_type',       # Document Type
            'T': 'legal_description',   # Legal Description
            'U': 'url_to_lead',         # URL to Lead
            'V': 'pdf_url',             # PDF URL
            'W': 'deed_of_trust_number' # Deed of Trust Number
        }
    
    def _initialize_client(self) -> gspread.Client:
        """Initialize Google Sheets client with service account"""
        try:
            print("🔑 Initializing Google Sheets client...")
            
            # Define required scopes
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Load credentials
            credentials = Credentials.from_service_account_file(
                self.credentials_file, 
                scopes=scopes
            )
            
            # Create client
            client = gspread.authorize(credentials)
            print("✅ Google Sheets client initialized successfully")
            return client
            
        except Exception as e:
            print(f"❌ Failed to initialize Google Sheets client: {e}")
            raise
    
    def _get_worksheet(self) -> gspread.Worksheet:
        """Get the worksheet from the spreadsheet URL"""
        try:
            print("📊 Opening Google Spreadsheet...")
            
            # Extract spreadsheet ID from URL
            spreadsheet_id = self._extract_spreadsheet_id(self.spreadsheet_url)
            
            # Open spreadsheet and get first worksheet
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.get_worksheet(0)  # First sheet
            
            print(f"✅ Opened worksheet: {worksheet.title}")
            return worksheet
            
        except Exception as e:
            print(f"❌ Failed to open worksheet: {e}")
            raise
    
    def _extract_spreadsheet_id(self, url: str) -> str:
        """Extract spreadsheet ID from Google Sheets URL"""
        # Pattern: https://docs.google.com/spreadsheets/d/{ID}/edit...
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        if match:
            return match.group(1)
        
        raise ValueError(f"Could not extract spreadsheet ID from URL: {url}")
    
    def load_checkpoints(self) -> set:
        """Load processed record checkpoints"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('processed_records', []))
            except:
                pass
        return set()
    
    def save_checkpoints(self):
        """Save processed record checkpoints"""
        checkpoint_data = {
            'processed_records': list(self.processed_records),
            'last_updated': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def find_next_empty_row(self) -> int:
        """Find the next empty row in the worksheet"""
        try:
            # Get all values in column A (assuming it has data for every row)
            col_a_values = self.worksheet.col_values(1)  # Column A
            return len(col_a_values) + 1
        except:
            return 2  # Start from row 2 (assuming row 1 has headers)
    
    def upload_record(self, record: Dict, row_number: int) -> bool:
        """Upload a single record to the specified row"""
        try:
            detail_id = record.get('detail_id')
            
            # Check if already processed
            if detail_id in self.processed_records:
                return True
            
            print(f"📤 Uploading record {detail_id} to row {row_number}")
            
            # Prepare row data
            row_data = []
            for col_letter in sorted(self.column_mapping.keys()):
                field_name = self.column_mapping[col_letter]
                value = record.get(field_name, '')
                
                # Clean and format the value
                try:
                    if value is None:
                        value = ''
                    elif isinstance(value, (list, dict)):
                        value = str(value)
                    else:
                        value = str(value)
                        if hasattr(value, 'strip'):
                            value = value.strip()
                except Exception as e:
                    print(f"⚠️ Warning: Error processing field {field_name}: {e}")
                    value = ''
                
                row_data.append(value)
            
            # Calculate the range (C to W for the specific row)
            start_col = 'C'  # Column C
            end_col = 'W'    # Column W (now includes deed of trust number)
            range_name = f"{start_col}{row_number}:{end_col}{row_number}"
            
            # Update the row
            self.worksheet.update(range_name, [row_data])
            
            # Mark as processed
            self.processed_records.add(detail_id)
            
            print(f"✅ Successfully uploaded record {detail_id}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to upload record {detail_id}: {e}")
            return False
    
    def upload_batch(self, records: List[Dict], start_row: int = None) -> Tuple[int, int]:
        """Upload multiple records in batch"""
        if start_row is None:
            start_row = self.find_next_empty_row()
        
        successful_uploads = 0
        failed_uploads = 0
        
        print(f"📊 Starting batch upload of {len(records)} records from row {start_row}")
        
        for i, record in enumerate(records):
            row_number = start_row + i
            
            success = self.upload_record(record, row_number)
            if success:
                successful_uploads += 1
            else:
                failed_uploads += 1
            
            # Save checkpoints periodically
            if (i + 1) % 10 == 0:
                self.save_checkpoints()
                print(f"📝 Checkpoint saved - Processed {i + 1}/{len(records)} records")
            
            # Rate limiting - avoid hitting API limits
            time.sleep(1)  # 1 second between uploads
        
        # Final checkpoint save
        self.save_checkpoints()
        
        print(f"📊 Batch upload complete: {successful_uploads} successful, {failed_uploads} failed")
        return successful_uploads, failed_uploads

class ForeClosureGoogleSheetsProcessor:
    """Main processor for uploading foreclosure data to Google Sheets"""
    
    def __init__(self, base_dir: str, credentials_file: str, spreadsheet_url: str):
        self.base_dir = base_dir
        self.original_data_file = os.path.join(base_dir, 'collin_foreclosures.json')
        self.parsed_data_file = os.path.join(base_dir, 'parsed_foreclosure_data.json')
        self.checkpoint_file = os.path.join(base_dir, 'sheets_upload_checkpoints.json')
        
        # Initialize components
        self.combiner = ForeClosureDataCombiner(self.original_data_file, self.parsed_data_file)
        self.uploader = GoogleSheetsUploader(credentials_file, spreadsheet_url, self.checkpoint_file)
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'new_records': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'already_processed': 0
        }
    
    def get_new_records(self, all_records: List[Dict]) -> List[Dict]:
        """Get records that haven't been processed yet"""
        new_records = []
        for record in all_records:
            detail_id = record.get('detail_id')
            if detail_id and detail_id not in self.uploader.processed_records:
                new_records.append(record)
            else:
                self.stats['already_processed'] += 1
        
        return new_records
    
    def process_all_records(self):
        """Process and upload all records"""
        print("🔄 Loading and combining foreclosure data...")
        
        # Load and combine data
        all_records = self.combiner.combine_records()
        self.stats['total_records'] = len(all_records)
        
        if not all_records:
            print("⚠️ No records found to process")
            return
        
        # Filter for new records
        new_records = self.get_new_records(all_records)
        self.stats['new_records'] = len(new_records)
        
        if not new_records:
            print("✅ All records have already been processed")
            return
        
        print(f"📊 Found {len(new_records)} new records to upload")
        
        # Upload records
        successful, failed = self.uploader.upload_batch(new_records)
        self.stats['successful_uploads'] = successful
        self.stats['failed_uploads'] = failed
    
    def run_continuous_monitoring(self, check_interval: int = 1800):  # 30 minutes
        """Run continuous monitoring for new records"""
        print(f"🔄 Starting continuous Google Sheets monitoring (checking every {check_interval/60} minutes)")
        
        while True:
            try:
                print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Checking for new records...")
                
                # Process any new records
                self.process_all_records()
                
                # Print statistics
                self.print_statistics()
                
                print(f"⏳ Waiting {check_interval/60} minutes before next check...")
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                print("\n🛑 Monitoring stopped by user")
                break
            except Exception as e:
                print(f"❌ Error in monitoring loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying
    
    def print_statistics(self):
        """Print processing statistics"""
        print("\n📊 GOOGLE SHEETS UPLOAD STATISTICS")
        print("-" * 45)
        print(f"📄 Total Records Found: {self.stats['total_records']}")
        print(f"🆕 New Records: {self.stats['new_records']}")
        print(f"✅ Successful Uploads: {self.stats['successful_uploads']}")
        print(f"❌ Failed Uploads: {self.stats['failed_uploads']}")
        print(f"⏭️ Already Processed: {self.stats['already_processed']}")

def main():
    """Main function"""
    # Configuration
    base_dir = r"C:\Users\zarya\Desktop\Python\Collin_Counties_Test"
    credentials_file = os.path.join(base_dir, "credentials.json")
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1U3sTAB4RARsv_w_LzijU-6hlc2c0_5_rQ__ATUP_4H8/edit?gid=0#gid=0"
    
    print("📊 COLLIN COUNTY FORECLOSURE - GOOGLE SHEETS UPLOADER")
    print("=" * 60)
    print(f"📁 Base Directory: {base_dir}")
    print(f"🔑 Credentials File: {credentials_file}")
    print(f"📋 Google Sheet: {spreadsheet_url}")
    print("=" * 60)
    
    try:
        # Create processor
        processor = ForeClosureGoogleSheetsProcessor(base_dir, credentials_file, spreadsheet_url)
        
        # Ask user for mode
        print("\nSelect processing mode:")
        print("1. Upload all new records once")
        print("2. Run continuous monitoring (checks every 30 minutes)")
        
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "1":
            processor.process_all_records()
            processor.print_statistics()
        elif choice == "2":
            processor.run_continuous_monitoring(1800)  # 30 minutes
        else:
            print("Invalid choice. Processing once...")
            processor.process_all_records()
            processor.print_statistics()
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()
