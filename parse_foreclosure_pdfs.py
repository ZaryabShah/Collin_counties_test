#!/usr/bin/env python3
"""
COLLIN COUNTY FORECLOSURE PDF PARSER WITH GEMINI AI
Parses PDF files and saves extracted data to parsed_foreclosure_data.json
"""

import os
import json
import time
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import google.generativeai as genai
import PyPDF2
import pdfplumber
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
from dataclasses import dataclass, asdict

# Configure poppler path for pdf2image
POPPLER_PATH = r"C:\Users\zarya\AppData\Local\Microsoft\WinGet\Packages\oschwartz10612.Poppler_Microsoft.Winget.Source_8wekyb3d8bbwe\poppler-25.07.0\Library\bin"

# Configure tesseract path for OCR
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

@dataclass
class ForeClosureData:
    """Structured data extracted from foreclosure PDFs"""
    # Basic case information
    case_number: Optional[str] = None
    filing_date: Optional[str] = None
    case_type: Optional[str] = None
    court: Optional[str] = None
    
    # Parties
    plaintiff: Optional[str] = None
    defendant: Optional[str] = None
    trustee: Optional[str] = None
    
    # Property information
    property_address: Optional[str] = None
    legal_description: Optional[str] = None
    parcel_number: Optional[str] = None
    lot_block_subdivision: Optional[str] = None
    
    # Financial information
    original_loan_amount: Optional[str] = None
    unpaid_balance: Optional[str] = None
    total_debt: Optional[str] = None
    
    # Deed of Trust information
    deed_of_trust_date: Optional[str] = None
    deed_of_trust_recording_date: Optional[str] = None
    deed_of_trust_volume_page: Optional[str] = None
    deed_of_trust_instrument_number: Optional[str] = None
    deed_of_trust_number: Optional[str] = None
    
    # Sale information
    sale_date: Optional[str] = None
    sale_time: Optional[str] = None
    sale_location: Optional[str] = None
    
    # Additional information
    borrower_names: List[str] = None
    lender_name: Optional[str] = None
    attorney_info: Dict[str, str] = None
    
    # Processing metadata
    ai_confidence: Optional[str] = None
    extraction_timestamp: Optional[str] = None
    source_pdf_file: Optional[str] = None

    def __post_init__(self):
        if self.borrower_names is None:
            self.borrower_names = []
        if self.attorney_info is None:
            self.attorney_info = {}

class GeminiForeClosureParser:
    """Gemini AI parser for foreclosure PDFs"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Configure Gemini
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Test API connection
        self._test_api_connection()
    
    def _test_api_connection(self):
        """Test API connection"""
        try:
            print("🚀 Testing Gemini API connection...")
            response = self.model.generate_content("Hello, respond with 'API working'")
            if response and response.text:
                print("✅ API Connection Success")
            else:
                raise Exception("Empty response from API")
        except Exception as e:
            print(f"❌ API Connection Failed: {e}")
            raise
    
    def extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from PDF using multiple methods including OCR"""
        text = ""
        
        # Try pdfplumber first (better for complex layouts)
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            
            if text.strip() and len(text.strip()) > 100:  # Must have meaningful content
                print(f"✅ Text extracted using pdfplumber ({len(text)} chars)")
                return text
        except Exception as e:
            print(f"⚠️ pdfplumber failed: {e}")
        
        # Fallback to PyPDF2
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
            
            if text.strip() and len(text.strip()) > 100:
                print(f"✅ Text extracted using PyPDF2 ({len(text)} chars)")
                return text
        except Exception as e:
            print(f"⚠️ PyPDF2 failed: {e}")
        
        # Final fallback: OCR using pytesseract
        try:
            print("🔍 Attempting OCR extraction...")
            images = convert_from_path(pdf_path, dpi=300, first_page=1, last_page=3, poppler_path=POPPLER_PATH)  # Only first 3 pages
            
            ocr_text = ""
            for i, image in enumerate(images):
                try:
                    page_text = pytesseract.image_to_string(image, config='--oem 3 --psm 6')
                    if page_text.strip():
                        ocr_text += f"PAGE {i+1}:\n{page_text}\n\n"
                except Exception as e:
                    print(f"⚠️ OCR failed for page {i+1}: {e}")
            
            if ocr_text.strip() and len(ocr_text.strip()) > 50:
                print(f"✅ Text extracted using OCR ({len(ocr_text)} chars)")
                return ocr_text
                
        except Exception as e:
            print(f"⚠️ OCR extraction failed: {e}")
        
        print("❌ Failed to extract text from PDF with all methods")
        return ""
    
    def _create_foreclosure_prompt(self, pdf_text: str) -> str:
        """Create specialized prompt for foreclosure document parsing"""
        
        prompt = f"""You are an expert foreclosure document parser. Extract ALL relevant information from this Texas foreclosure notice/deed of trust document.

DOCUMENT TEXT:
{pdf_text}

REQUIRED JSON OUTPUT FORMAT:
{{
    "case_number": "Extract case number if any",
    "filing_date": "Extract filing date in YYYY-MM-DD format",
    "case_type": "FORECLOSURE or DEED_OF_TRUST",
    "court": "Extract court name if mentioned",
    "plaintiff": "Extract plaintiff/lender name (clean, no extra text)",
    "defendant": "Extract defendant/borrower name (clean)",
    "trustee": "Extract trustee name if mentioned",
    "property_address": "Extract COMPLETE property address",
    "legal_description": "Extract full legal description (lot, block, subdivision, etc.)",
    "parcel_number": "Extract parcel/property ID number",
    "lot_block_subdivision": "Extract lot, block, and subdivision details",
    "original_loan_amount": "Extract original loan amount (format: $X,XXX.XX)",
    "unpaid_balance": "Extract current unpaid balance",
    "total_debt": "Extract total debt amount",
    "deed_of_trust_date": "Extract deed of trust execution date",
    "deed_of_trust_recording_date": "Extract recording date",
    "deed_of_trust_volume_page": "Extract volume and page numbers",
    "deed_of_trust_instrument_number": "Extract instrument/document number",
    "deed_of_trust_number": "Extract deed of trust number/ID",
    "sale_date": "Extract foreclosure sale date",
    "sale_time": "Extract sale time",
    "sale_location": "Extract where sale will be held",
    "borrower_names": ["List ALL borrower/debtor names"],
    "lender_name": "Extract original lender name",
    "attorney_info": {{
        "name": "Attorney name",
        "firm": "Law firm name",
        "address": "Attorney address",
        "phone": "Phone number",
        "email": "Email address",
        "bar_number": "State bar number"
    }},
    "ai_confidence": "HIGH/MEDIUM/LOW based on data clarity and completeness"
}}

EXTRACTION RULES:
1. Return ONLY valid JSON - no explanations, no markdown, no code blocks
2. Use null for missing fields (not empty strings)
3. For addresses, extract the COMPLETE property address, not attorney addresses
4. For legal descriptions, include ALL details (lot, block, subdivision, survey, etc.)
5. For monetary amounts, include dollar signs and proper formatting
6. For dates, convert to YYYY-MM-DD format when possible
7. Extract ALL borrower names mentioned
8. Clean names by removing titles, addresses, and extra text
9. Look for recording information (volume, page, instrument numbers)
10. Look for deed of trust number (may appear as "Deed of Trust #", "DOT #", "Trust Deed No.", or similar)
11. Extract sale details (date, time, location)
12. CRITICAL: Distinguish between PROPERTY ADDRESS and ATTORNEY/OFFICE addresses
13. Look for Collin County specific details
13. IMPORTANT: Ensure all string values are properly escaped (no unescaped quotes or backslashes)
14. IMPORTANT: Do not include newlines or tabs inside string values

RESPOND WITH CLEAN JSON ONLY - START WITH {{ AND END WITH }}"""
        
        return prompt
    
    def _make_api_request(self, pdf_text: str, pdf_filename: str) -> Optional[Dict[str, Any]]:
        """Make API request with error handling"""
        max_retries = 3
        
        # Truncate if too long
        max_length = 45000
        if len(pdf_text) > max_length:
            print(f"⚠️ Text truncated to {max_length} characters")
            pdf_text = pdf_text[:max_length] + "\n[TRUNCATED DUE TO LENGTH]"
        
        for attempt in range(max_retries):
            try:
                print(f"🤖 Gemini AI Request: {pdf_filename} (Attempt {attempt + 1})")
                
                if self.api_requests > 0:
                    time.sleep(2)  # Rate limiting
                
                prompt = self._create_foreclosure_prompt(pdf_text)
                
                # Conservative settings
                generation_config = {
                    'temperature': 0.1,
                    'top_p': 0.9,
                    'max_output_tokens': 4096,
                }
                
                # Safety settings
                safety_settings = [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
                ]
                
                response = self.model.generate_content(
                    prompt,
                    generation_config=generation_config,
                    safety_settings=safety_settings
                )
                
                self.api_requests += 1
                
                # Better response validation
                if not response:
                    raise Exception("No response object")
                
                # Check for safety blocks
                if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                    if hasattr(response.prompt_feedback, 'block_reason') and response.prompt_feedback.block_reason:
                        raise Exception(f"Content blocked: {response.prompt_feedback.block_reason}")
                
                # Check for valid response parts
                if not hasattr(response, 'parts') or not response.parts:
                    raise Exception("No response parts")
                
                # Get text content safely
                response_text = ""
                try:
                    if hasattr(response, 'text') and response.text:
                        response_text = response.text.strip()
                    else:
                        # Try to get text from parts
                        for part in response.parts:
                            if hasattr(part, 'text') and part.text:
                                response_text += part.text
                        response_text = response_text.strip()
                except Exception as text_error:
                    raise Exception(f"Failed to get response text: {text_error}")
                
                if not response_text:
                    raise Exception("Empty response text")
                
                # Extract JSON
                parsed_data = self._extract_json_from_response(response_text)
                
                if parsed_data:
                    self.successful_requests += 1
                    return parsed_data
                else:
                    raise Exception("Failed to parse JSON")
                
            except Exception as e:
                print(f"⚠️ Attempt {attempt + 1} failed: {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))  # Increasing delay
        
        self.failed_requests += 1
        return None
    
    def _extract_json_from_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from AI response with enhanced error handling"""
        if not response_text:
            return None
            
        try:
            # Clean response
            response_text = response_text.strip()
            
            # Debug: Show first 200 chars of response
            print(f"📝 Response preview: {response_text[:200]}...")
            
            # Remove markdown if present
            if '```json' in response_text.lower():
                start = response_text.lower().find('```json') + 7
                end = response_text.find('```', start)
                if end != -1:
                    response_text = response_text[start:end].strip()
            elif '```' in response_text:
                # Generic code block
                start = response_text.find('```') + 3
                end = response_text.rfind('```')
                if end > start:
                    response_text = response_text[start:end].strip()
            
            # Multiple attempts to find and parse JSON
            json_attempts = []
            
            # Attempt 1: Find complete JSON object
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start != -1 and json_end > json_start:
                json_attempts.append(response_text[json_start:json_end])
            
            # Attempt 2: Look for JSON after "JSON:" marker
            if "JSON:" in response_text:
                json_part = response_text.split("JSON:")[-1].strip()
                json_start = json_part.find('{')
                json_end = json_part.rfind('}') + 1
                if json_start != -1 and json_end > json_start:
                    json_attempts.append(json_part[json_start:json_end])
            
            # Attempt 3: Just try the whole response
            json_attempts.append(response_text)
            
            for attempt_num, json_str in enumerate(json_attempts, 1):
                try:
                    # Clean up common issues
                    json_str = json_str.strip()
                    
                    # Enhanced JSON repairs
                    json_str = json_str.replace("'", '"')  # Single to double quotes
                    json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas from objects
                    json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas from arrays
                    
                    # Fix common unescaped characters in strings
                    json_str = re.sub(r'(?<!\\)"([^"]*?)\\([^"]*?)"(?!\s*:)', r'"\1\\\\\2"', json_str)
                    
                    # Fix missing quotes around keys
                    json_str = re.sub(r'(\w+):', r'"\1":', json_str)
                    
                    # Fix already quoted keys (prevent double quotes)
                    json_str = re.sub(r'""(\w+)"":', r'"\1":', json_str)
                    
                    # Fix newlines and tabs in string values
                    json_str = re.sub(r'"\s*([^"]*?)\s*\n\s*([^"]*?)\s*"', r'"\1 \2"', json_str, flags=re.MULTILINE)
                    
                    # Try to parse
                    parsed = json.loads(json_str)
                    if isinstance(parsed, dict):
                        print(f"✅ JSON parsed successfully (attempt {attempt_num})")
                        return parsed
                        
                except json.JSONDecodeError as e:
                    print(f"⚠️ JSON parse attempt {attempt_num} failed: {str(e)[:100]}")
                    
                    # Try a more aggressive repair for this specific attempt
                    if attempt_num == 1:
                        try:
                            # Remove problematic characters that might be causing issues
                            cleaned = re.sub(r'[^\x00-\x7F]+', ' ', json_str)  # Remove non-ASCII
                            cleaned = re.sub(r'\s+', ' ', cleaned)  # Normalize whitespace
                            cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)  # Remove trailing commas more aggressively
                            
                            parsed = json.loads(cleaned)
                            if isinstance(parsed, dict):
                                print(f"✅ JSON parsed with aggressive cleaning (attempt {attempt_num})")
                                return parsed
                        except:
                            pass
                    continue
            
            # Final desperate attempt: extract key-value pairs manually
            print("🔧 Attempting manual JSON reconstruction...")
            try:
                manual_json = self._manual_json_extraction(response_text)
                if manual_json:
                    return manual_json
            except Exception as e:
                print(f"⚠️ Manual extraction failed: {e}")
            
            print("❌ All JSON parsing attempts failed")
            return None
            
        except Exception as e:
            print(f"❌ Unexpected error in JSON extraction: {e}")
            return None
    
    def _manual_json_extraction(self, response_text: str) -> Optional[Dict[str, Any]]:
        """Manually extract key-value pairs when JSON parsing fails"""
        try:
            # Initialize result with null values
            result = {
                "case_number": None,
                "filing_date": None,
                "case_type": None,
                "court": None,
                "plaintiff": None,
                "defendant": None,
                "trustee": None,
                "property_address": None,
                "legal_description": None,
                "parcel_number": None,
                "lot_block_subdivision": None,
                "original_loan_amount": None,
                "unpaid_balance": None,
                "total_debt": None,
                "deed_of_trust_date": None,
                "deed_of_trust_recording_date": None,
                "deed_of_trust_volume_page": None,
                "deed_of_trust_instrument_number": None,
                "deed_of_trust_number": None,
                "sale_date": None,
                "sale_time": None,
                "sale_location": None,
                "borrower_names": [],
                "lender_name": None,
                "attorney_info": {},
                "ai_confidence": "LOW"
            }
            
            # Look for key-value patterns
            patterns = {
                "case_number": r'"case_number":\s*"([^"]*)"',
                "filing_date": r'"filing_date":\s*"([^"]*)"',
                "case_type": r'"case_type":\s*"([^"]*)"',
                "court": r'"court":\s*"([^"]*)"',
                "plaintiff": r'"plaintiff":\s*"([^"]*)"',
                "defendant": r'"defendant":\s*"([^"]*)"',
                "property_address": r'"property_address":\s*"([^"]*)"',
                "legal_description": r'"legal_description":\s*"([^"]*)"',
                "original_loan_amount": r'"original_loan_amount":\s*"([^"]*)"',
                "deed_of_trust_number": r'"deed_of_trust_number":\s*"([^"]*)"',
                "sale_date": r'"sale_date":\s*"([^"]*)"',
                "sale_time": r'"sale_time":\s*"([^"]*)"',
                "sale_location": r'"sale_location":\s*"([^"]*)"',
                "lender_name": r'"lender_name":\s*"([^"]*)"'
            }
            
            extracted_count = 0
            for key, pattern in patterns.items():
                match = re.search(pattern, response_text, re.IGNORECASE | re.DOTALL)
                if match:
                    value = match.group(1).strip()
                    if value and value.lower() != 'null':
                        result[key] = value
                        extracted_count += 1
            
            # Look for borrower names array
            borrower_match = re.search(r'"borrower_names":\s*\[(.*?)\]', response_text, re.DOTALL)
            if borrower_match:
                borrower_content = borrower_match.group(1)
                borrowers = re.findall(r'"([^"]*)"', borrower_content)
                if borrowers:
                    result["borrower_names"] = [b.strip() for b in borrowers if b.strip()]
                    extracted_count += 1
            
            if extracted_count >= 3:  # At least 3 fields extracted
                print(f"✅ Manual extraction successful ({extracted_count} fields)")
                return result
            
            return None
            
        except Exception as e:
            print(f"⚠️ Manual extraction error: {e}")
            return None
    
    def parse_pdf(self, pdf_path: str) -> Optional[ForeClosureData]:
        """Parse a single PDF and return structured data"""
        pdf_filename = os.path.basename(pdf_path)
        print(f"\n📄 Processing: {pdf_filename}")
        
        # Extract text
        pdf_text = self.extract_pdf_text(pdf_path)
        if not pdf_text.strip():
            print(f"❌ No text extracted from {pdf_filename}")
            return None
        
        # Get AI response
        ai_response = self._make_api_request(pdf_text, pdf_filename)
        if not ai_response:
            print(f"❌ AI extraction failed, trying fallback extraction for {pdf_filename}")
            ai_response = self._fallback_extraction(pdf_text, pdf_filename)
            if not ai_response:
                print(f"❌ All extraction methods failed for {pdf_filename}")
                return None
        
        # Create ForeClosureData object
        foreclosure_data = ForeClosureData(
            case_number=ai_response.get('case_number'),
            filing_date=ai_response.get('filing_date'),
            case_type=ai_response.get('case_type'),
            court=ai_response.get('court'),
            plaintiff=ai_response.get('plaintiff'),
            defendant=ai_response.get('defendant'),
            trustee=ai_response.get('trustee'),
            property_address=ai_response.get('property_address'),
            legal_description=ai_response.get('legal_description'),
            parcel_number=ai_response.get('parcel_number'),
            lot_block_subdivision=ai_response.get('lot_block_subdivision'),
            original_loan_amount=ai_response.get('original_loan_amount'),
            unpaid_balance=ai_response.get('unpaid_balance'),
            total_debt=ai_response.get('total_debt'),
            deed_of_trust_date=ai_response.get('deed_of_trust_date'),
            deed_of_trust_recording_date=ai_response.get('deed_of_trust_recording_date'),
            deed_of_trust_volume_page=ai_response.get('deed_of_trust_volume_page'),
            deed_of_trust_instrument_number=ai_response.get('deed_of_trust_instrument_number'),
            deed_of_trust_number=ai_response.get('deed_of_trust_number'),
            sale_date=ai_response.get('sale_date'),
            sale_time=ai_response.get('sale_time'),
            sale_location=ai_response.get('sale_location'),
            borrower_names=ai_response.get('borrower_names', []),
            lender_name=ai_response.get('lender_name'),
            attorney_info=ai_response.get('attorney_info', {}),
            ai_confidence=ai_response.get('ai_confidence'),
            extraction_timestamp=datetime.now().isoformat(),
            source_pdf_file=pdf_filename
        )
        
        print(f"✅ Successfully parsed {pdf_filename}")
        return foreclosure_data
    
    def _fallback_extraction(self, pdf_text: str, pdf_filename: str) -> Optional[Dict[str, Any]]:
        """Fallback regex-based extraction when AI fails"""
        print(f"🔧 Using fallback regex extraction for {pdf_filename}")
        
        fallback_data = {
            "case_number": None,
            "filing_date": None,
            "case_type": "FORECLOSURE",
            "court": None,
            "plaintiff": None,
            "defendant": None,
            "trustee": None,
            "property_address": None,
            "legal_description": None,
            "parcel_number": None,
            "lot_block_subdivision": None,
            "original_loan_amount": None,
            "unpaid_balance": None,
            "total_debt": None,
            "deed_of_trust_date": None,
            "deed_of_trust_recording_date": None,
            "deed_of_trust_volume_page": None,
            "deed_of_trust_instrument_number": None,
            "deed_of_trust_number": None,
            "sale_date": None,
            "sale_time": None,
            "sale_location": None,
            "borrower_names": [],
            "lender_name": None,
            "attorney_info": {},
            "ai_confidence": "LOW"
        }
        
        try:
            text_upper = pdf_text.upper()
            
            # Look for property address patterns
            address_patterns = [
                r'PROPERTY ADDRESS[:\s]+([^\n]+(?:\n[^\n]+)*?)(?=\n\n|\n[A-Z]{3,}|$)',
                r'PROPERTY[:\s]+([^\n]+(?:\n[^\n\r]+)*?)(?=\n\n|\nLEGAL|$)',
                r'(\d+[^\n]*(?:STREET|ST|DRIVE|DR|LANE|LN|AVENUE|AVE|ROAD|RD|BOULEVARD|BLVD|CIRCLE|CIR|COURT|CT|PLACE|PL|TRAIL|TRL)[^\n]*(?:\n[^\n]*(?:TX|TEXAS)[^\n]*\d{5})?)'
            ]
            
            for pattern in address_patterns:
                match = re.search(pattern, pdf_text, re.IGNORECASE | re.MULTILINE)
                if match:
                    address = match.group(1).strip()
                    if len(address) > 10:  # Must be meaningful
                        fallback_data["property_address"] = address
                        break
            
            # Look for borrower/defendant names
            name_patterns = [
                r'BORROWER[S]?[:\s]+([^\n]+)',
                r'GRANTOR[S]?[:\s]+([^\n]+)',
                r'DEBTOR[S]?[:\s]+([^\n]+)'
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, pdf_text, re.IGNORECASE)
                if match:
                    names = match.group(1).strip()
                    if names and len(names) > 3:
                        fallback_data["defendant"] = names
                        fallback_data["borrower_names"] = [name.strip() for name in names.split('AND') if name.strip()]
                        break
            
            # Look for monetary amounts
            money_patterns = [
                r'\$[\d,]+\.?\d*',
                r'(?:AMOUNT|SUM|DEBT)[:\s]+\$?([\d,]+\.?\d*)'
            ]
            
            amounts = []
            for pattern in money_patterns:
                matches = re.findall(pattern, pdf_text, re.IGNORECASE)
                for match in matches:
                    amount_str = match.replace(',', '') if isinstance(match, str) else match
                    try:
                        if float(amount_str.replace('$', '')) > 1000:  # Must be significant
                            amounts.append(match)
                    except:
                        pass
            
            if amounts:
                fallback_data["total_debt"] = f"${amounts[0]}" if not amounts[0].startswith('$') else amounts[0]
            
            # Look for dates
            date_patterns = [
                r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
                r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})',
                r'((?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{1,2},?\s+\d{4})'
            ]
            
            dates_found = []
            for pattern in date_patterns:
                matches = re.findall(pattern, pdf_text, re.IGNORECASE)
                dates_found.extend(matches)
            
            if dates_found:
                fallback_data["deed_of_trust_date"] = dates_found[0]
            
            # Check if we extracted anything meaningful
            meaningful_fields = ['property_address', 'defendant', 'total_debt']
            if any(fallback_data.get(field) for field in meaningful_fields):
                print(f"✅ Fallback extraction found some data for {pdf_filename}")
                return fallback_data
            else:
                print(f"⚠️ Fallback extraction found no meaningful data for {pdf_filename}")
                return None
                
        except Exception as e:
            print(f"❌ Fallback extraction error for {pdf_filename}: {e}")
            return None

class ForeClosureJSONUpdater:
    """Saves parsed PDF data to a separate JSON file"""
    
    def __init__(self, parsed_data_file: str, checkpoint_file: str):
        self.parsed_data_file = parsed_data_file
        self.checkpoint_file = checkpoint_file
        self.processed_pdfs = self.load_checkpoints()
    
    def load_checkpoints(self) -> set:
        """Load processed PDF checkpoints"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('processed_pdfs', []))
            except:
                pass
        return set()
    
    def save_checkpoints(self):
        """Save processed PDF checkpoints"""
        checkpoint_data = {
            'processed_pdfs': list(self.processed_pdfs),
            'last_updated': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def load_parsed_data(self) -> List[Dict]:
        """Load existing parsed PDF data"""
        try:
            if os.path.exists(self.parsed_data_file):
                with open(self.parsed_data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            print(f"❌ Error loading parsed data file: {e}")
            return []
    
    def save_parsed_data(self, data: List[Dict]):
        """Save parsed PDF data to separate file"""
        try:
            with open(self.parsed_data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"✅ Saved parsed data to {self.parsed_data_file}")
        except Exception as e:
            print(f"❌ Error saving parsed data file: {e}")
    
    def check_if_already_parsed(self, foreclosure_data: ForeClosureData, parsed_records: List[Dict]) -> bool:
        """Check if this PDF has already been parsed"""
        pdf_filename = foreclosure_data.source_pdf_file
        if not pdf_filename:
            return False
        
        # Extract detail_id from filename (e.g., "3927190.pdf" -> "3927190")
        detail_id = pdf_filename.replace('.pdf', '')
        
        for record in parsed_records:
            if record.get('detail_id') == detail_id:
                return True
        
        return False
    
    def create_parsed_record(self, foreclosure_data: ForeClosureData) -> Dict:
        """Create a new record from parsed PDF data"""
        pdf_data = asdict(foreclosure_data)
        pdf_filename = foreclosure_data.source_pdf_file
        
        # Extract detail_id from filename (e.g., "3927190.pdf" -> "3927190")
        detail_id = pdf_filename.replace('.pdf', '') if pdf_filename else None
        
        # Create comprehensive record with all parsed data
        parsed_record = {
            # Identifier
            'detail_id': detail_id,
            'source_pdf_file': pdf_filename,
            
            # Case Information
            'case_number': pdf_data.get('case_number'),
            'filing_date': pdf_data.get('filing_date'),
            'case_type': pdf_data.get('case_type'),
            'court': pdf_data.get('court'),
            
            # Parties
            'plaintiff': pdf_data.get('plaintiff'),
            'defendant': pdf_data.get('defendant'),
            'trustee': pdf_data.get('trustee'),
            'borrower_names': pdf_data.get('borrower_names', []),
            'lender_name': pdf_data.get('lender_name'),
            
            # Property Information
            'property_address': pdf_data.get('property_address'),
            'legal_description': pdf_data.get('legal_description'),
            'parcel_number': pdf_data.get('parcel_number'),
            'lot_block_subdivision': pdf_data.get('lot_block_subdivision'),
            
            # Financial Information
            'original_loan_amount': pdf_data.get('original_loan_amount'),
            'unpaid_balance': pdf_data.get('unpaid_balance'),
            'total_debt': pdf_data.get('total_debt'),
            
            # Deed of Trust Information
            'deed_of_trust_date': pdf_data.get('deed_of_trust_date'),
            'deed_of_trust_recording_date': pdf_data.get('deed_of_trust_recording_date'),
            'deed_of_trust_volume_page': pdf_data.get('deed_of_trust_volume_page'),
            'deed_of_trust_instrument_number': pdf_data.get('deed_of_trust_instrument_number'),
            'deed_of_trust_number': pdf_data.get('deed_of_trust_number'),
            
            # Sale Information
            'sale_date': pdf_data.get('sale_date'),
            'sale_time': pdf_data.get('sale_time'),
            'sale_location': pdf_data.get('sale_location'),
            
            # Attorney Information
            'attorney_info': pdf_data.get('attorney_info', {}),
            
            # Processing Metadata
            'ai_confidence': pdf_data.get('ai_confidence'),
            'extraction_timestamp': pdf_data.get('extraction_timestamp'),
            'parsed_date': datetime.now().isoformat()
        }
        
        # Parse address components if available
        if pdf_data.get('property_address'):
            address_parts = self._parse_address(pdf_data['property_address'])
            parsed_record.update(address_parts)
        
        # Parse borrower names if available
        if pdf_data.get('borrower_names'):
            borrowers = pdf_data['borrower_names']
            if len(borrowers) >= 1:
                name_parts = self._parse_name(borrowers[0])
                parsed_record['owner_1_first_name'] = name_parts.get('first', '')
                parsed_record['owner_1_last_name'] = name_parts.get('last', '')
                if len(borrowers) >= 2:
                    name_parts2 = self._parse_name(borrowers[1])
                    parsed_record['owner_2_first_name'] = name_parts2.get('first', '')
                    parsed_record['owner_2_last_name'] = name_parts2.get('last', '')
                
                # Create combined owner name
                parsed_record['owner_name'] = ' & '.join(borrowers)
        
        return parsed_record
    
    def _parse_address(self, address: str) -> Dict[str, str]:
        """Parse address into components"""
        if not address:
            return {}
        
        # Basic address parsing
        address_parts = {}
        lines = address.strip().split('\n')
        
        if len(lines) >= 1:
            address_parts['street_address'] = lines[0].strip()
        
        if len(lines) >= 2:
            # Try to parse "CITY, STATE ZIP"
            city_state_zip = lines[1].strip()
            match = re.match(r'^([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$', city_state_zip)
            if match:
                address_parts['city'] = match.group(1)
                address_parts['state'] = match.group(2)
                address_parts['zip'] = match.group(3)
        
        return address_parts
    
    def _parse_name(self, full_name: str) -> Dict[str, str]:
        """Parse full name into first and last name"""
        if not full_name:
            return {}
        
        # Remove common titles and suffixes
        name = re.sub(r'\b(Mr|Mrs|Ms|Dr|Jr|Sr|III|II)\b\.?', '', full_name, flags=re.IGNORECASE).strip()
        
        parts = name.split()
        if len(parts) >= 2:
            return {
                'first': parts[0],
                'last': ' '.join(parts[1:])
            }
        elif len(parts) == 1:
            return {
                'first': parts[0],
                'last': ''
            }
        
        return {}
    
    def save_parsed_pdf_data(self, foreclosure_data: ForeClosureData) -> bool:
        """Save parsed PDF data to separate JSON file"""
        try:
            # Load existing parsed data
            parsed_records = self.load_parsed_data()
            
            # Check if already exists
            if self.check_if_already_parsed(foreclosure_data, parsed_records):
                print(f"⚠️ PDF {foreclosure_data.source_pdf_file} already parsed, skipping...")
                return True
            
            # Create new parsed record
            parsed_record = self.create_parsed_record(foreclosure_data)
            
            # Add to existing records
            parsed_records.append(parsed_record)
            
            # Save updated data
            self.save_parsed_data(parsed_records)
            
            # Mark as processed
            self.processed_pdfs.add(foreclosure_data.source_pdf_file)
            self.save_checkpoints()
            
            detail_id = foreclosure_data.source_pdf_file.replace('.pdf', '') if foreclosure_data.source_pdf_file else "Unknown"
            print(f"✅ Saved parsed data for detail_id: {detail_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving parsed data for {foreclosure_data.source_pdf_file}: {e}")
            return False

class ForeClosurePDFProcessor:
    """Main processor that monitors and processes PDF files"""
    
    def __init__(self, api_key: str, base_dir: str):
        self.base_dir = base_dir
        self.pdf_dir = os.path.join(base_dir, 'pdf_files')
        self.parsed_data_file = os.path.join(base_dir, 'parsed_foreclosure_data.json')
        self.checkpoint_file = os.path.join(base_dir, 'pdf_parsing_checkpoints.json')
        
        # Initialize components
        self.parser = GeminiForeClosureParser(api_key)
        self.updater = ForeClosureJSONUpdater(self.parsed_data_file, self.checkpoint_file)
        
        # Statistics
        self.stats = {
            'total_pdfs_found': 0,
            'pdfs_processed': 0,
            'pdfs_skipped': 0,
            'successful_updates': 0,
            'failed_updates': 0
        }
    
    def get_unprocessed_pdfs(self) -> List[str]:
        """Get list of unprocessed PDF files"""
        if not os.path.exists(self.pdf_dir):
            print(f"⚠️ PDF directory not found: {self.pdf_dir}")
            return []
        
        all_pdfs = []
        for filename in os.listdir(self.pdf_dir):
            if filename.endswith('.pdf'):
                if filename not in self.updater.processed_pdfs:
                    pdf_path = os.path.join(self.pdf_dir, filename)
                    all_pdfs.append(pdf_path)
        
        return sorted(all_pdfs)
    
    def process_single_pdf(self, pdf_path: str) -> bool:
        """Process a single PDF file"""
        try:
            # Parse PDF
            foreclosure_data = self.parser.parse_pdf(pdf_path)
            if not foreclosure_data:
                return False
            
            # Save parsed data to separate JSON file
            success = self.updater.save_parsed_pdf_data(foreclosure_data)
            if success:
                self.stats['successful_updates'] += 1
            else:
                self.stats['failed_updates'] += 1
            
            return success
            
        except Exception as e:
            print(f"❌ Error processing {os.path.basename(pdf_path)}: {e}")
            self.stats['failed_updates'] += 1
            return False
    
    def process_all_pdfs(self):
        """Process all unprocessed PDFs"""
        print("🔍 Scanning for unprocessed PDFs...")
        
        unprocessed_pdfs = self.get_unprocessed_pdfs()
        self.stats['total_pdfs_found'] = len(unprocessed_pdfs)
        
        if not unprocessed_pdfs:
            print("✅ No unprocessed PDFs found")
            return
        
        print(f"📄 Found {len(unprocessed_pdfs)} unprocessed PDFs")
        
        for i, pdf_path in enumerate(unprocessed_pdfs, 1):
            pdf_filename = os.path.basename(pdf_path)
            print(f"\n[{i}/{len(unprocessed_pdfs)}] Processing: {pdf_filename}")
            print("-" * 60)
            
            success = self.process_single_pdf(pdf_path)
            self.stats['pdfs_processed'] += 1
            
            # Small delay to avoid rate limiting
            time.sleep(1)
    
    def run_continuous_monitoring(self, check_interval: int = 300):
        """Run continuous monitoring for new PDFs"""
        print(f"🔄 Starting continuous PDF monitoring (checking every {check_interval} seconds)")
        
        while True:
            try:
                print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Checking for new PDFs...")
                
                # Process any new PDFs
                self.process_all_pdfs()
                
                # Print statistics
                self.print_statistics()
                
                print(f"⏳ Waiting {check_interval} seconds before next check...")
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                print("\n🛑 Monitoring stopped by user")
                break
            except Exception as e:
                print(f"❌ Error in monitoring loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def print_statistics(self):
        """Print processing statistics"""
        print("\n📊 PROCESSING STATISTICS")
        print("-" * 40)
        print(f"📄 Total PDFs Found: {self.stats['total_pdfs_found']}")
        print(f"⚙️ PDFs Processed: {self.stats['pdfs_processed']}")
        print(f"✅ Successful Updates: {self.stats['successful_updates']}")
        print(f"❌ Failed Updates: {self.stats['failed_updates']}")
        print(f"🤖 API Requests: {self.parser.api_requests}")
        print(f"✅ API Success: {self.parser.successful_requests}")
        print(f"❌ API Failures: {self.parser.failed_requests}")

def main():
    """Main function"""
    # Gemini API Key
    api_key = "AIzaSyDazFB331RBkuK0geXQoYFpB1WaGfkVjd4"
    
    # Base directory
    base_dir = r"C:\Users\zarya\Desktop\Python\Collin_Counties_Test"
    
    print("🏠 COLLIN COUNTY FORECLOSURE PDF PARSER")
    print("=" * 50)
    print(f"📁 Base Directory: {base_dir}")
    print(f"🔑 Using Gemini AI for PDF parsing")
    print(f"💾 Parsed data will be saved to: parsed_foreclosure_data.json")
    print("=" * 50)
    
    try:
        # Create processor
        processor = ForeClosurePDFProcessor(api_key, base_dir)
        
        # Ask user for mode
        print("\nSelect processing mode:")
        print("1. Process all unprocessed PDFs once")
        print("2. Run continuous monitoring (checks every 5 minutes)")
        
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "1":
            processor.process_all_pdfs()
            processor.print_statistics()
        elif choice == "2":
            processor.run_continuous_monitoring(300)  # Check every 5 minutes
        else:
            print("Invalid choice. Processing once...")
            processor.process_all_pdfs()
            processor.print_statistics()
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()
