"""
SEC EDGAR Form D Puller
Pulls Form D filings from SEC EDGAR, filters by funding amount ($2M-$6M),
and posts structured data to Clay webhook.

Designed to run daily via GitHub Actions.
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import time
import re
from typing import Optional
import logging

# Configuration (from environment variables)
import os

CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL", "YOUR_CLAY_WEBHOOK_URL")
MIN_OFFERING_AMOUNT = int(os.getenv("MIN_OFFERING", 2_000_000))
MAX_OFFERING_AMOUNT = int(os.getenv("MAX_OFFERING", 6_000_000))
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "AriOEM contact@example.com")  # Required by SEC
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", 1))

# Industry groups to EXCLUDE (funds, real estate, etc. - not SaaS targets)
EXCLUDED_INDUSTRIES = [
    "Pooled Investment Fund",
    "Hedge Fund",
    "Private Equity Fund",
    "Venture Capital Fund",
    "Real Estate",
    "REITS & Finance",
    "Banking & Financial Services",
    "Insurance",
    "Oil & Gas",
    "Mining",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_recent_form_d_filings(days_back: int = 1) -> list[dict]:
    """
    Get recent Form D filings from SEC EDGAR.
    Uses EFTS (EDGAR Full-Text Search) API as primary method.
    Falls back to daily index if EFTS fails.
    """
    filings = []
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    headers = {"User-Agent": SEC_USER_AGENT}
    
    # Method 1: Try EDGAR Full-Text Search API
    try:
        filings = get_filings_from_efts(start_date, end_date, headers)
        if filings:
            logger.info(f"Got {len(filings)} filings from EFTS API")
            return filings
    except Exception as e:
        logger.warning(f"EFTS API failed: {e}, falling back to daily index")
    
    # Method 2: Fall back to full-index (quarterly master files)
    try:
        filings = get_filings_from_full_index(start_date, end_date)
    except Exception as e:
        logger.error(f"Full-index also failed: {e}")
    
    return filings


def get_filings_from_efts(start_date: datetime, end_date: datetime, headers: dict) -> list[dict]:
    """
    Get Form D filings using EDGAR Full-Text Search API.
    Endpoint: https://efts.sec.gov/LATEST/search-index
    """
    filings = []
    
    # EFTS search endpoint
    search_url = "https://efts.sec.gov/LATEST/search-index"
    
    # Correct payload format for EFTS
    payload = {
        "q": "formType:\"D\" OR formType:\"D/A\"",
        "dateRange": "custom",
        "startdt": start_date.strftime("%Y-%m-%d"),
        "enddt": end_date.strftime("%Y-%m-%d"),
        "from": 0,
        "size": 200
    }
    
    try:
        response = requests.post(
            search_url,
            json=payload,
            headers={**headers, "Content-Type": "application/json"},
            timeout=60
        )
        
        if response.status_code != 200:
            logger.warning(f"EFTS returned {response.status_code}: {response.text[:200]}")
            raise Exception(f"EFTS returned {response.status_code}")
        
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])
        
        logger.info(f"EFTS returned {len(hits)} hits")
        
        for hit in hits:
            source = hit.get("_source", {})
            
            # Extract company name
            display_names = source.get("display_names", [])
            company_name = display_names[0] if display_names else "Unknown"
            
            # Extract CIK
            ciks = source.get("ciks", [])
            cik = str(ciks[0]) if ciks else ""
            
            # Extract filename from _id (format: accession:filename)
            file_id = hit.get("_id", "")
            accession = ""
            if ":" in file_id:
                parts = file_id.split(":")
                accession = parts[0]
            
            filings.append({
                'company_name': company_name,
                'form_type': source.get("form", "D"),
                'cik': cik,
                'date_filed': source.get("file_date", ""),
                'filename': source.get("file_name", ""),
                'accession_number': accession
            })
        
    except requests.exceptions.RequestException as e:
        logger.error(f"EFTS request failed: {e}")
        raise
    
    return filings


def get_filings_from_full_index(start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Get Form D filings from SEC full-index quarterly master files.
    Uses https://www.sec.gov/Archives/edgar/full-index/YYYY/QTR#/master.idx
    which is publicly accessible (unlike daily-index which returns 403).
    """
    filings = []
    headers = {"User-Agent": SEC_USER_AGENT}
    
    # Determine which quarters we need to fetch
    quarters_needed = set()
    current = start_date
    while current <= end_date:
        year = current.year
        quarter = (current.month - 1) // 3 + 1
        quarters_needed.add((year, quarter))
        current += timedelta(days=32)  # Jump roughly a month
    
    # Also add the end date's quarter in case we missed it
    end_year = end_date.year
    end_quarter = (end_date.month - 1) // 3 + 1
    quarters_needed.add((end_year, end_quarter))
    
    logger.info(f"Fetching full-index for quarters: {sorted(quarters_needed)}")
    
    for year, quarter in sorted(quarters_needed):
        index_url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
        
        try:
            logger.info(f"Fetching: {index_url}")
            response = requests.get(index_url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                # Parse the index and filter by date range
                quarter_filings = parse_full_index(response.text, start_date, end_date)
                logger.info(f"Found {len(quarter_filings)} Form D filings in {year} Q{quarter} within date range")
                filings.extend(quarter_filings)
            else:
                logger.warning(f"Full-index returned {response.status_code}: {index_url}")
                
        except Exception as e:
            logger.error(f"Error fetching {index_url}: {e}")
        
        # Rate limiting
        time.sleep(0.2)
    
    return filings


def parse_full_index(index_content: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Parse SEC full-index master.idx file and extract Form D filings within date range.
    Format: CIK|Company Name|Form Type|Date Filed|Filename
    """
    filings = []
    lines = index_content.split('\n')
    
    # Skip header lines until we hit the separator
    data_started = False
    for line in lines:
        if line.startswith('-----'):
            data_started = True
            continue
        
        if not data_started or not line.strip():
            continue
        
        # Parse pipe-delimited format
        parts = line.split('|')
        
        if len(parts) >= 5:
            cik = parts[0].strip()
            company_name = parts[1].strip()
            form_type = parts[2].strip()
            date_filed = parts[3].strip()
            filename = parts[4].strip()
            
            # Only process Form D and D/A
            if form_type not in ['D', 'D/A']:
                continue
            
            # Parse date and check if within range
            try:
                filing_date = datetime.strptime(date_filed, '%Y-%m-%d')
                if filing_date < start_date or filing_date > end_date:
                    continue
            except ValueError:
                continue
            
            filings.append({
                'company_name': company_name,
                'form_type': form_type,
                'cik': cik,
                'date_filed': date_filed,
                'filename': filename,
                'accession_number': filename.split('/')[-1].replace('.txt', '') if filename else ''
            })
    
    return filings


def get_form_d_xml_url(cik: str, accession_number: str) -> str:
    """
    Construct URL to Form D primary XML document.
    """
    # Normalize CIK to 10 digits
    cik_padded = cik.zfill(10)
    # Remove dashes from accession number for path
    acc_nodash = accession_number.replace('-', '')
    
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/primary_doc.xml"


def fetch_form_d_details(filing: dict) -> Optional[dict]:
    """
    Fetch and parse Form D XML to extract offering details.
    """
    headers = {"User-Agent": SEC_USER_AGENT}
    
    # First, get the filing index to find the XML document
    cik = filing['cik']
    filename = filing.get('filename', '')
    
    if not filename:
        return None
    
    # Extract accession number from filename
    # Format: edgar/data/CIK/ACCESSION/filename
    match = re.search(r'edgar/data/\d+/(\d{10}-\d{2}-\d{6})', filename)
    if not match:
        # Try without dashes
        match = re.search(r'edgar/data/\d+/(\d+)', filename)
    
    if not match:
        logger.warning(f"Could not extract accession number from {filename}")
        return None
    
    accession = match.group(1)
    
    # Construct URL to filing index
    acc_nodash = accession.replace('-', '')
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{accession}-index.htm"
    
    try:
        # Get filing index to find primary XML document
        response = requests.get(index_url, headers=headers, timeout=30)
        time.sleep(0.15)
        
        if response.status_code != 200:
            # Try direct XML URL patterns
            xml_urls = [
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/primary_doc.xml",
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/formd.xml",
            ]
            
            for xml_url in xml_urls:
                try:
                    xml_response = requests.get(xml_url, headers=headers, timeout=30)
                    time.sleep(0.15)
                    if xml_response.status_code == 200:
                        return parse_form_d_xml(xml_response.text, filing)
                except:
                    continue
            
            return None
        
        # Find XML file in index (skip xsl transformed versions)
        # First try to find primary_doc.xml without xsl path
        xml_match = re.search(r'href="([^"]*(?<!xsl[^/]*)/primary_doc\.xml)"', response.text, re.IGNORECASE)
        if not xml_match:
            # Fall back to any XML file that's not in an xsl folder
            for match in re.finditer(r'href="([^"]+\.xml)"', response.text, re.IGNORECASE):
                if 'xsl' not in match.group(1).lower():
                    xml_match = match
                    break
        
        if xml_match:
            xml_filename = xml_match.group(1)
            # Handle absolute vs relative paths
            if xml_filename.startswith('/'):
                xml_url = f"https://www.sec.gov{xml_filename}"
            else:
                xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{xml_filename}"
            
            xml_response = requests.get(xml_url, headers=headers, timeout=30)
            time.sleep(0.15)
            
            if xml_response.status_code == 200:
                return parse_form_d_xml(xml_response.text, filing)
    
    except Exception as e:
        logger.error(f"Error fetching Form D details for {filing['company_name']}: {e}")
    
    return None


def parse_form_d_xml(xml_content: str, filing: dict) -> Optional[dict]:
    """
    Parse Form D XML and extract key fields.
    """
    try:
        # Handle namespace
        xml_content = re.sub(r'xmlns="[^"]+"', '', xml_content)
        root = ET.fromstring(xml_content)
        
        # Helper to find element text
        def find_text(path: str, default: str = "") -> str:
            elem = root.find(f".//{path}")
            return elem.text.strip() if elem is not None and elem.text else default
        
        def find_number(path: str, default: int = 0) -> int:
            text = find_text(path)
            if text:
                # Remove commas and convert
                clean = re.sub(r'[^\d.]', '', text)
                try:
                    return int(float(clean))
                except:
                    pass
            return default
        
        # Extract primary issuer info
        issuer_name = find_text("primaryIssuer/entityName") or find_text("issuerName") or filing.get('company_name', '')
        
        # Extract offering amounts
        total_offering = find_number("offeringSalesAmounts/totalOfferingAmount")
        total_sold = find_number("offeringSalesAmounts/totalAmountSold")
        total_remaining = find_number("offeringSalesAmounts/totalRemaining")
        
        # If totalOfferingAmount is 0 or indefinite, check for clarificationOfResponse
        if total_offering == 0:
            indefinite = find_text("offeringSalesAmounts/indefiniteSecuritiesIncluded")
            if indefinite and indefinite.lower() in ['true', 'yes', '1']:
                total_offering = -1  # Mark as indefinite
        
        # Extract additional details
        result = {
            # Company info
            'company_name': issuer_name,
            'cik': filing.get('cik', ''),
            'entity_type': find_text("primaryIssuer/entityType"),
            'jurisdiction': find_text("primaryIssuer/jurisdictionOfInc"),
            'year_of_incorporation': find_text("primaryIssuer/yearOfInc/value"),
            
            # Address
            'street': find_text("primaryIssuer/issuerAddress/street1"),
            'city': find_text("primaryIssuer/issuerAddress/city"),
            'state': find_text("primaryIssuer/issuerAddress/stateOrCountry"),
            'zip': find_text("primaryIssuer/issuerAddress/zipCode"),
            'phone': find_text("primaryIssuer/issuerPhoneNumber"),
            
            # Offering details
            'total_offering_amount': total_offering,
            'total_amount_sold': total_sold,
            'total_remaining': total_remaining,
            'is_indefinite': total_offering == -1,
            
            # Industry
            'industry_group': find_text("industryGroup/industryGroupType"),
            'investment_fund_type': find_text("industryGroup/investmentFundInfo/investmentFundType"),
            
            # Securities offered
            'is_equity': find_text("typesOfSecuritiesOffered/isEquityType").lower() in ['true', 'yes', '1'],
            'is_debt': find_text("typesOfSecuritiesOffered/isDebtType").lower() in ['true', 'yes', '1'],
            
            # Dates
            'date_of_first_sale': find_text("dateOfFirstSale/value"),
            'date_filed': filing.get('date_filed', ''),
            
            # Filing info
            'form_type': filing.get('form_type', 'D'),
            'accession_number': filing.get('filename', '').split('/')[-2] if '/' in filing.get('filename', '') else '',
            
            # Metadata
            'pulled_at': datetime.now().isoformat(),
            'source': 'SEC_EDGAR_FORM_D'
        }
        
        return result
        
    except ET.ParseError as e:
        logger.error(f"XML parse error for {filing.get('company_name', 'Unknown')}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing Form D XML: {e}")
        return None


def filter_by_funding_range(filings: list[dict], min_amount: int, max_amount: int) -> list[dict]:
    """
    Filter filings by total offering amount range and exclude non-SaaS industries.
    """
    filtered = []
    for filing in filings:
        amount = filing.get('total_offering_amount', 0)
        
        # Skip indefinite offerings
        if amount == -1:
            continue
        
        # Check range
        if not (min_amount <= amount <= max_amount):
            continue
        
        # Exclude non-SaaS industries
        industry = filing.get('industry_group', '')
        fund_type = filing.get('investment_fund_type', '')
        
        if industry in EXCLUDED_INDUSTRIES or fund_type:
            logger.debug(f"Excluding {filing.get('company_name')} - industry: {industry}")
            continue
        
        filtered.append(filing)
    
    return filtered


def post_to_clay(filings: list[dict], webhook_url: str) -> bool:
    """
    Post filtered filings to Clay webhook.
    """
    if not filings:
        logger.info("No filings to post to Clay")
        return True
    
    if webhook_url == "YOUR_CLAY_WEBHOOK_URL":
        logger.warning("Clay webhook URL not configured. Printing results instead.")
        print(json.dumps(filings, indent=2))
        return False
    
    try:
        # Clay accepts JSON array or individual records
        # Posting as batch
        response = requests.post(
            webhook_url,
            json={"records": filings},
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        
        if response.status_code in [200, 201, 202]:
            logger.info(f"Successfully posted {len(filings)} filings to Clay")
            return True
        else:
            logger.error(f"Clay webhook error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error posting to Clay: {e}")
        return False


def main():
    """
    Main execution flow.
    """
    logger.info(f"Starting SEC Form D pull for last {LOOKBACK_DAYS} day(s)")
    logger.info(f"Funding range: ${MIN_OFFERING_AMOUNT:,} - ${MAX_OFFERING_AMOUNT:,}")
    
    # Step 1: Get recent Form D filings from index
    logger.info("Fetching Form D filings from SEC EDGAR...")
    raw_filings = get_recent_form_d_filings(days_back=LOOKBACK_DAYS)
    logger.info(f"Found {len(raw_filings)} Form D filings in index")
    
    # Step 2: Fetch detailed data for each filing
    logger.info("Fetching detailed Form D data...")
    detailed_filings = []
    for i, filing in enumerate(raw_filings):
        logger.info(f"Processing {i+1}/{len(raw_filings)}: {filing['company_name']}")
        details = fetch_form_d_details(filing)
        if details:
            detailed_filings.append(details)
        
        # Progress checkpoint every 50 filings
        if (i + 1) % 50 == 0:
            logger.info(f"Processed {i+1}/{len(raw_filings)} filings...")
    
    logger.info(f"Successfully parsed {len(detailed_filings)} filings")
    
    # Step 3: Filter by funding range
    logger.info(f"Filtering for ${MIN_OFFERING_AMOUNT:,} - ${MAX_OFFERING_AMOUNT:,} range...")
    filtered_filings = filter_by_funding_range(
        detailed_filings, 
        MIN_OFFERING_AMOUNT, 
        MAX_OFFERING_AMOUNT
    )
    logger.info(f"Found {len(filtered_filings)} filings in target range")
    
    # Step 4: Post to Clay
    logger.info("Posting results to Clay...")
    success = post_to_clay(filtered_filings, CLAY_WEBHOOK_URL)
    
    # Step 5: Save results to JSON for artifacts
    output_file = f"form_d_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump({
            'run_date': datetime.now().isoformat(),
            'lookback_days': LOOKBACK_DAYS,
            'funding_range': {'min': MIN_OFFERING_AMOUNT, 'max': MAX_OFFERING_AMOUNT},
            'total_in_index': len(raw_filings),
            'total_parsed': len(detailed_filings),
            'total_filtered': len(filtered_filings),
            'results': filtered_filings
        }, f, indent=2)
    logger.info(f"Results saved to {output_file}")
    
    # Step 6: Summary
    print("\n" + "="*50)
    print("SEC Form D Pull Summary")
    print("="*50)
    print(f"Total filings in index: {len(raw_filings)}")
    print(f"Successfully parsed: {len(detailed_filings)}")
    print(f"In target range (${MIN_OFFERING_AMOUNT/1e6:.1f}M - ${MAX_OFFERING_AMOUNT/1e6:.1f}M): {len(filtered_filings)}")
    print(f"Posted to Clay: {'Yes' if success else 'No'}")
    
    if filtered_filings:
        print("\nFiltered Companies:")
        for f in filtered_filings[:10]:  # Show first 10
            print(f"  - {f['company_name']}: ${f['total_offering_amount']:,}")
        if len(filtered_filings) > 10:
            print(f"  ... and {len(filtered_filings) - 10} more")
    
    return filtered_filings


if __name__ == "__main__":
    results = main()
