#!/usr/bin/env python3
"""
Fetch and parse SEC Form ADV filings for registered investment advisers.
Downloads the ADV PDF from adviserinfo.sec.gov and extracts key fields.

Usage:
    python3 scripts/fetch_adv.py --crd 311134
    python3 scripts/fetch_adv.py --name "Gavilan Investment Partners"
"""

import argparse
import io
import re
import sys
import json
import requests
import pypdf


def resolve_crd_from_name(firm_name: str):
    """Resolve a CRD number from a firm name using the IAPD search API."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Origin': 'https://adviserinfo.sec.gov',
        'Referer': 'https://adviserinfo.sec.gov/',
    }
    encoded = requests.utils.quote(firm_name)
    url = f'https://api.adviserinfo.sec.gov/search/firm?query={encoded}&hl=true&nrows=12&r=25&sort=score%2Bdesc&wt=json'
    try:
        resp = requests.get(url, timeout=15, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])
            if hits:
                # First hit is best match; firm_source_id is the CRD
                crd = hits[0].get('_source', {}).get('firm_source_id')
                if crd:
                    return str(crd)
    except Exception:
        pass

    return None


def download_adv_pdf(crd: str) -> bytes:
    """Download the Form ADV PDF for a given CRD number."""
    url = f'https://reports.adviserinfo.sec.gov/reports/ADV/{crd}/PDF/{crd}.pdf'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,*/*',
        'Origin': 'https://adviserinfo.sec.gov',
        'Referer': 'https://adviserinfo.sec.gov/',
    }
    resp = requests.get(url, timeout=60, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f'Failed to download ADV PDF (HTTP {resp.status_code}). URL: {url}')
    if len(resp.content) < 1000:
        raise RuntimeError(f'Downloaded PDF is too small ({len(resp.content)} bytes) — CRD {crd} may be invalid.')
    return resp.content


def extract_and_clean_text(pdf_bytes: bytes) -> tuple[str, str]:
    """Extract text from all PDF pages, returning (raw_text, cleaned_text).

    The raw text contains all page content concatenated (including repeated headers).
    The cleaned text strips the per-page boilerplate header that the IAPD system
    renders on every single page.

    Only reads the PDF once for efficiency.
    """
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    pages_text = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pages_text.append(text)

    raw_text = '\n'.join(pages_text)

    if len(pages_text) < 3:
        return raw_text, raw_text

    # Find the common boilerplate prefix by comparing consecutive non-identical pages.
    boilerplate_prefix = None
    for i in range(1, len(pages_text) - 1):
        pa = pages_text[i]
        pb = pages_text[i + 1]
        if pa == pb:
            continue
        common_len = 0
        for j in range(min(len(pa), len(pb))):
            if pa[j] == pb[j]:
                common_len = j + 1
            else:
                break
        if common_len >= 500:
            boilerplate_prefix = pa[:common_len]
            break

    if not boilerplate_prefix:
        return raw_text, raw_text

    bp_len = len(boilerplate_prefix)

    # Keep page 1 in full, strip the boilerplate prefix from pages 2+
    result_parts = [pages_text[0]]
    for page_text in pages_text[1:]:
        if len(page_text) <= bp_len + 10:
            continue
        check_len = min(200, bp_len)
        if page_text[:check_len] == boilerplate_prefix[:check_len]:
            unique = page_text[bp_len:]
            if unique.strip():
                result_parts.append(unique)
        else:
            result_parts.append(page_text)

    cleaned_text = '\n'.join(result_parts)
    return raw_text, cleaned_text


def parse_header_info(text: str) -> dict:
    """Extract firm name, CRD, SEC file number, and filing date from the header."""
    info = {}

    # Primary Business Name and CRD
    m = re.search(r'Primary Business Name:\s*(.+?)\s*CRD Number:\s*(\d+)', text)
    if m:
        info['firm_name'] = m.group(1).strip()
        info['crd'] = m.group(2).strip()

    # SEC file number (801- for RIAs, 802- for ERAs)
    m = re.search(r'your SEC file number:\s*(80[12]-\d+)', text)
    if m:
        info['sec_file_number'] = m.group(1)

    # Filing date (timestamp on first page)
    # Pattern: "Rev. 10/2021\n5/7/2025 2:19:41 PM" or similar
    m = re.search(r'Rev\.\s*\d+/\d+\n(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*[AP]M)', text)
    if m:
        info['filing_date'] = m.group(1).strip()
    else:
        # Try alternate pattern
        m = re.search(r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*[AP]M)', text)
        if m:
            info['filing_date'] = m.group(1).strip()

    # Amendment type
    m = re.search(r'(Annual (?:Updating )?Amendment|Other-Than-Annual Amendment|Initial Application)', text)
    if m:
        info['amendment_type'] = m.group(1).strip()

    return info


def parse_employees(text: str) -> dict:
    """Extract employee counts from Item 5.A and 5.B."""
    employees = {}

    # Total employees (Item 5.A)
    # PDF text often has no space: "employeesdo you have" or "employeesdo"
    m = re.search(
        r'how many employees?\s*do you have\?[^?]*?clerical workers\.?\s*\n(\d+)',
        text, re.IGNORECASE
    )
    if m:
        employees['total'] = int(m.group(1))

    # Advisory employees (Item 5.B.(1))
    m = re.search(
        r'perform investment advisory functions[^?]*?\?\s*\n(\d+)',
        text, re.IGNORECASE
    )
    if m:
        employees['advisory'] = int(m.group(1))

    # Registered reps (Item 5.B.(2))
    m = re.search(
        r'registered representatives of a broker-dealer\?\s*\n(\d+)',
        text, re.IGNORECASE
    )
    if m:
        employees['registered_reps'] = int(m.group(1))

    return employees


def parse_aum(text: str) -> dict:
    """Extract AUM from Item 5.F."""
    aum = {}

    # Discretionary AUM
    m = re.search(r'Discretionary:\s*\(a\)\s*\$\s*([\d,]+)', text)
    if m:
        aum['discretionary'] = int(m.group(1).replace(',', ''))

    # Non-Discretionary AUM
    m = re.search(r'Non-Discretionary:\s*\(b\)\s*\$\s*([\d,]+)', text)
    if m:
        aum['non_discretionary'] = int(m.group(1).replace(',', ''))

    # Total RAUM
    m = re.search(r'Total:\s*\(c\)\s*\$\s*([\d,]+)', text)
    if m:
        aum['total_raum'] = int(m.group(1).replace(',', ''))

    # Discretionary accounts
    m = re.search(r'Discretionary:\s*\(a\)\s*\$\s*[\d,]+\s*\(d\)\s*(\d+)', text)
    if m:
        aum['discretionary_accounts'] = int(m.group(1))

    # Non-Discretionary accounts
    m = re.search(r'Non-Discretionary:\s*\(b\)\s*\$\s*[\d,]+\s*\(e\)\s*(\d+)', text)
    if m:
        aum['non_discretionary_accounts'] = int(m.group(1))

    # Total accounts
    m = re.search(r'Total:\s*\(c\)\s*\$\s*[\d,]+\s*\(f\)\s*(\d+)', text)
    if m:
        aum['total_accounts'] = int(m.group(1))

    # Non-US RAUM
    m = re.search(r'attributable to clients\s*who\s*\nare non-\s*United States persons\??\s*\n\$\s*([\d,]+)', text)
    if m:
        aum['non_us_raum'] = int(m.group(1).replace(',', ''))

    return aum


def parse_private_funds(raw_text: str) -> list[dict]:
    """Extract private fund data from Schedule D, Section 7.B.(1).

    Uses the FULL raw text (before header stripping) because fund data blocks
    are split across pages with boilerplate inserted in between. We find each
    unique fund by its 805- ID, then search forward for its data patterns.
    """
    funds = []

    # Find fund entry points: "Name of the private fund:\n{NAME}\n(b) Private fund identification number:\n...\n805-XXXX"
    fund_pattern = re.compile(
        r'Name of the private fund:\s*\n(.+?)\n'
        r'\(b\) Private fund identification number:\s*\n'
        r'\(include the "805-" prefix also\)\s*\n'
        r'(805-\d+)'
    )

    fund_matches = list(fund_pattern.finditer(raw_text))

    # Group all occurrences by fund ID. ADV PDFs repeat fund sections for
    # amendments/versions — different occurrences may have different fields
    # filled in. We merge across all occurrences, with later values overwriting
    # earlier ones (so the most current data wins).
    from collections import OrderedDict
    occurrences_by_id: dict[str, list] = OrderedDict()
    for m in fund_matches:
        fund_id = m.group(2)
        if fund_id not in occurrences_by_id:
            occurrences_by_id[fund_id] = []
        occurrences_by_id[fund_id].append(m)

    for fund_id, matches in occurrences_by_id.items():
        fund_name = matches[0].group(1).strip()
        fund = {'name': fund_name, 'fund_id': fund_id}

        # Process each occurrence; later values overwrite earlier ones
        for idx, match in enumerate(matches):
            start = match.start()
            # Block extends to the next fund_pattern match (any fund) or 200k chars
            next_start = len(raw_text)
            for other_m in fund_matches:
                if other_m.start() > start and other_m.start() < next_start:
                    next_start = other_m.start()
            end = min(next_start, start + 200000)
            block = raw_text[start:end]

            # Master-feeder detection (question 6)
            # Master fund: feeder name appears between Q6(b) header and "Yes No"
            m_master = re.search(
                r'Is this a "master fund" in a master-feeder arrangement\?'
                r'.*?Name of private fund\s+Private fund identification number\s*\n'
                r'(.+?)\s+(805-\d+)',
                block, re.DOTALL
            )
            if m_master:
                fund['is_master_fund'] = True
                fund['feeder_fund'] = m_master.group(1).strip()

            # Feeder fund: master name appears after Q6(d) "Name of private fund:"
            m_feeder = re.search(
                r'Is this a "feeder fund" in a master-feeder arrangement\?'
                r'.*?Name of private fund:\s*\n(.+?)\n'
                r'Private fund identification number:\s*\n'
                r'.*?\n(805-\d+)',
                block, re.DOTALL
            )
            if m_feeder and m_feeder.group(1).strip():
                fund['is_feeder_fund'] = True
                fund['master_fund'] = m_feeder.group(1).strip()

            # State/Country of organization (pypdf splits State/Country onto separate lines)
            m = re.search(r'organized:\s*\nState:\s*\n(.+?)\s*\nCountry:\s*\n(.+?)(?:\n|$)', block)
            if m:
                fund['jurisdiction'] = f"{m.group(1).strip()} {m.group(2).strip()}"
            else:
                m = re.search(r'private fund organized:\s*\nState:\s*Country:\s*\n(.+)', block)
                if m:
                    fund['jurisdiction'] = m.group(1).strip()

            # Fund type (question 10)
            m = re.search(r'type of fund is the private fund\?\s*\n(.+?)(?:\n|$)', block)
            if m:
                type_line = m.group(1).strip()
                all_types = 'hedge fund liquidity fund private equity fund real estate fund securitized asset fund venture capital fund Other private fund'
                if type_line.lower().startswith(all_types.lower()[:20]):
                    fund['fund_type'] = '(see ADV — checkbox not extractable from PDF)'
                else:
                    fund['fund_type'] = type_line

            # Gross asset value (question 11)
            m = re.search(r'(?:Current )?[Gg]ross asset value of the private fund:\s*\n?\$\s*([\d,]+)', block)
            if m:
                fund['gross_asset_value'] = int(m.group(1).replace(',', ''))

            # Net asset value
            m = re.search(r'(?:Current )?[Nn]et asset value of the private fund:\s*\n?\$\s*([\d,]+)', block)
            if m:
                fund['net_asset_value'] = int(m.group(1).replace(',', ''))

            # Number of beneficial owners (question 13)
            m = re.search(
                r"(?:number of the private fund.s?\s*beneficial owners|Approximate number of.*beneficial owners):\s*\n?(\d+)",
                block, re.IGNORECASE
            )
            if m:
                fund['beneficial_owners'] = int(m.group(1))

            # Percentage owned by adviser and related persons (question 14)
            m = re.search(r'percentage of the private fund beneficially owned by you[^:]*:\s*\n?(\d+)%', block)
            if m:
                fund['pct_owned_by_adviser'] = f"{m.group(1)}%"
                fund['_pct_owned_int'] = int(m.group(1))

            # Minimum investment
            m = re.search(r'Minimum investment commitment[^:]*:\s*\n?\$\s*([\d,]+)', block)
            if m:
                fund['minimum_investment'] = int(m.group(1).replace(',', ''))

            # Prime brokers (question 24b) — deduplicate across repeated pages
            for pb_m in re.finditer(r'\(b\) Name of the prime broker:\s*\n(.+)', block):
                name = pb_m.group(1).strip()
                brokers = fund.setdefault('prime_brokers', [])
                if name not in brokers:
                    brokers.append(name)

            # Third-party marketers/placement agents (question 28c) — deduplicate
            for mk_m in re.finditer(r'\(c\) Name of the marketer:\s*\n(.+)', block):
                name = mk_m.group(1).strip()
                marketers = fund.setdefault('marketers', [])
                if name not in marketers:
                    marketers.append(name)

        funds.append(fund)

    return funds


def parse_schedule_a(text: str) -> list[dict]:
    """Extract owners above 25% from Schedule A.

    Ownership codes: NA = <5%, A = 5-10%, B = 10-25%, C = 25-50%, D = 50-75%, E = 75%+
    We want C, D, or E.
    """
    owners = []

    # Find the Schedule A section
    sched_a_idx = text.find('Schedule A')
    if sched_a_idx < 0:
        return owners

    # Find the data section after "FULL LEGAL NAME"
    header_idx = text.find('FULL LEGAL NAME', sched_a_idx)
    if header_idx < 0:
        return owners

    # Find the end of Schedule A (start of Schedule B or Schedule C)
    end_idx = len(text)
    for end_marker in ['Schedule B', 'Schedule C']:
        idx = text.find(end_marker, header_idx + 100)
        if idx > 0 and idx < end_idx:
            end_idx = idx

    sched_a_text = text[header_idx:end_idx]

    # Parse owner entries
    # The table format is messy in PDF text extraction. Look for patterns with ownership codes.
    # Pattern: NAME, TYPE (I/DE/FE), TITLE, DATE, OWNERSHIP_CODE, CONTROL, PR, CRD
    # The ownership code is a single letter: NA, A, B, C, D, or E

    # Split into lines and look for entries with ownership codes
    lines = sched_a_text.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Look for lines that contain ownership code pattern (single letter code followed by Y/N)
        # Format varies but typically: NAME I TITLE DATE CODE Y/N Y/N CRD
        # Try to match a line that has a name in LAST, FIRST, MIDDLE format with ownership code

        # Pattern 1: "LAST, FIRST, MIDDLE I TITLE DATE CODE Y N CRD"
        m = re.match(
            r'([A-Z][A-Z\s,\.\-]+?)\s+(I|DE|FE)\s+(.+?)\s+(\d{2}/\d{4})\s+(NA|[A-E])\s+(Y|N)\s+(Y|N)\s*(\d*)',
            line
        )
        if m:
            name = m.group(1).strip().rstrip(',')
            entity_type = m.group(2)
            title = m.group(3).strip()
            date_acquired = m.group(4)
            ownership_code = m.group(5)
            control_person = m.group(6)
            crd = m.group(8) if m.group(8) else None

            code_meanings = {
                'NA': '<5%', 'A': '5-10%', 'B': '10-25%',
                'C': '25-50%', 'D': '50-75%', 'E': '75%+'
            }

            if ownership_code in ('C', 'D', 'E'):
                owners.append({
                    'name': name,
                    'title': title,
                    'ownership_code': ownership_code,
                    'ownership_range': code_meanings.get(ownership_code, ''),
                    'control_person': control_person == 'Y',
                    'date_acquired': date_acquired,
                    'crd': crd,
                })
            i += 1
            continue

        # Pattern 2: Multi-line entry where name and details span multiple lines
        # Check if this line looks like a name (ALL CAPS with comma)
        if re.match(r'^[A-Z][A-Z\s,\.\-]+$', line) and len(line) > 3:
            # Look ahead for ownership code
            combined = line
            for j in range(1, min(4, len(lines) - i)):
                combined += ' ' + lines[i + j].strip()

            m = re.search(r'(I|DE|FE)\s+(.+?)\s+(\d{2}/\d{4})\s+(NA|[A-E])\s+(Y|N)\s+(Y|N)', combined)
            if m:
                name = line.strip().rstrip(',')
                entity_type = m.group(1)
                title = m.group(2).strip()
                date_acquired = m.group(3)
                ownership_code = m.group(4)
                control_person = m.group(5)

                code_meanings = {
                    'NA': '<5%', 'A': '5-10%', 'B': '10-25%',
                    'C': '25-50%', 'D': '50-75%', 'E': '75%+'
                }

                if ownership_code in ('C', 'D', 'E'):
                    owners.append({
                        'name': name,
                        'title': title,
                        'ownership_code': ownership_code,
                        'ownership_range': code_meanings.get(ownership_code, ''),
                        'control_person': control_person == 'Y',
                        'date_acquired': date_acquired,
                    })

        i += 1

    # Deduplicate by name
    seen_names = set()
    unique_owners = []
    for o in owners:
        if o['name'] not in seen_names:
            seen_names.add(o['name'])
            unique_owners.append(o)

    return unique_owners


def parse_client_breakdown(text: str) -> list[dict]:
    """Extract client type breakdown from Item 5.D.

    The PDF table renders each client type on one or two lines, with the account
    count and AUM at the end of the line containing the label. Only rows with
    actual data (account count + dollar amount) are included.

    Format example:
        (f) Pooled investment vehicles (other than investment companies and 1 $ 60,414,982
        business development companies)
        (g) Pension and profit sharing plans (but not the plan participants or $
        government pension plans)
    """
    clients = []

    # Client type labels (the letter code is enough to identify each row)
    client_labels = {
        'a': 'Individuals (non-HNW)',
        'b': 'High net worth individuals',
        'c': 'Banking/thrift institutions',
        'd': 'Investment companies',
        'e': 'Business development companies',
        'f': 'Pooled investment vehicles',
        'g': 'Pension/profit sharing plans',
        'h': 'Charitable organizations',
        'i': 'State/municipal government',
        'j': 'Other investment advisers',
        'k': 'Insurance companies',
        'l': 'Sovereign wealth funds',
        'm': 'Corporations/other businesses',
        'n': 'Other',
    }

    # Find the 5.D section — try both pdfplumber and pypdf anchor text
    section_start = text.find('Type of Client')
    if section_start < 0:
        section_start = text.find('Number of (2) Fewer than (3) Amount of Regulatory Assets')
    if section_start < 0:
        return clients

    section_end = text.find('Compensation Arrangements', section_start)
    if section_end < 0:
        section_end = section_start + 5000

    section_text = text[section_start:section_end]

    # Split section into per-letter blocks, then find count + $ in each block.
    # pypdf wraps long labels onto the next line, so we can't rely on a single
    # line matching — instead we grab everything up to the next (letter) entry.
    entries = re.findall(
        r'\(([a-n])\)\s+(.*?)(?=\n\([a-n]\)|\nCompensation|$)',
        section_text, re.DOTALL
    )
    for letter, block in entries:
        m = re.search(r'(\d+)\s*\n?\s*\$\s*([\d,]+)', block)
        if m:
            accounts = int(m.group(1))
            aum = int(m.group(2).replace(',', ''))
            label = client_labels.get(letter, f'Unknown ({letter})')
            clients.append({'type': label, 'accounts': accounts, 'aum': aum})

    return clients


def format_currency(amount: int) -> str:
    """Format a dollar amount with appropriate suffix."""
    if amount >= 1_000_000_000:
        return f'${amount / 1_000_000_000:.2f}B'
    elif amount >= 1_000_000:
        return f'${amount / 1_000_000:.1f}M'
    elif amount >= 1_000:
        return f'${amount / 1_000:.0f}K'
    else:
        return f'${amount:,}'


def format_output(header: dict, employees: dict, aum: dict,
                   funds: list, owners: list, clients: list) -> str:
    """Format the parsed data into a clean markdown summary."""
    lines = []

    # Header
    firm_name = header.get('firm_name', 'Unknown')
    crd = header.get('crd', 'Unknown')
    sec_num = header.get('sec_file_number', 'N/A')
    filing_date = header.get('filing_date', 'Unknown')
    amendment = header.get('amendment_type', '')

    lines.append(f'## Form ADV Summary: {firm_name}')
    lines.append(f'CRD: {crd} | SEC#: {sec_num} | Filing Date: {filing_date}')
    if amendment:
        lines.append(f'Filing Type: {amendment}')
    lines.append('')

    # Employees
    lines.append('### Employees (Item 5.B)')
    if employees:
        lines.append(f'- Total: {employees.get("total", "N/A")}')
        lines.append(f'- Investment advisory professionals: {employees.get("advisory", "N/A")}')
        if employees.get('registered_reps') is not None:
            lines.append(f'- Registered representatives: {employees.get("registered_reps", 0)}')
    else:
        lines.append('- No employee data found')
    lines.append('')

    # AUM
    lines.append('### Assets Under Management (Item 5.F)')
    if aum:
        disc = aum.get('discretionary')
        non_disc = aum.get('non_discretionary')
        total = aum.get('total_raum')
        lines.append(f'- Discretionary: {format_currency(disc) if disc is not None else "N/A"} ({aum.get("discretionary_accounts", "?")} accounts)')
        lines.append(f'- Non-Discretionary: {format_currency(non_disc) if non_disc is not None else "N/A"} ({aum.get("non_discretionary_accounts", "?")} accounts)')
        lines.append(f'- Total RAUM: {format_currency(total) if total is not None else "N/A"} ({aum.get("total_accounts", "?")} accounts)')
        if aum.get('non_us_raum') is not None:
            lines.append(f'- Non-US RAUM: {format_currency(aum["non_us_raum"])}')
    else:
        lines.append('- No AUM data found')
    lines.append('')

    # Private Funds
    lines.append('### Private Funds (Schedule D, Section 7.B.(1))')
    if funds:
        lines.append(f'Total private funds: {len(funds)}')
        lines.append('')
        for fund in funds:
            lines.append(f'- **{fund.get("name", "Unknown")}**')
            if fund.get('fund_id'):
                lines.append(f'  - Fund ID: {fund["fund_id"]}')
            if fund.get('fund_type'):
                lines.append(f'  - Fund Type: {fund["fund_type"]}')
            if fund.get('gross_asset_value') is not None:
                lines.append(f'  - Gross Asset Value: {format_currency(fund["gross_asset_value"])}')
            if fund.get('net_asset_value') is not None:
                lines.append(f'  - Net Asset Value: {format_currency(fund["net_asset_value"])}')
            if fund.get('pct_owned_by_adviser'):
                pct = fund['pct_owned_by_adviser']
                pct_int = fund.get('_pct_owned_int')
                implied_str = ''
                if pct_int is not None:
                    base = fund.get('net_asset_value') or fund.get('gross_asset_value')
                    if base:
                        implied = int(base * pct_int / 100)
                        implied_str = f' (~{format_currency(implied)} implied)'
                lines.append(f'  - % Owned by Adviser + Related Persons: {pct}{implied_str}')
            if fund.get('beneficial_owners') is not None:
                lines.append(f'  - Beneficial Owners: {fund["beneficial_owners"]}')
            if fund.get('minimum_investment') is not None:
                lines.append(f'  - Minimum Investment: {format_currency(fund["minimum_investment"])}')
            if fund.get('prime_brokers'):
                lines.append(f'  - Prime Broker(s): {", ".join(fund["prime_brokers"])}')
            if fund.get('marketers'):
                lines.append(f'  - Third-Party Marketer(s): {", ".join(fund["marketers"])}')
            else:
                lines.append(f'  - Third-Party Marketer(s): None')
            if fund.get('is_master_fund') and fund.get('feeder_fund'):
                lines.append(f'  - Master fund (feeder: {fund["feeder_fund"]})')
            if fund.get('is_feeder_fund') and fund.get('master_fund'):
                lines.append(f'  - Feeder fund (master: {fund["master_fund"]})')
            if fund.get('jurisdiction'):
                lines.append(f'  - Jurisdiction: {fund["jurisdiction"]}')
    else:
        lines.append('No private funds reported.')
    lines.append('')

    # Owners above 25%
    lines.append('### Owners Above 25% (Schedule A)')
    if owners:
        for owner in owners:
            code = owner.get('ownership_code', '?')
            range_str = owner.get('ownership_range', '')
            lines.append(f'- {owner["name"]} — {owner.get("title", "N/A")} — Ownership: {code} ({range_str})')
            if owner.get('crd'):
                lines.append(f'  CRD: {owner["crd"]}')
    else:
        lines.append('No owners above 25% found (or could not parse Schedule A).')
    lines.append('')

    # Client Breakdown
    lines.append('### Client Breakdown (Item 5.D)')
    if clients:
        for client in clients:
            lines.append(f'- {client["type"]}: {client["accounts"]} account(s), {format_currency(client["aum"])}')
    else:
        lines.append('No client breakdown data found.')

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Fetch and parse SEC Form ADV filings')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--crd', type=str, help='CRD number of the adviser')
    group.add_argument('--name', type=str, help='Firm name to look up')
    parser.add_argument('--json', action='store_true', help='Output as JSON instead of formatted text')

    args = parser.parse_args()

    # Resolve CRD
    crd = args.crd
    if args.name:
        print(f'Resolving CRD for "{args.name}"...', file=sys.stderr)
        crd = resolve_crd_from_name(args.name)
        if not crd:
            print(f'\nCould not resolve CRD number for "{args.name}".', file=sys.stderr)
            print(f'Please provide the CRD number directly with --crd.', file=sys.stderr)
            print(f'You can look it up at: https://adviserinfo.sec.gov/', file=sys.stderr)
            sys.exit(1)
        print(f'Found CRD: {crd}', file=sys.stderr)

    # Download PDF
    print(f'Downloading Form ADV for CRD {crd}...', file=sys.stderr)
    try:
        pdf_bytes = download_adv_pdf(crd)
    except RuntimeError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)
    print(f'Downloaded {len(pdf_bytes):,} bytes', file=sys.stderr)

    # Extract text from PDF once, producing both raw and cleaned versions
    print('Extracting text from PDF...', file=sys.stderr)
    raw_text, text = extract_and_clean_text(pdf_bytes)
    print(f'Extracted {len(raw_text):,} raw / {len(text):,} cleaned characters', file=sys.stderr)

    # Parse sections
    header = parse_header_info(text)
    employees = parse_employees(text)
    aum = parse_aum(text)
    funds = parse_private_funds(raw_text)  # Uses raw text to handle page-split fund blocks
    owners = parse_schedule_a(text)
    clients = parse_client_breakdown(text)

    if args.json:
        output = {
            'header': header,
            'employees': employees,
            'aum': aum,
            'private_funds': funds,
            'owners_above_25pct': owners,
            'client_breakdown': clients,
        }
        print(json.dumps(output, indent=2))
    else:
        print(format_output(header, employees, aum, funds, owners, clients))


if __name__ == '__main__':
    main()
