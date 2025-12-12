#!/usr/bin/env python3
"""
Process Elite Agent Data for GitHub Actions
Reads CSV files from data/ directory, computes agent statistics,
and outputs JSON files to output/ directory.

This runs automatically via GitHub Actions when new data is pushed.
"""

import csv
import json
import hashlib
import os
from collections import defaultdict
from datetime import datetime
import re

def clean_price(price_str):
    """Convert price string like '$295,000' to integer."""
    if not price_str:
        return 0
    return int(re.sub(r'[^\d]', '', str(price_str)) or 0)

def parse_date(date_str):
    """Parse date string like '01/10/2025', '12/31/2024', or '12/31/24' to YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Try multiple date formats
    for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d']:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    return None

def parse_dom(dom_str):
    """Parse days on market - only return valid positive integers."""
    if not dom_str:
        return None
    try:
        dom = int(dom_str)
        if dom > 0:
            return dom
        return None
    except:
        return None

def generate_token(email):
    """Generate a short token from email."""
    return hashlib.md5(email.lower().encode()).hexdigest()[:8]

def process_csv(filepath, market_name):
    """Process CSV and return agent data."""
    agents = defaultdict(lambda: {
        'name': '',
        'first_name': '',
        'email': '',
        'office': '',
        'transactions': 0,
        'listings': 0,
        'sales': 0,
        'list_volume': 0,
        'sale_volume': 0,
        'total_volume': 0,
        'prices': [],
        'dom_values': [],
        'cities': defaultdict(int),
        'zips': defaultdict(int),
        'close_dates': []
    })

    companies = defaultdict(lambda: {
        'name': '',
        'agents': set(),
        'transactions': 0,
        'listings': 0,
        'sales': 0,
        'total_volume': 0
    })

    rows_processed = 0
    rows_skipped_no_date = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            close_date = parse_date(row.get('close_date', ''))
            if not close_date:
                rows_skipped_no_date += 1
                continue

            rows_processed += 1

            sold_price = clean_price(row.get('sold_price', '0'))
            city = row.get('city', '').strip()
            zip_code = row.get('zip', '').strip()
            dom = parse_dom(row.get('days_on_market', ''))

            # Process listing agent
            listing_agent = row.get('listing_agent_name', '').strip()
            listing_first = row.get('listing_agent_first_name', '').strip()
            listing_email = row.get('listing_agent_email', '').strip().lower()
            listing_office = row.get('listing_office_name', '').strip()

            if listing_agent and listing_email:
                key = listing_email
                agents[key]['name'] = listing_agent
                agents[key]['first_name'] = listing_first
                agents[key]['email'] = listing_email
                agents[key]['office'] = listing_office
                agents[key]['transactions'] += 1
                agents[key]['listings'] += 1
                agents[key]['list_volume'] += sold_price
                agents[key]['total_volume'] += sold_price
                agents[key]['prices'].append(sold_price)
                if dom is not None:
                    agents[key]['dom_values'].append(dom)
                if city:
                    agents[key]['cities'][city] += 1
                if zip_code:
                    agents[key]['zips'][zip_code] += 1
                if close_date:
                    agents[key]['close_dates'].append(close_date)

                if listing_office:
                    companies[listing_office.lower()]['name'] = listing_office
                    companies[listing_office.lower()]['agents'].add(key)
                    companies[listing_office.lower()]['transactions'] += 1
                    companies[listing_office.lower()]['listings'] += 1
                    companies[listing_office.lower()]['total_volume'] += sold_price

            # Process selling/buying agent
            selling_agent = row.get('selling_agent_name', '').strip()
            selling_first = row.get('selling_agent_first_name', '').strip()
            selling_email = row.get('selling_agent_email', '').strip().lower()
            selling_office = row.get('selling_office_name', '').strip()

            if selling_agent and selling_email:
                key = selling_email
                agents[key]['name'] = selling_agent
                agents[key]['first_name'] = selling_first
                agents[key]['email'] = selling_email
                agents[key]['office'] = selling_office
                agents[key]['transactions'] += 1
                agents[key]['sales'] += 1
                agents[key]['sale_volume'] += sold_price
                agents[key]['total_volume'] += sold_price
                agents[key]['prices'].append(sold_price)
                if dom is not None:
                    agents[key]['dom_values'].append(dom)
                if city:
                    agents[key]['cities'][city] += 1
                if zip_code:
                    agents[key]['zips'][zip_code] += 1
                if close_date:
                    agents[key]['close_dates'].append(close_date)

                if selling_office:
                    companies[selling_office.lower()]['name'] = selling_office
                    companies[selling_office.lower()]['agents'].add(key)
                    companies[selling_office.lower()]['transactions'] += 1
                    companies[selling_office.lower()]['sales'] += 1
                    companies[selling_office.lower()]['total_volume'] += sold_price

    print(f"  Rows processed: {rows_processed}")
    print(f"  Rows skipped (no close date): {rows_skipped_no_date}")

    return agents, companies

def format_agents_for_js(agents):
    """Format agent data for JSON output."""
    result = []
    for email, data in agents.items():
        if data['transactions'] == 0:
            continue

        avg_price = int(sum(data['prices']) / len(data['prices'])) if data['prices'] else 0
        avg_dom = 0
        if data['dom_values']:
            avg_dom = int(sum(data['dom_values']) / len(data['dom_values']))

        dates = sorted(set(data['close_dates']))
        top_cities = dict(sorted(data['cities'].items(), key=lambda x: -x[1])[:10])
        top_zips = dict(sorted(data['zips'].items(), key=lambda x: -x[1])[:10])

        agent_obj = {
            'n': data['name'],
            'fn': data.get('first_name', ''),
            'e': data['email'],
            'o': data['office'],
            't': data['transactions'],
            'l': data['listings'],
            's': data['sales'],
            'lv': data.get('list_volume', 0),
            'sv': data.get('sale_volume', 0),
            'tv': data['total_volume'],
            'ap': avg_price,
            'asf': 0,
            'ad': avg_dom,
            'dc': len(data['dom_values']),
            'c': top_cities,
            'z': top_zips,
            'tok': generate_token(data['email']),
            'd': dates
        }
        result.append(agent_obj)

    result.sort(key=lambda x: (-x['t'], -x['tv']))
    return result

def format_companies_for_js(companies):
    """Format company data for JSON output."""
    result = []
    for key, data in companies.items():
        if data['transactions'] == 0:
            continue

        company_obj = {
            'n': data['name'],
            't': data['transactions'],
            'l': data['listings'],
            's': data['sales'],
            'tv': data['total_volume'],
            'a': len(data['agents'])
        }
        result.append(company_obj)

    result.sort(key=lambda x: (-x['t'], -x['tv']))
    return result

def calculate_market_stats(agents_js):
    """Calculate market-level statistics."""
    total = len(agents_js)
    if total == 0:
        return {'total': 0, 'avg_trans': 0, 'avg_vol': 0, 'avg_listings': 0, 'avg_dom': 0}

    total_trans = sum(a['t'] for a in agents_js)
    total_vol = sum(a['tv'] for a in agents_js)
    total_listings = sum(a['l'] for a in agents_js)

    dom_agents = [a for a in agents_js if a.get('dc', 0) > 0]
    avg_dom = sum(a['ad'] for a in dom_agents) / len(dom_agents) if dom_agents else 0

    return {
        'total': total,
        'avg_trans': round(total_trans / total, 1),
        'avg_vol': int(total_vol / total),
        'avg_listings': round(total_listings / total, 1),
        'avg_dom': round(avg_dom, 0)
    }

def main():
    # Ensure output directory exists
    os.makedirs('output', exist_ok=True)

    # Process Phoenix
    phoenix_csv = 'data/phoenix_closed.csv'
    if os.path.exists(phoenix_csv):
        print("\n" + "="*60)
        print("Processing Phoenix data...")
        print("="*60)

        agents, companies = process_csv(phoenix_csv, 'Phoenix')
        agents_js = format_agents_for_js(agents)
        companies_js = format_companies_for_js(companies)
        market_stats = calculate_market_stats(agents_js)

        print(f"\nPhoenix Results:")
        print(f"  Unique agents: {len(agents_js)}")
        print(f"  Unique companies: {len(companies_js)}")

        # Output JSON files
        with open('output/phoenix_agents.json', 'w') as f:
            json.dump({
                'agents': agents_js,
                'stats': market_stats,
                'updated': datetime.now().isoformat()
            }, f)
        print(f"  Wrote output/phoenix_agents.json")

        with open('output/phoenix_companies.json', 'w') as f:
            json.dump({
                'companies': companies_js,
                'total': len(companies_js),
                'updated': datetime.now().isoformat()
            }, f)
        print(f"  Wrote output/phoenix_companies.json")

    # Process Tucson
    tucson_csv = 'data/tucson_closed.csv'
    if os.path.exists(tucson_csv):
        print("\n" + "="*60)
        print("Processing Tucson data...")
        print("="*60)

        agents, companies = process_csv(tucson_csv, 'Tucson')
        agents_js = format_agents_for_js(agents)
        companies_js = format_companies_for_js(companies)
        market_stats = calculate_market_stats(agents_js)

        print(f"\nTucson Results:")
        print(f"  Unique agents: {len(agents_js)}")
        print(f"  Unique companies: {len(companies_js)}")

        # Output JSON files
        with open('output/tucson_agents.json', 'w') as f:
            json.dump({
                'agents': agents_js,
                'stats': market_stats,
                'updated': datetime.now().isoformat()
            }, f)
        print(f"  Wrote output/tucson_agents.json")

        with open('output/tucson_companies.json', 'w') as f:
            json.dump({
                'companies': companies_js,
                'total': len(companies_js),
                'updated': datetime.now().isoformat()
            }, f)
        print(f"  Wrote output/tucson_companies.json")

    print("\n" + "="*60)
    print("Done! JSON files written to output/")
    print("="*60)

if __name__ == '__main__':
    main()
