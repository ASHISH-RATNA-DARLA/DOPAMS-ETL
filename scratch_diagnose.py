
import sys
import psycopg2
import requests
import json
from datetime import datetime

def check_db(host, port, dbname, user, password):
    print(f"\n--- Checking Database: {dbname} on {host} ---")
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password,
            connect_timeout=5
        )
        cur = conn.cursor()
        
        tables = ['hierarchy', 'crimes', 'accused', 'persons']
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM public.{table}")
                count = cur.fetchone()[0]
                print(f"Table public.{table}: {count} rows")
                
                if count > 0:
                    cur.execute(f"SELECT date_created, date_modified FROM public.{table} ORDER BY date_modified DESC LIMIT 1")
                    last_mod = cur.fetchone()
                    print(f"  Last modified record: {last_mod}")
            except Exception as e:
                print(f"  Error checking {table}: {e}")
                conn.rollback()
        
        cur.close()
        conn.close()
        print("Database connection check completed.")
    except Exception as e:
        print(f"Database connection failed: {e}")

def check_api(base_url, api_key):
    print(f"\n--- Checking API: {base_url} ---")
    # Test Hierarchy API with a known range
    url = f"{base_url}/master-data/hierarchy"
    headers = {'x-api-key': api_key}
    
    # Try a range like 2022-01-01 to 2022-01-10
    params = {
        'fromDate': '2022-01-01',
        'toDate': '2022-01-10'
    }
    
    try:
        print(f"Calling: {url}")
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('data', [])
            print(f"Success! Found {len(items)} hierarchy records in sample range.")
            if items:
                print(f"Sample record PS_CODE: {items[0].get('PS_CODE')}")
        else:
            print(f"API Error: {response.text}")
            
        # Also try Crimes API
        url_crimes = f"{base_url}/crimes"
        params_crimes = {
            'fromDate': '2024-01-01',
            'toDate': '2024-01-05'
        }
        print(f"Calling: {url_crimes}")
        response = requests.get(url_crimes, params=params_crimes, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            items = data.get('data', [])
            print(f"Success! Found {len(items)} crime records in sample range.")
        else:
            print(f"API Error: {response.text}")

    except Exception as e:
        print(f"API request failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 8:
        print("Usage: python scratch_diagnose.py <db_host> <db_port> <db_name> <db_user> <db_pass> <api_url> <api_key>")
        sys.exit(1)
        
    db_host, db_port, db_name, db_user, db_pass, api_url, api_key = sys.argv[1:8]
    
    check_db(db_host, db_port, db_name, db_user, db_pass)
    check_api(api_url, api_key)
