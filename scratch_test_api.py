
import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_api():
    base_url = os.getenv('DOPAMAS_API_URL')
    api_key = os.getenv('DOPAMAS_API_KEY')
    
    # Test Hierarchy API
    url = f"{base_url}/master-data/hierarchy"
    params = {
        'fromDate': '2022-01-01',
        'toDate': '2022-01-05'
    }
    headers = {
        'x-api-key': api_key
    }
    
    print(f"Testing URL: {url}")
    print(f"Params: {params}")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Status in JSON: {data.get('status')}")
            items = data.get('data', [])
            print(f"Number of records: {len(items)}")
            if items:
                print(f"Sample: {items[0]}")
        else:
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
