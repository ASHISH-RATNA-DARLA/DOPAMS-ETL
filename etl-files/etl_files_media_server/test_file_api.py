#!/usr/bin/env python3
"""
Manual test script to check if file API is working properly.

Usage:
    python3 test_file_api.py <file_id>
    
Example:
    python3 test_file_api.py 88ce70ff-dbc2-4e26-8ac7-8e3ea8f8f62a
"""

import sys
import requests
from config import API_CONFIG

def test_file_download(file_id: str):
    """Test downloading a specific file from the API."""
    
    # Build URL
    files_base = API_CONFIG.get("files_url")
    if files_base:
        base_url = files_base.rstrip("/")
    else:
        base_url = f"{API_CONFIG['base_url'].rstrip('/')}/files"
    
    url = f"{base_url}/{file_id}"
    
    print("=" * 60)
    print("File API Test")
    print("=" * 60)
    print(f"File ID: {file_id}")
    print(f"URL: {url}")
    print(f"API Key: {API_CONFIG['api_key'][:20]}...")
    print("=" * 60)
    
    headers = {
        "x-api-key": API_CONFIG["api_key"],
    }
    
    try:
        print("\n📡 Sending GET request...")
        response = requests.get(
            url,
            headers=headers,
            timeout=API_CONFIG.get("timeout", 30),
            stream=True
        )
        
        print(f"\n📊 Response Status: {response.status_code}")
        print(f"Response Headers:")
        for key, value in response.headers.items():
            if key.lower() in ['content-type', 'content-length', 'content-disposition', 'retry-after']:
                print(f"  {key}: {value}")
        
        if response.status_code == 200:
            print("\n✅ SUCCESS - File is available!")
            print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            content_length = response.headers.get('Content-Length')
            if content_length:
                print(f"File Size: {int(content_length):,} bytes ({int(content_length)/1024:.2f} KB)")
            
            # Try to read first few bytes
            try:
                chunk = next(response.iter_content(chunk_size=1024))
                print(f"First chunk size: {len(chunk)} bytes")
                print("✅ File content is accessible")
            except Exception as e:
                print(f"⚠️  Could not read content: {e}")
                
        elif response.status_code == 400:
            print("\n❌ BAD REQUEST (400)")
            print("This usually means:")
            print("  - File ID is invalid")
            print("  - File doesn't exist in the system")
            print("  - API endpoint doesn't recognize this file_id")
            try:
                error_body = response.text[:500]
                print(f"\nError response: {error_body}")
            except:
                pass
                
        elif response.status_code == 404:
            print("\n❌ NOT FOUND (404)")
            print("File does not exist in the API")
            
        elif response.status_code == 429:
            retry_after = response.headers.get('Retry-After', 'Unknown')
            print(f"\n⚠️  RATE LIMITED (429)")
            print(f"Retry-After: {retry_after} seconds")
            print("You've exceeded the rate limit (10 requests per minute)")
            
        elif response.status_code >= 500:
            print(f"\n❌ SERVER ERROR ({response.status_code})")
            print("The API server is having issues")
            try:
                error_body = response.text[:500]
                print(f"\nError response: {error_body}")
            except:
                pass
        else:
            print(f"\n⚠️  UNEXPECTED STATUS ({response.status_code})")
            try:
                error_body = response.text[:500]
                print(f"\nResponse: {error_body}")
            except:
                pass
                
    except requests.exceptions.Timeout:
        print("\n❌ TIMEOUT")
        print(f"Request timed out after {API_CONFIG.get('timeout', 30)} seconds")
        
    except requests.exceptions.ConnectionError:
        print("\n❌ CONNECTION ERROR")
        print("Could not connect to the API server")
        print("Check if the server is running and accessible")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 test_file_api.py <file_id>")
        print("\nExample:")
        print("  python3 test_file_api.py 88ce70ff-dbc2-4e26-8ac7-8e3ea8f8f62a")
        sys.exit(1)
    
    file_id = sys.argv[1]
    test_file_download(file_id)


