#!/usr/bin/env python
# coding: utf-8

import requests
from bs4 import BeautifulSoup
import json
import time
from urllib.parse import urljoin, urlparse
import re
import os

# Initial seed URLs
program_links = [
    # 'https://www.chevening.org',
    'https://www.manchester.ac.uk/study/international/finance-and-scholarships/funding/'
]

# Configure request headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

# Create output directory if it doesn't exist
os.makedirs("scraped_data", exist_ok=True)

def is_relevant_link(url, base_domain):
    """Check if a URL is relevant to scholarships or funding."""
    # List of keywords that suggest scholarship/funding content
    scholarship_keywords = [
        'scholarship', 'scholarships', 'funding', 'finance', 'financial', 
        'bursary', 'bursaries', 'award', 'awards', 'grant', 'grants', 
        'support', 'fees', 'stipend', 'tuition', 'fund'
    ]
    
    # Skip PDF files and other downloadable content
    file_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 
                       '.zip', '.rar', '.mp3', '.mp4', '.avi', '.mov', '.jpg', 
                       '.jpeg', '.png', '.gif', '.csv']
    
    if any(url.lower().endswith(ext) for ext in file_extensions):
        return False
    
    # Check if URL contains any scholarship-related keywords
    has_keyword = any(keyword in url.lower() for keyword in scholarship_keywords)
    
    # Check if URL is from the same domain
    parsed_url = urlparse(url)
    url_domain = parsed_url.netloc.replace('www.', '')
    base_domain = base_domain.replace('www.', '')
    
    same_domain = url_domain == base_domain
    
    return same_domain and (has_keyword or 'study' in url.lower() or 'international' in url.lower())

def fetch_page(url, headers):
    """Fetch a page with error handling and retries."""
    for attempt in range(3):  # Try up to 3 times
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt < 2:  # Don't sleep after the last attempt
                time.sleep(2)  # Wait before retrying
    return None

def scrape_site(base_url):
    """Scrape a site for scholarship information."""
    parsed_url = urlparse(base_url)
    base_domain = parsed_url.netloc
    
    # Initialize data structure for this site
    site_data = {
        "url": base_url,
        "pages": []
    }
    
    # Track visited URLs to avoid duplicates
    visited_urls = set()
    
    # URLs to be processed
    urls_to_process = [base_url]
    
    while urls_to_process and len(visited_urls) < 100:  # Limit to 100 pages per site
        current_url = urls_to_process.pop(0)
        
        if current_url in visited_urls:
            continue
        
        visited_urls.add(current_url)
        print(f"Processing: {current_url}")
        
        response = fetch_page(current_url, headers)
        if not response:
            continue
        
        # Parse the page content
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Add current page to the collection
        site_data["pages"].append({
            "url": current_url,
            "html": soup.prettify(),
            "title": soup.title.string if soup.title else "No title"
        })
        
        # Find all links on the page
        links = soup.find_all("a", href=True)
        
        for link in links:
            href = link.get('href', '')
            full_url = urljoin(current_url, href)
            
            # Skip non-HTTP links, already visited URLs, and irrelevant URLs
            if not full_url.lower().startswith(('http://', 'https://')):
                continue
                
            if full_url in visited_urls:
                continue
                
            if is_relevant_link(full_url, base_domain):
                urls_to_process.append(full_url)
        
        # Be nice to the server
        time.sleep(1)
    
    return site_data

# Main script execution
for i, link in enumerate(program_links, 1):
    try:
        print(f"\nStarting to scrape: {link}")
        result = scrape_site(link)
        
        # Generate filename based on domain
        domain = urlparse(link).netloc.replace('www.', '').replace('.', '_')
        filename = f"scraped_data/program_{domain}_{i}.json"
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
        
        print(f"Successfully saved: {filename}")
        print(f"Total pages scraped: {len(result['pages'])}")
        
    except Exception as e:
        print(f"Error processing {link}: {e}")
    
    # Wait between processing different sites
    if i < len(program_links):
        time.sleep(3)

print("\nScraping completed!")