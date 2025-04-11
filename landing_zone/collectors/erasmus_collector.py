#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
from bs4 import BeautifulSoup
import json


# In[3]:


# This script scrapes the Erasmus Mundus catalogue for program links from pages 6 to 11.

# Catalog URL
base_url = "https://www.eacea.ec.europa.eu/scholarships/erasmus-mundus-catalogue_en"

# Function to get the links from a specific page
def get_program_links(page_number):
    url = f"{base_url}?page={page_number}"
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        program_links = soup.find_all('a', href=True)
        links = [a['href'] for a in program_links if "http" in a['href'] and "europa" not in a['href']]
        return links
    else:
        print(f"Failed to retrieve page {page_number}")
        return []

# Collect links from pages 6 to 11 (indexes 5 to 10)
program_links = []
for i in range(5, 11):
    links = get_program_links(i)
    program_links += links  # or use program_links.extend(links)

# Print all collected links
print("All Program Links from Pages 6 to 11:")
for link in program_links:
    print(link)


# In[4]:


from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import json

i = 1
programs_json = {}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

title_prefixes = ("Erasmus Mundus Master", "International MSc", "International Master")

def extract_titles(soup):
    titles = []
    title_tag = soup.find("title")
    if title_tag and title_tag.text.strip():
        titles.append(title_tag.text.strip().replace('\xa0',' ').replace('Home - ',''))

    for tag in ["h1", "h2", "h3"]:
        for heading in soup.find_all(tag):
            heading_text = heading.text.strip()
            for prefix in title_prefixes:
                if prefix.lower() in heading_text.lower():                
                    titles.append(heading_text.replace('\xa0',' ').replace('Home - ',''))
    return list(set(titles))

for link in program_links:
    try:
        programs_json[f"p{i}"] = {
            "url": link,
            "pages": []
        }

        response = requests.get(link, headers=headers, timeout=10)
        response.raise_for_status()

        if "text/html" not in response.headers.get("Content-Type", ""):
            print(f"Skipping non-HTML link: {link}")
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        program_titles = extract_titles(soup)
        programs_json[f"p{i}"]["titles"] = program_titles

        canonical_tag = soup.find("link", rel="canonical")
        program_base = urljoin(link, canonical_tag['href']) if canonical_tag and canonical_tag.get('href') else link

        # Add base page
        programs_json[f"p{i}"]["pages"].append({
            "url": program_base,
            "html": BeautifulSoup(requests.get(program_base, headers=headers).content, "html.parser").prettify()
        })

        # Add subpages
        sub_links = soup.find_all("a", href=True)
        parsed_url = urlparse(program_base)
        domain = parsed_url.netloc.replace('www.', '')

        for sub_link in sub_links:
            full_sub_link_url = urljoin(link, sub_link['href'])
            if domain in full_sub_link_url and 'http' in full_sub_link_url and not any(d["url"] == full_sub_link_url for d in programs_json[f"p{i}"]["pages"]):
                try:
                    sub_response = requests.get(full_sub_link_url, headers=headers, timeout=10)
                    sub_response.raise_for_status()

                    if "text/html" in sub_response.headers.get("Content-Type", ""):
                        html = BeautifulSoup(sub_response.content, "html.parser").prettify()
                        programs_json[f"p{i}"]["pages"].append({
                            "url": full_sub_link_url,
                            "html": html
                        })
                    else:
                        print(f"Skipped non-HTML subpage: {full_sub_link_url}")
                except Exception as sub_e:
                    print(f"Error fetching subpage {full_sub_link_url}: {sub_e}")
                    continue

        # Generate domain-based filename
        parsed_domain = urlparse(link).netloc.replace("www.", "").replace(".", "_")
        filename = f"program_{parsed_domain}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(programs_json[f"p{i}"], f, indent=4, ensure_ascii=False)

        print(f"Saved: {filename}")

    except (requests.RequestException, ValueError) as e:
        print(f"Skipping {link} due to error: {e}")
        continue

    i += 1


# In[5]:


# Iterate over the programs_json to get all URLs from the "pages" key
all_urls=[]

for program_key, program_data in programs_json.items():
    test=[]
    test.append(program_data["titles"])
    # Extract URLs from the "pages" list
    for page in program_data["pages"]:
        test.append(page["url"])
    all_urls.append(test)
    


# In[6]:


for i in all_urls:
    print(i[0])


# In[7]:


# all_urls

