#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
from bs4 import BeautifulSoup
import json


# In[3]:


program_links = ['https://www.chevening.org','https://www.manchester.ac.uk/study/international/finance-and-scholarships']


# In[4]:


from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

i = 1
programs_json = {}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

for link in program_links:
    try:

        programs_json[f"p{i}"] = {
            "url": link,
            "pages": []
        }

        page = requests.get(link, headers=headers, timeout=10)  # Set timeout to avoid hanging
        page.raise_for_status()  # Raise error for HTTP issues (e.g., 404, 500)
        soup = BeautifulSoup(page.content, "html.parser")

        canonical_tag = soup.find("link", rel="canonical")
        if canonical_tag and canonical_tag.get('href'):
            program_base = urljoin(link, canonical_tag['href'])
        else:
            program_base = link  # Fallback to the original link if no canonical tag

        programs_json[f"p{i}"]["pages"].append({"url": program_base,
        "html":BeautifulSoup(requests.get(program_base, headers=headers).text, "html.parser").prettify()
        })

        sub_links = soup.find_all("a", href=True)
        
        for sub_link in sub_links:
            full_sub_link_url = urljoin(link, sub_link['href'])
            # program_base_without_www = program_base.replace('www.', '')
            if not full_sub_link_url.lower().startswith(('http://', 'https://')):
                continue
            parsed_url = urlparse(program_base)
            domain = parsed_url.netloc
            normalized_program_base = domain.replace('www.', '')
            if normalized_program_base in full_sub_link_url and 'http' in full_sub_link_url and not any(d["url"] == full_sub_link_url for d in programs_json[f"p{i}"]["pages"]):
                programs_json[f"p{i}"]["pages"].append({"url": full_sub_link_url,         
                "html": BeautifulSoup(requests.get(full_sub_link_url, headers=headers).text, "html.parser").prettify()
        })

        filename = f"program_p_uk{i}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump( programs_json[f"p{i}"], f, indent=4, ensure_ascii=False)

        print(f"Saved: {filename}")  # Indicate progress

    except (requests.RequestException, ValueError) as e:
        print(f"Skipping {link} due to error: {e}")
        continue  # Skip this site and move to the next one

    i += 1

