#!/usr/bin/env python
# coding: utf-8

# In[24]:


import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import time
import json
from collections import OrderedDict


# In[27]:


url="https://www.internationalscholarships.com/scholarships"
response=requests.get(url)
if response.status_code==200:
    print("successful")
else:
    print("failed")


# In[28]:


soup=BeautifulSoup(response.text, "html.parser")
# print(soup)


# In[31]:


# t = []
for row in soup.select("table.table tbody tr"):
    first_td = row.find("td")
    if not first_td:
        continue

    # Find all <a> tags inside first <td> (some are empty or badges)
    scholarships = first_td.find_all("a", href=True)
    
    for scholarship in scholarships:
        name = scholarship.text.strip()
        href = scholarship["href"].strip()
        
        if name and href.startswith("/scholarships/"):
            scholarship_url = "https://www.internationalscholarships.com/scholarships" + href
            scholarship_response=requests.get(scholarship_url)
            scholarship_soup=BeautifulSoup(scholarship_response.content, 'html.parser')
            title=scholarship_soup.find('h1', class_="title").text
            section = scholarship_soup.find('section', class_='award-title award-shaded')

            result = {}
            result = OrderedDict()
            result["Title"] = title.strip()
            # Handle <h2> + <p> pairs inside .award-description
            description_div = section.find('div', class_='award-description')
            if description_div:
                headings = description_div.find_all(['h2'])
                for heading in headings:
                    # Get next <p> sibling (which holds the content)
                    para = heading.find_next_sibling('p')
                    if para:
                        result[heading.text.strip()] = para.get_text(separator="\n", strip=True)

            # Handle the <h4> + <p> entries outside award-description
            extra_divs = section.find_all('div', class_='clear')
            for div in extra_divs:
                heading = div.find('h4')
                para = div.find('p')
                if heading and para:
                    result[heading.text.strip()] = para.get_text(separator="\n", strip=True)
            json_result = json.dumps(result, indent=4)
            print(json_result)

            break  # Only get the first valid one per row


# In[33]:


all_scholarships = []

for page_num in range(1,4):
    url=f'https://www.internationalscholarships.com/scholarships?page={page_num}&per-page=40'
    response=requests.get(url)
    soup=BeautifulSoup(response.content, 'html.parser')
    for row in soup.select("table.table tbody tr"):
        first_td = row.find("td")
        if not first_td:
            continue

        # Find all <a> tags inside first <td> (some are empty or badges)
        scholarships = first_td.find_all("a", href=True)
        
        for scholarship in scholarships:
            name = scholarship.text.strip()
            href = scholarship["href"].strip()
            
            if name and href.startswith("/scholarships/"):
                scholarship_url = "https://www.internationalscholarships.com/scholarships" + href
                scholarship_response=requests.get(scholarship_url)
                scholarship_soup=BeautifulSoup(scholarship_response.content, 'html.parser')
                title=scholarship_soup.find('h1', class_="title").text
                section = scholarship_soup.find('section', class_='award-title award-shaded')

                result = {}
                result = OrderedDict()
                result["Title"] = title.strip()
                # Handle <h2> + <p> pairs inside .award-description
                description_div = section.find('div', class_='award-description')
                if description_div:
                    headings = description_div.find_all(['h2'])
                    for heading in headings:
                        # Get next <p> sibling (which holds the content)
                        para = heading.find_next_sibling('p')
                        if para:
                            result[heading.text.strip()] = para.get_text(separator="\n", strip=True)

                # Handle the <h4> + <p> entries outside award-description
                extra_divs = section.find_all('div', class_='clear')
                for div in extra_divs:
                    heading = div.find('h4')
                    para = div.find('p')
                    if heading and para:
                        result[heading.text.strip()] = para.get_text(separator="\n", strip=True)
                all_scholarships.append(result)
                # json_result = json.dumps(result, indent=4)
                # print(json_result)

                break  # Only get the first valid one per row

with open("scholarships_aggregate.json", "w", encoding="utf-8") as f:
    json.dump(all_scholarships, f, indent=4, ensure_ascii=False)

print("Saved all scholarship data to scholarships_aggregate.json ✅")


# In[ ]:




