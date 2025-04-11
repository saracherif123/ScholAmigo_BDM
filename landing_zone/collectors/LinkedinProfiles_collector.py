#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import libraries and packages for the project 

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException

from bs4 import BeautifulSoup
from time import sleep
import csv
import json



print('- Finish importing packages')


# In[2]:


pip install tldextract


# In[7]:


import tldextract
import os
import glob

Titles = {}

json_files = glob.glob("../data/erasmus_data/*.json")

for file_path in json_files:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

        url_name = tldextract.extract(data["url"]).domain
        json_titles = data.get("titles", [])

        if len(json_titles) == 0:
            json_titles.append(url_name)
        elif len(json_titles) == 1 and "Home" in json_titles[0]:
            json_titles = [url_name]

        # Store in dictionary using file name (without extension) as the key
        file_name = os.path.basename(file_path).replace(".json", "").replace("program_", "")
        Titles[file_name] = json_titles


# In[9]:


# Open Chrome
driver = webdriver.Chrome()
sleep(2)
url = 'https://www.google.com'
driver.get(url)
print('- Finish initializing the driver')
sleep(3)

# Switch to the iframe containing the cookie consent popup
try:
    WebDriverWait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it(
        (By.CSS_SELECTOR, "iframe[src^='https://consent.google.com']")
    ))

    print("Please manually reject cookies and press Enter to continue the script...")
    input() 

    driver.switch_to.default_content()
    print("Cookie rejection done. Continuing the script...")

except Exception as e:
    print(f"Could not handle cookie consent: {e}")

sleep(3)


# In[10]:


def get_linkedin_links(program):

    url = 'https://www.google.com'
    driver.get(url)

    program_str = ' OR '.join(f'“{p}”' for p in program)

    query = f'site:linkedin.com/in/ AND "Erasmus Mundus" AND ({program_str}) AND "Student"'

    print(query)

    search_box = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.NAME, "q")))

    search_box.send_keys(query)

    search_box.send_keys(Keys.RETURN)

    print("Search submitted.")

    sleep(20)

    # Retrieve 7 pages of google search output

    num_pages = 7
    links =[]
    for page in range(1, num_pages+1):  # Adjust range as needed for the number of pages
        print(f"Scraping Page {page}...")
        
        # Extract links or perform actions on the current page
        search_results = driver.find_elements(By.CSS_SELECTOR, 'a')
        for link in search_results:
            url = link.get_attribute('href')
            if url and 'linkedin.com/' in url and 'google.com' not in url:
                links.append(url)
        
        # Try to find and click the "Next" button
        try:

            next_button = driver.find_element(By.ID, "pnnext")
            next_page_url = next_button.get_attribute("href")  # Get the link

            print(next_page_url)
            
            if next_page_url:
                driver.get(next_page_url)  # Navigate to next page
            else:
                print("Next button found, but no href attribute.")

            sleep(2)  # Wait for the next page to load
        except NoSuchElementException:
            print("No more pages available.")
            break
    

    return links




# In[12]:


programs_profiles={}

for i in Titles:
    profiles= get_linkedin_links(Titles[i])

    programs_profiles[i] ={
        "prog_title" : Titles[i],
        "profiles_links" : profiles
    }

driver.quit() 



# In[60]:


with open("../data/erasmus_linkedin_profiles/Erasmus_Linkedin_Links.json", "w", encoding="utf-8") as f:
    json.dump(programs_profiles, f, indent=4, ensure_ascii=False)

print("Saved: Erasmus_Linkedin_Links.json")


# Parse profiles

# In[50]:


driver = webdriver.Chrome()

driver.get('https://www.linkedin.com/login')

email = driver.find_element(By.ID, 'username')
email.send_keys("EMAIL_HERE")

password = driver.find_element(By.ID, 'password')
password.send_keys("PASSWORD_HERE")

password.submit()


# In[51]:


import re

def get_profile(url):
    driver.get(url)

    sleep(3)
    
    profile_data = {}

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'lxml')

    # Name
    name_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "h1.inline.t-24.v-align-middle.break-words"))
    )
    name = name_element.text.strip()
    profile_data['name'] = name
    profile_data['url'] = url

    # Headline
    headline = soup.find('div', {'class': 'text-body-medium break-words'})
    headline = headline.get_text().strip()
    profile_data['headline'] = headline

    # About
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'lxml')
    about = soup.find('div', {'class': 'display-flex ph5 pv3'})
    about = about.get_text().strip() if about else None
    profile_data['about'] = about

    # Experience
    sections = soup.find_all('section', {'class': 'artdeco-card pv-profile-card break-words mt2'})
    experience_list = []

    for section in sections:
        # Check if the section is the experience section
        if 'experience' in section.find('div', {'class': 'pv-profile-card__anchor'}).get('id', ''):
            # Extract all job entries within the experience section
            jobs = section.find_all('li', class_='artdeco-list__item')

            for job in jobs:
                title = job.find('div', class_='t-bold')
                title=title.find('span', {'aria-hidden': 'true'})

                company = job.find('span', class_='t-14 t-normal')
                company=company.find('span', {'aria-hidden': 'true'})

                duration = job.find('span', class_='t-14 t-normal t-black--light')
                duration=duration.find('span', {'aria-hidden': 'true'}) if duration else ""

                experience_list.append({
                    'Title': title.text.strip() if title else None,
                    'Company': company.text.strip() if company else None,
                    'Duration': duration.text.strip() if duration else None
                })

    profile_data['experience']=experience_list


    # Education
    try:
        show_all_button = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.ID, 'navigation-index-see-all-education'))
        )
        show_all_button.click()
    except TimeoutException:
        print("Show all button not present within wait time.")

    sleep(3)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    education_entries = soup.find_all('li', class_='pvs-list__paged-list-item')

    education_data = []
    i=0

    # Loop through each entry and extract the necessary information
    for entry in education_entries:
        # Extract institution name, ensuring to avoid duplicate text
        institution = entry.find('div', class_='display-flex align-items-center mr1 hoverable-link-text t-bold')
        institution_name = institution.find('span', {'aria-hidden': 'true'}).text.strip()

        # Extract degree information (ensuring no duplication)
        degree = entry.find('span',class_='t-14 t-normal')
        degree_info = degree.find('span', {'aria-hidden': 'true'}).text.strip() if degree else ""

        # Extract dates (like Sep 2022 - Sep 2023)
        dates = entry.find('span', class_='pvs-entity__caption-wrapper')
        date_range = dates.get_text(strip=True) if dates else None
        
        testt = soup.find_all('li', class_='pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column')

        # Extract coursework details
        details_section = testt[i].find('div', class_='display-flex align-items-center t-14 t-normal t-black')
        details = details_section.find('span', {'aria-hidden': 'true'}).text.strip() if details_section else None
        # print(details_section)

        # Extract skills
        skills_section = testt[i].find('div', class_='display-flex align-items-center t-14 t-normal t-black')
        skills = skills_section.find('span', {'aria-hidden': 'true'}).text.strip() if skills_section else None

        education_data.append({
            'Institution': institution_name,
            'Degree': degree_info,
            'Dates': date_range,
            'Details': details
            # 'Skills': skills
        })

        i+=1


    profile_data["education"]=education_data


    # Honors and Awards
    driver.back()
    page_source = driver.page_source

    soup = BeautifulSoup(page_source, 'html.parser')

    honors_section = soup.find('div', {'id': 'honors_and_awards'})  # Note: Your HTML has 'honors_and_awards', adjust if needed

    honors_list_items = honors_section.find_parent().find_all('li', class_='artdeco-list__item') if honors_section else []

    honors_and_awards = []

    for item in honors_list_items:
        # Extract award name
        award_name = item.select_one('.t-bold span[aria-hidden="true"]')
        if not award_name:
            continue
        
        award_name = award_name.get_text(strip=True)
        
        # Extract date
        date = item.select_one('.t-normal span[aria-hidden="true"]')
        date = date.get_text(strip=True) if date else "N/A"
        
        # Extract associated organization (if available)
        associated_with = item.select_one('.t-black span[aria-hidden="true"]')
        associated_with = associated_with.get_text(strip=True) if associated_with else None
        
        # Extract description (if available)
        description = item.select_one('.inline-show-more-text--is-collapsed span[aria-hidden="true"]')
        description = description.get_text(strip=True) if description else None
        
        honors_and_awards.append({
            "award": award_name,
            "date": date,
            "associated_with": associated_with,
            "description": description
        })

    profile_data["honors_awards"]=honors_and_awards


    # Skills
    try:
        # Find button where ID contains "Show-all" and "skills"
        show_all_skills_button = driver.find_element(
            By.XPATH, '//*[contains(@id, "Show-all") and contains(@id, "skills")]'
        )
        show_all_skills_button.click()

        sleep(3)

        page_source = driver.page_source

        # Use BeautifulSoup to parse the HTML
        soup = BeautifulSoup(page_source, 'html.parser')

        skills_section = soup.find('div', {'class': 'scaffold-layout__inner scaffold-layout-container scaffold-layout-container--reflow'})  # Note: Your HTML has 'honors_and_awards', adjust if needed

        skills = []

        # Find all skill items in the "All" tab (main skills section)
        skill_items = soup.select('li.pvs-list__paged-list-item')

        for item in skill_items:
            # Extract skill name
            skill_name = item.select_one('.t-bold span[aria-hidden="true"]')
            if not skill_name:
                continue
                
            skill_name = skill_name.get_text(strip=True)
            
            # Extract endorsement count if available
            endorsement = item.select_one('.t-black span[aria-hidden="true"]')
            endorsement_count = endorsement.get_text(strip=True) if endorsement else "0"
            
            # Clean endorsement text (remove " endorsement(s)" if present)
            if "endorsement" in endorsement_count:
                endorsement_count = endorsement_count.split()[0]
            
            skill = {
                "skill": skill_name,
                "endorsements": endorsement_count
            }

            if skill not in skills:
                skills.append(skill)
            
        profile_data["skills"]=skills

    except NoSuchElementException:
        print("'Show all skills' button not found.")
        profile_data["skills"]=[]

    return profile_data




# In[29]:


print(get_profile("https://www.linkedin.com/in/elenapirtac/"))


# In[ ]:


profiles = {}


# In[55]:


for i in programs_profiles:
    profiles[i] = {"Title":programs_profiles[i]["prog_title"],
                   "Profiles":[]}
    
    for link in programs_profiles[i]["profiles_links"]:
        profiles[i]["Profiles"].append(get_profile(link))
        sleep(2)



# In[59]:


with open("../data/erasmus_linkedin_profiles/Erasmus_Linkedin_Profiles.json", "w", encoding="utf-8") as f:
    json.dump(profiles, f, indent=4, ensure_ascii=False)

print("Saved: Erasmus_Linkedin_Profiles.json")

