### Import used libraries
from urllib.parse import quote
import sys
import pandas as pd
import re
import time
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from googletrans import Translator

#-----------------------------------------------------get base countries data script-----------------------------------------------------
### Get ISO2CODE, Country_Head_Name_EN and Country_URL ===== (input --> iso URL)
def get_country_data(url):
    country_list = []

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'class': 'wikitable sortable'})
    if table is None:
        print("Table not found. Check the class name and the structure of the HTML.")
    else:
        for row in table.find_all('tr')[1:]:  
            cells = row.find_all('td')
            if len(cells) >= 2:
                code = cells[0].text.strip()
                country_cell = cells[1]
                country = country_cell.text.strip()
                link = country_cell.find('a')
                country_url = 'https://en.wikipedia.org/' + link['href'] if link else None
                country_list.append({'ISO2CODE': code, 'Country_Head_Name_EN': country, 'Country_URL': country_url})
    return country_list

my_url = "https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2"
government_data = get_country_data(my_url)
#government_data


### Append ID, ISO2CODE, Country_Head_Name_EN, Country_URL in df
### Remove Antarctica and Heard Island and McDonald Islands from countries
my_url = "https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2"
df = pd.DataFrame(get_country_data(my_url), columns=['ISO2CODE', 'Country_Head_Name_EN', 'Country_URL'])
values_to_remove = ['Antarctica', 'Heard Island and McDonald Islands']
df = df[~df['Country_Head_Name_EN'].isin(values_to_remove)]
df.insert(0, 'ID', range(1, len(df) + 1))



### Updating some countries URLs
url_mapping = {
    'Western Sahara': 'https://en.wikipedia.org/wiki/Sahrawi_Arab_Democratic_Republic',
    'French Guiana': 'https://en.wikipedia.org/wiki/French_Guiana',
    'Guadeloupe': 'https://en.wikipedia.org/wiki/Guadeloupe',
    'Martinique': 'https://en.wikipedia.org/wiki/Martinique',
    'Réunion': 'https://en.wikipedia.org/wiki/R%C3%A9union',
    'Svalbard and Jan Mayen': 'https://en.wikipedia.org/wiki/Svalbard',
    'Taiwan, Province of China': 'https://en.wikipedia.org/wiki/Taiwan',
    'Mayotte': 'https://en.wikipedia.org/wiki/Mayotte'
}

def assign_country_url(row):
    country_name = row['Country_Head_Name_EN']
    if country_name in url_mapping:
        return url_mapping[country_name]
    else:
        return row['Country_URL']

df['Country_URL'] = df.apply(assign_country_url, axis=1)

taiwan_url = df.loc[df['Country_Head_Name_EN'] == 'Taiwan, Province of China', 'Country_URL'].values[0]
#print(taiwan_url)


### Get the Arabic wikipedia link ===== (input --> English wikipedia link for person or country)
def get_arabic_wikipedia_link(english_url):
    if pd.isna(english_url):
        return None
    try:
        response = requests.get(english_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        language_links = soup.find_all('a', {'class': 'interlanguage-link-target'})
        for link in language_links:
            if 'العربية' in link.text:
                return link['href']
        return None
    except Exception as e:
        print(f"Error fetching Arabic link for {english_url}: {e}")
        return None

english_url = 'https://en.wikipedia.org/wiki/Anguilla'
arabic_url = get_arabic_wikipedia_link(english_url)
#print(f"Arabic Wikipedia link: {arabic_url}")


### Apply get_arabic_wikipedia_link function to the df to add Arabic_URL column
df['Arabic_URL'] = df['Country_URL'].apply(get_arabic_wikipedia_link)
#print(df)


### Get country/person arabic name ===== (input --> Arabic URL for person or country)
def get_country_name_arabic(arabic_url):
    response = requests.get(arabic_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    header = soup.find('h1', id='firstHeading')
    if header:
        return header.text.strip()
    else:
        return "Country name not found."

arabic_url = 'https://ar.wikipedia.org/wiki/%D8%A7%D9%84%D8%A5%D9%85%D8%A7%D8%B1%D8%A7%D8%AA_%D8%A7%D9%84%D8%B9%D8%B1%D8%A8%D9%8A%D8%A9_%D8%A7%D9%84%D9%85%D8%AA%D8%AD%D8%AF%D8%A9'
country_name_arabic = get_country_name_arabic(arabic_url)
#print(f"Country name in Arabic: {country_name_arabic}")


### Apply get_country_name_arabic to df to add Country_Head_Name_AR column
df['Country_Head_Name_AR'] = df['Arabic_URL'].apply(get_country_name_arabic)


### Get Government Data (Designation_EN, Person_Name_EN)  ===== (input --> country URL)
def fetch_government_data(url):
    base_url = 'https://en.wikipedia.org'
    gov_data = []
    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    gove_found = False
    gov_section = soup.find('th', text='Government')
    if gov_section:
        current_row = gov_section.find_parent('tr').find_next_sibling('tr')
        while current_row:
            position_tag = current_row.find('th')
            name_tag = current_row.find('td')
            if position_tag and position_tag.text.strip().startswith('•') and name_tag:
                for br in soup.find_all('br'):
                    br.replace_with('\n')
                position = position_tag.text.strip().replace('•', '').strip()
                position = position.replace('\xa0', ' ')
                position = re.sub(r'\(.*?\)', '', position).strip()
                position = re.sub(r'\[.*?\]', '', position).strip()
                position = position.strip()
                position = re.sub(r'\s+', ' ', position).strip()
                position = position.replace('\n', ' ')
                position = ' '.join(position.split())
                name_tag_text = re.sub(r'\(.*?\)', '', name_tag.text).strip()
                names = []
                urls = []
                li_elements = name_tag.find_all('li')
                if li_elements:
                    for li in li_elements:
                        name = li.text.strip()
                        name = re.sub(r'\[\w\]', '', name).strip()
                        name = re.sub(r'\(.*?\)', '', name).strip()
                        if name:
                            names.append(name)
                        added = False
                        for a in li.find_all('a', href=True):
                            if '[' not in a.text and a.text.strip() in name:
                                urls.append(base_url + a['href'])
                                added = True
                                break
                        if not added:
                            urls.append('')
                else:
                    name = name_tag.text.strip()
                    name = re.sub(r'\[\w\]', '', name).strip()
                    name = re.sub(r'\(.*?\)', '', name).strip()
                    if name:
                        names.append(name)
                    added = False
                    for a in name_tag.find_all('a', href=True):
                        if '[' not in a.text and a.text.strip() in name:
                            urls.append(base_url + a['href'])
                            added = True
                            break
                    if not added:
                        urls.append('')
                person_url = '***'.join(urls) if urls else None
                if position:
                    gov_data.append({
                        "Designation_EN": position,
                        "Person_Name_EN": names,
                        "Person_URL": urls
                    })
            
            current_row = current_row.find_next_sibling('tr')

            if current_row:
                next_position_tag = current_row.find('th')
                if next_position_tag and not next_position_tag.text.strip().startswith('•'):
                    gove_found = True
                    break
    else:
        return [{
            "Designation_EN": "Not Available",
            "Person_Name_EN": "Not Available",
            "Person_URL": None
        }]
    if not gov_data:
        print(f"No valid government data found for {url}")
    return gov_data

url = 'https://en.wikipedia.org/wiki/United_Arab_Emirates'
government_data = fetch_government_data(url)
# for gov_data in government_data:
#     print(gov_data)


### To append the Person_Name_EN list and Person_URL list
def expand_data(gov_data):
    processed_data = []
    for record in gov_data:
        designation = record['Designation_EN']
        names = record['Person_Name_EN']
        urls = record['Person_URL']
        if not names or not urls: 
            continue
        if len(names) > 1 and designation.endswith('s'):
            designation = designation[:-1]
        if isinstance(urls, str): 
            urls = [urls]
        for name, url in zip(names, urls):
            processed_data.append({
                'Designation_EN': designation,
                'Person_Name_EN': name,
                'Person_URL': url if url else 'None'
            })

    df_expanded = pd.DataFrame(processed_data)
    return df_expanded

url = 'https://en.wikipedia.org/wiki/United_Arab_Emirates'
government_data = fetch_government_data(url)
expanded_data = expand_data(government_data)
df_expanded = pd.DataFrame(expanded_data)
#print(df_expanded)


### Add Designation_EN, Person_Name_EN and Person_URL to the df (df_2)
all_data = []
for index, row in df.iterrows():
    country_url = row['Country_URL']
    government_data = fetch_government_data(country_url)
    expanded_data = expand_data(government_data)
    for _, data in expanded_data.iterrows():
        all_data.append({
            'ID': row['ID'],
            'ISO2CODE': row['ISO2CODE'],
            'Country_Head_Name_EN': row['Country_Head_Name_EN'],
            'Country_Head_Name_AR': row['Country_Head_Name_AR'],
            'Designation_EN': data['Designation_EN'],
            'Person_Name_EN': data['Person_Name_EN'],
            'Person_URL': data['Person_URL']
        })

df_2 = pd.DataFrame(all_data)
#print(df_2)


### Apply get_arabic_wikipedia_link function to the df to add Arabic_Person_URL column
df_2['Arabic_Person_URL'] = df_2['Person_URL'].apply(
    lambda url: get_arabic_wikipedia_link(url) if pd.notna(url) and url != 'None' else None
)
#print(df_2)


### Apply get_country_name_arabic to df to add Person_Name_AR column
df_2['Person_Name_AR'] = df_2['Arabic_Person_URL'].apply(
    lambda url: get_country_name_arabic(url) if pd.notna(url) and url.startswith('https://') else None
)
#print(df_2)


### Add Designation_ID
designation_id_dict = {}

def assign_designation_id(row):
    country_id = row['ID']
    designation = row['Designation_EN']
    if country_id not in designation_id_dict:
        designation_id_dict[country_id] = {}
    if designation not in designation_id_dict[country_id]:
        designation_id_dict[country_id][designation] = len(designation_id_dict[country_id]) + 1
    return designation_id_dict[country_id][designation]

df_2['Designation_ID'] = df_2.apply(assign_designation_id, axis=1)
#print(df_2)


### Check if the Assumed_Office_Date is a valid date or not
def has_date(text):
    date_patterns = [
        r'\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}', 
        r'\d{1,2} (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{4}', 
        r'\d{1,2}\/\d{1,2}\/\d{4}',  
        r'\d{1,2}-\d{1,2}-\d{4}',  
        r'\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}\s*–\s*[a-zA-Z]+',  
    ]
    for pattern in date_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


### Get the Assumed Office Date and Image URL ===== (input --> person URL)
def get_person_data_main(url):
    assumed_office = 'Not Available'
    image_url = 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg'
    person_data = []
    try: 
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        counter = 0
        tables = soup.find_all('table')
        
        found = False
    
        for table in tables:
            trs = table.find_all("tr")
            for tr in trs:
                th = tr.find("th")
                td = tr.find("td")
                # image
                img_tag = tr.find("img")
                if img_tag:
                    img_src = img_tag['src']
                    if '.svg' not in img_src:
                        width = int(img_tag.get('width', 0))
                        height = int(img_tag.get('height', 0))
                        
                        if width > 150 and height > 150:
                            image_url = img_src
                            image_link_without_thumb = image_url.replace('/thumb', '')
                            last_slash_index = image_link_without_thumb.rfind('/')
                            final_url = image_link_without_thumb[:last_slash_index]
                            image_url = "https:" + final_url
                # date
                if td and "assumed office" in td.text.lower():
                    br_tag = td.find("br")
                    if br_tag:
                        assumed_office = br_tag.next_sibling.strip()
                        found = True
                        break  
                elif td and "in office" in td.text.lower():
                    br_tag = td.find("br")
                    if br_tag:
                        assumed_office = br_tag.next_sibling.strip()
                        found = True
                        break 
                elif td and "incumbent" in td.text.lower():
                    br_tag = td.find("br")
                    if br_tag:
                        assumed_office = br_tag.next_sibling.strip()
                        found = True
                        break
                # for the Vatican City
                elif th and "papacy began" in th.text.strip().lower():
                    if td:
                        assumed_office = td.text.strip()
                        found = True
                        break
                # for the Vatican City
                elif th and "appointed" in th.text.strip().lower():
                    if td:
                        assumed_office = td.text.strip()
                        found = True
                        break
                elif th and "reign" in th.text.lower():
                    if td:
                        assumed_office = td.text.strip()
                        found = True
                        break
                elif th and "incumbent" in th.text.lower():
                    if td:
                        assumed_office = td.text.strip()
                        found = True
                        break
                else:
                    assumed_office = 'Not Available'
            counter += 1
            if counter % 100 == 0:
                time.sleep(5)
            if found: 
                break
            else:
                image_url = 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg'
    except:
        pass
    person_data.append({
                "Assumed_Office_Date" : assumed_office if has_date(assumed_office) else 'Not Available',
                "Image_URL" : image_url
            })
    return person_data

person_url = 'https://en.wikipedia.org/wiki/Holy_See'
person_data = get_person_data_main(person_url)
#person_data


### Add Assumed_Office_Date and Image_URL to the df (df_3)
all_gov_data2 = []
for index, row in df_2.iterrows():
    person_url = row['Person_URL']
    person_data = get_person_data_main(person_url)
    for data in person_data:
        all_gov_data2.append({
            'ID': row['ID'],
            'ISO2CODE': row['ISO2CODE'],
            'Country_Head_Name_EN': row['Country_Head_Name_EN'],
            'Country_Head_Name_AR': row['Country_Head_Name_AR'],
            'Designation_EN': row['Designation_EN'],
            'Person_Name_EN': row['Person_Name_EN'],
            'Person_Name_AR': row['Person_Name_AR'],
            'Designation_ID': row['Designation_ID'],
            'Assumed_Office_Date': data['Assumed_Office_Date'],
            'Image_URL': data['Image_URL']
        })

df_3 = pd.DataFrame(all_gov_data2)
#print(df_3)


### Merge the duplicated names in 1 row === Output(df_4)
def aggregate_designations(group):
    combined_designation = '/'.join(group['Designation_EN'].unique())
    assumed_office_dates = group['Assumed_Office_Date'].tolist()
    assumed_office_date = next((date for date in assumed_office_dates if date != 'Not Available'), 'Not Available')
    image_urls = group['Image_URL'].tolist()
    image_url = next((url for url in image_urls if 'dataverse' not in url), 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg')

    return pd.Series({
        'ID': group['ID'].iloc[0],
        'ISO2CODE': group['ISO2CODE'].iloc[0],
        'Designation_ID': group['Designation_ID'].min(),
        'Country_Head_Name_EN': group['Country_Head_Name_EN'].iloc[0],
        'Country_Head_Name_AR': group['Country_Head_Name_AR'].iloc[0],
        'Designation_EN': combined_designation,
        'Person_Name_EN': group['Person_Name_EN'].iloc[0],
        'Person_Name_AR': group['Person_Name_AR'].iloc[0],
        'Assumed_Office_Date': assumed_office_date,
        'Image_URL': image_url
    })

df_4 = df_3.groupby(['ISO2CODE', 'Person_Name_EN']).apply(aggregate_designations).reset_index(drop=True)
#print(df_4)


### Sort the data using 'ID' and 'Designation_ID'
df_4 = df_4.sort_values(by=['ID', 'Designation_ID'])
#df_4


### Add Person_ID
df_4['Person_ID'] = df_4.groupby('ID').cumcount() + 1
#df_4.head(10)


### Save df_4 in df_main_country_heads
df_main_country_heads = df_4.copy()


### Print end line
print('Get base countries data part --> Done')


#-----------------------------------------------------get defense ministers data script-----------------------------------------------------
### Get State, Defense_Minister_Name, and Defense_Minister_url ===== (input --> wiki link for defense ministers, table index)
def extract_defense_ministers_from_table(url, table_index=0):
    base_url = "https://en.wikipedia.org"
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve the webpage.")
        return None
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    if len(tables) <= table_index:
        print("The specified table index does not exist.")
        return None
    table = tables[table_index]
    data = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) >= 3:
            state = cells[0].get_text(strip=True)
            defense_minister_cell = cells[2]
            defense_minister_name = defense_minister_cell.get_text(strip=True)
            defense_minister_name = re.sub(r'\[.*?\]', '', defense_minister_name).strip()
            defense_minister_name = re.sub(r'\(.*?\)', '', defense_minister_name).strip()
            if not defense_minister_name or defense_minister_name == '—'or defense_minister_name.startswith('—'):
                continue
            link = defense_minister_cell.find("a")
            defense_minister_link = link.get('href') if link else None
            if defense_minister_link:
                defense_minister_link = base_url + defense_minister_link
            row_data = {
                "State": state,
                "Defense_Minister_Name": defense_minister_name,
                "Defense_Minister_url": defense_minister_link
            }
            data.append(row_data)
    return data

url = "https://en.wikipedia.org/wiki/List_of_current_defence_ministers"
data_second_table = extract_defense_ministers_from_table(url, table_index=1)
# if data_second_table is not None:
#     for item in data_second_table:
#         print(item)


### Uses the function to get the first table data and save it in df_first_table dataframe
url = "https://en.wikipedia.org/wiki/List_of_current_defence_ministers"
data_first_table = extract_defense_ministers_from_table(url,  table_index=0)
df_first_table  = pd.DataFrame(data_first_table)
#print(df_first_table )


### Uses the function to get the second table data and save it in df_second_table dataframe
url = "https://en.wikipedia.org/wiki/List_of_current_defence_ministers"
data_second_table = extract_defense_ministers_from_table(url,  table_index=1)
df_second_table  = pd.DataFrame(data_second_table)
#print(df_second_table )


### Renames Taiwan to be 'Taiwan, Province of China' instead of 'Taiwan (Republic of China)'
df_second_table['State'] = df_second_table['State'].replace(
    {
        'Taiwan (Republic of China)': 'Taiwan, Province of China'
    }
)
#print(df_second_table)


### Combines both dataframes of the first and second tables and put the result in df_combined dataframe
df_combined = pd.concat([df_first_table, df_second_table], ignore_index=True)
#print(df_combined)


### Apply get_arabic_wikipedia_link function to the df_combined to add Defense_Minister_url_AR column
df_combined['Defense_Minister_url_AR'] = df_combined['Defense_Minister_url'].apply(
    lambda url: get_arabic_wikipedia_link(url) if pd.notna(url) and url != 'None' else None
)
#print(df_combined)


### Apply get_country_name_arabic to df_combined to add Defense_Minister_Name_AR column
df_combined['Defense_Minister_Name_AR'] = df_combined['Defense_Minister_url_AR'].apply(
    lambda url: get_country_name_arabic(url) if pd.notna(url) and url.startswith('https://') else None
)
#print(df_combined)


### Gets the image link of the defense minister ===== (input --> minister URL)
def get_person_minister_data(url):
    image_url = 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg'
    person_data = []
    try: 
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        tables = soup.find_all('table')
        found = False
        for table in tables:
            trs = table.find_all("tr")
            for tr in trs:
                th = tr.find("th")
                td = tr.find("td")
                # image
                img_tag = tr.find("img")
                if img_tag:
                    img_src = img_tag['src']
                    if '.svg' not in img_src:
                        width = int(img_tag.get('width', 0))
                        height = int(img_tag.get('height', 0))
                        image_url = img_src
                        image_link_without_thumb = image_url.replace('/thumb', '')
                        if image_link_without_thumb.count('.') > 3:
                            last_slash_index = image_link_without_thumb.rfind('/')
                            image_link_without_thumb = image_link_without_thumb[:last_slash_index]
                        image_url = "https:" + image_link_without_thumb
                        found = True
                        break
            if found: 
                break
        if not found:
            image_url = extract_large_jpg_image_link(url)
    except Exception as e:
        print(f"Error processing {url}: {e}")
    person_data.append({
        "Image_URL" : image_url
    })
    return person_data

def extract_large_jpg_image_link(url):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to load page {url}")
        soup = BeautifulSoup(response.content, 'html.parser')
        img_tags = soup.find_all('img')
        for img_tag in img_tags:
            img_url = img_tag.get('src')
            if not img_url or not img_url.lower().endswith('.jpg'):
                continue
            try:
                width = int(img_tag.get('width', 0))
                height = int(img_tag.get('height', 0))
            except ValueError:
                continue  
            if width > 100 and height > 100:
                image_url = img_url
                image_link_without_thumb = image_url.replace('/thumb', '')
                
                if image_link_without_thumb.count('.') > 3:
                    last_slash_index = image_link_without_thumb.rfind('/')
                    image_link_without_thumb = image_link_without_thumb[:last_slash_index]
                    
                image_url = "https:" + image_link_without_thumb
                
                return image_url
        
        return 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg'
    except Exception as e:
        print(f"Error searching for image in {url}: {e}")
        return 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg'
person_url = 'https://en.wikipedia.org/wiki/Bernardino_Soto'
person_data = get_person_minister_data(person_url)
#print(person_data)


'''Loop over the df_combined dataframe and use the Defense_Minister_url as input for the get_person_minister_data function 
to get the image URL of the minister and save the result in df_defense_ministers_data dataframe'''
all_defense_ministers_data = []
for index, row in df_combined.iterrows():
    person_url = row['Defense_Minister_url']
    person_data = get_person_minister_data(person_url)
    for data in person_data:
        all_defense_ministers_data.append({
            'State': row['State'],
            'Defense_Minister_Name': row['Defense_Minister_Name'],
            'Defense_Minister_Name_AR': row['Defense_Minister_Name_AR'],
            'Image_URL': data['Image_URL']
        })
df_defense_ministers_data = pd.DataFrame(all_defense_ministers_data)
#print(df_defense_ministers_data)


### Starts the process of combining the df_defense_ministers_data dataframe with the df_main_country_heads dataframe
### Reads both dataframes in 2 new dataframes
df_first = df_defense_ministers_data.copy()
df_second = df_main_country_heads.copy()


''' Loops over the data of defense ministers and if it finds any matching between the state (from df_first) 
and the Country_Head_Name_EN (from df_second) append the founded minister data in df_new_rows dataframe'''
### Then combined df_second with the appended data in df_new_rows
### Then sortes by ID and Designation_ID and saves to excel sheet Country_Heads_with_Defense_Ministers
def add_defense_ministers(df_first, df_second):
    new_rows_dict = {}
    for index, row in df_first.iterrows():
        state = row['State']
        defense_minister_name = row['Defense_Minister_Name']
        defense_minister_name_ar = row['Defense_Minister_Name_AR']
        image_url = row['Image_URL']
        matching_rows = df_second[df_second['Country_Head_Name_EN'] == state]
        if not matching_rows.empty:
            if state not in new_rows_dict:
                new_row = {
                    'ID': matching_rows.iloc[0]['ID'],
                    'ISO2CODE': matching_rows.iloc[0]['ISO2CODE'],
                    'Country_Head_Name_EN': matching_rows.iloc[0]['Country_Head_Name_EN'],
                    'Country_Head_Name_AR': matching_rows.iloc[0]['Country_Head_Name_AR'],
                    'Designation_EN': 'Defense minister',
                    'Person_ID': None,
                    'Designation_ID': None,
                    'Person_Name_EN': defense_minister_name,
                    'Person_Name_AR': defense_minister_name_ar,
                    'Assumed_Office_Date': None,
                    'Image_URL': image_url
                }
                new_rows_dict[state] = new_row
    df_new_rows = pd.DataFrame(list(new_rows_dict.values()))
    df_combined2 = pd.concat([df_second, df_new_rows], ignore_index=True)
    df_combined2 = df_combined2.sort_values(by=['ID', 'Designation_ID']).reset_index(drop=True)
    return df_combined2
df_country_heads_with_defense_ministers = add_defense_ministers(df_first, df_second)


### Print end line
print('Get defense ministers data part --> Done')


#-----------------------------------------------------get interior ministers data-----------------------------------------------------
### Get State, Interior_Minister_Name, and Interior_Minister_url ===== (input --> wiki link for interior ministers, table index)
def extract_interior_ministers_from_table(url, table_index=0):
    base_url = "https://en.wikipedia.org"
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve the webpage.")
        return None
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    if len(tables) <= table_index:
        print("The specified table index does not exist.")
        return None
    table = tables[table_index]
    data = []
    for row in table.find_all("tr")[1:]: 
        cells = row.find_all("td")
        if len(cells) >= 3: 
            state = cells[0].get_text(strip=True)
            interior_minister_cell = cells[2]
            interior_minister_name = interior_minister_cell.get_text(strip=True)
            #interior_minister_name = re.sub(r'\[\w\]', '', interior_minister_name).strip()
            interior_minister_name = re.sub(r'\[.*?\]', '', interior_minister_name).strip()
            interior_minister_name = re.sub(r'\(.*?\)', '', interior_minister_name).strip()
            if not interior_minister_name or interior_minister_name == '—'or interior_minister_name.startswith('—'):
                continue
            link = interior_minister_cell.find("a")
            interior_minister_link = link.get('href') if link else None
            if interior_minister_link:
                interior_minister_link = base_url + interior_minister_link
            row_data = {
                "State": state,
                "Interior_Minister_Name": interior_minister_name,
                "Interior_Minister_url": interior_minister_link
            }
            data.append(row_data)
    return data

url = "https://en.wikipedia.org/wiki/List_of_current_interior_ministers"
data_second_table = extract_interior_ministers_from_table(url, table_index=1)
# if data_second_table is not None:
#     for item in data_second_table:
#         print(item)


### Uses the function to get the first table data and save it in df_first_table dataframe
url = "https://en.wikipedia.org/wiki/List_of_current_interior_ministers"
data_first_table = extract_interior_ministers_from_table(url,  table_index=0)
df_first_table  = pd.DataFrame(data_first_table)
#print(df_first_table )


### Uses the function to get the second table data and save it in df_second_table dataframe
url = "https://en.wikipedia.org/wiki/List_of_current_interior_ministers"
data_second_table = extract_interior_ministers_from_table(url,  table_index=1)
df_second_table  = pd.DataFrame(data_second_table)
#print(df_second_table )


### Renames Taiwan to be 'Taiwan, Province of China' instead of 'Taiwan (Republic of China)'
df_second_table['State'] = df_second_table['State'].replace(
    {
        #'Somaliland (Republic of Somaliland)': 'Somalia',
        'Taiwan (Republic of China)': 'Taiwan, Province of China'
    }
)
#print(df_second_table)


### Combines both dataframes of the first and second tables and put the result in df_combined dataframe
df_combined = pd.concat([df_first_table, df_second_table], ignore_index=True)
#print(df_combined)


### Apply get_arabic_wikipedia_link function to the df_combined to add Interior_Minister_url_AR column
df_combined['Interior_Minister_url_AR'] = df_combined['Interior_Minister_url'].apply(
    lambda url: get_arabic_wikipedia_link(url) if pd.notna(url) and url != 'None' else None
)
#print(df_combined)


### Apply get_country_name_arabic to df_combined to add Interior_Minister_Name_AR column
df_combined['Interior_Minister_Name_AR'] = df_combined['Interior_Minister_url_AR'].apply(
    lambda url: get_country_name_arabic(url) if pd.notna(url) and url.startswith('https://') else None
)
#print(df_combined)


''' Loop over the df_combined dataframe and use the Interior_Minister_url as input for the get_person_minister_data function 
to get the image URL of the minister and save the result in df_interior_ministers_data dataframe'''
all_interior_ministers_data = []
for index, row in df_combined.iterrows():
    person_url = row['Interior_Minister_url']
    person_data = get_person_minister_data(person_url)
    for data in person_data:
        all_interior_ministers_data.append({
            'State': row['State'],
            'Interior_Minister_Name': row['Interior_Minister_Name'],
            'Interior_Minister_Name_AR': row['Interior_Minister_Name_AR'],
            'Image_URL': data['Image_URL']
        })
df_interior_ministers_data = pd.DataFrame(all_interior_ministers_data)
#print(df_interior_ministers_data)


## Starts the process of combining the Interior_Ministers_Data dataframe with the df_country_heads_with_defense_ministers dataframe
### Reads dataframes in 2 new dataframes
df_first = df_interior_ministers_data.copy() 
df_second =df_country_heads_with_defense_ministers.copy()


### Loops over the data of interior ministers and if it finds any matching between the state (from df_first) and the Country_Head_Name_EN (from df_second) append the founded minister data in df_new_rows dataframe
### Then combined df_second with the appended data in df_new_rows
### Then sortes by ID and Designation_ID and saves to excel sheet Country_Heads_with_Defense_Ministers_and_Interior_Ministers
def add_interior_ministers(df_first, df_second):
    new_rows_dict = {}
    for index, row in df_first.iterrows():
        state = row['State']
        interior_minister_name = row['Interior_Minister_Name']
        interior_minister_name_ar = row['Interior_Minister_Name_AR']
        image_url = row['Image_URL']
        matching_rows = df_second[df_second['Country_Head_Name_EN'] == state]
        if not matching_rows.empty:
            if state not in new_rows_dict:
                new_row = {
                    'ID': matching_rows.iloc[0]['ID'],
                    'ISO2CODE': matching_rows.iloc[0]['ISO2CODE'],
                    'Country_Head_Name_EN': matching_rows.iloc[0]['Country_Head_Name_EN'],
                    'Country_Head_Name_AR': matching_rows.iloc[0]['Country_Head_Name_AR'],
                    'Designation_EN': 'Interior minister',
                    'Person_ID': None,
                    'Designation_ID': None,
                    'Person_Name_EN': interior_minister_name,
                    'Person_Name_AR': interior_minister_name_ar,
                    'Assumed_Office_Date': None,
                    'Image_URL': image_url
                }
                new_rows_dict[state] = new_row
    df_new_rows = pd.DataFrame(list(new_rows_dict.values()))
    df_combined2 = pd.concat([df_second, df_new_rows], ignore_index=True)
    df_combined2 = df_combined2.sort_values(by=['ID', 'Designation_ID']).reset_index(drop=True)
    return df_combined2
df_combined2 = add_interior_ministers(df_first, df_second)


### Update Designation_ID to add values for defense and interior ministers
designation_id_dict = {}
def assign_designation_id(row):
    country_id = row['ID']
    designation = row['Designation_EN']
    if country_id not in designation_id_dict:
        designation_id_dict[country_id] = {} 
    if designation not in designation_id_dict[country_id]:
        designation_id_dict[country_id][designation] = len(designation_id_dict[country_id]) + 1
    return designation_id_dict[country_id][designation]
df_combined2['Designation_ID'] = df_combined2.apply(assign_designation_id, axis=1)
#print(df_combined2)


### Merge the duplicated names in 1 row === Output(df_combined3)
def aggregate_designations(group):
    combined_designation = '/'.join(group['Designation_EN'].unique())
    assumed_office_dates = group['Assumed_Office_Date'].tolist()
    assumed_office_date = next((date for date in assumed_office_dates if date != 'Not Available'), 'Not Available')
    image_urls = group['Image_URL'].tolist()
    image_url = next((url for url in image_urls if 'dataverse' not in url), 'https://dataverse.fcsc.gov.ae/content/Default/no-image.jpg')
    return pd.Series({
        'ID': group['ID'].iloc[0],
        'ISO2CODE': group['ISO2CODE'].iloc[0],
        'Designation_ID': group['Designation_ID'].min(),
        'Country_Head_Name_EN': group['Country_Head_Name_EN'].iloc[0],
        'Country_Head_Name_AR': group['Country_Head_Name_AR'].iloc[0],
        'Designation_EN': combined_designation,
        'Person_Name_EN': group['Person_Name_EN'].iloc[0],
        'Person_Name_AR': group['Person_Name_AR'].iloc[0],
        'Assumed_Office_Date': assumed_office_date,
        'Image_URL': image_url
    })
df_combined3 = df_combined2.groupby(['ISO2CODE', 'Person_Name_EN']).apply(aggregate_designations).reset_index(drop=True)
#print(df_combined3)


### Sort the dataframe by ID then by Designation_ID
df_sorted = df_combined3.sort_values(by=['ID', 'Designation_ID']).reset_index(drop=True)
#df_sorted


### Update Person_ID to add values for defense and interior ministers
df_sorted['Person_ID'] = df_sorted.groupby('ID').cumcount() + 1
#df_sorted.head(10)


### Update the name of King Salman in Saudi Arabia
df_sorted['Person_Name_EN'] = df_sorted['Person_Name_EN'].replace(
    {
        'Salman': 'King Salman'
    }
)
#print(df_sorted)


### Save the dataframe in 'Country_Heads_with_Defense_Ministers_and_Interior_Ministers' excel sheet
df_sorted.to_excel('Country_Heads_with_Defense_Ministers_and_Interior_Ministers.xlsx', index=False)


### Print end line
print('Get defense ministers data part --> Done')