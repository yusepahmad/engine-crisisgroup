import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from s3 import S3

sekarang = datetime.now()
format_ymd_hms = sekarang.strftime("%Y-%m-%d %H:%M:%S")

import requests

cookies = {
    '_gid': 'GA1.2.1998334497.1713256716',
    '_gat_UA-1201493-17': '1',
    '_ga_0BDV0CNSW1': 'GS1.1.1713413295.8.1.1713414573.0.0.0',
    '_ga': 'GA1.2.625019535.1713256686',
}

headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
    # 'cookie': '_gid=GA1.2.1998334497.1713256716; _gat_UA-1201493-17=1; _ga_0BDV0CNSW1=GS1.1.1713413295.8.1.1713414573.0.0.0; _ga=GA1.2.625019535.1713256686',
    'referer': 'https://www.crisisgroup.org/africa/east-and-southern-africa/angola',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

response = requests.get('https://www.crisisgroup.org/ajax/crisiswatch/23213', cookies=cookies, headers=headers).json()

for data in response:
    i = data['tid']
    web = requests.get(f'https://www.crisisgroup.org/crisiswatch/database?location[]={i}').status_code
    if web == 200:
        web : str = requests.get(f'https://www.crisisgroup.org/crisiswatch/database?location[]={i}').text
        soup_one = BeautifulSoup(web, 'html.parser')

        try:
            page_number = soup_one.find('span', class_='u-franklin u-fsn').text.strip()
            page_number = page_number.split('of')
            number = page_number[1].strip()
        except:
            number = 0

        for ii in range(0, int(number)):
            web_page : str = requests.get(f'https://www.crisisgroup.org/crisiswatch/database?location[]={i}&page={ii}').text

            soup = BeautifulSoup(web_page, 'html.parser')

            content = soup.find(class_='c-grouping o-container o-container--cols1 o-container--cols1-r u-mar-b30')

            if 'No Results Found.' in content.text:
                print(f'iterasi {i} No Results Found.')
            else:
                try:
                    h1 = content.find_all('h1')
                    h2 = content.find_all('h2')
                    h3 = content.find_all('h3')
                except:
                    h1 = []
                    h2 = []
                    h3 = []



                details = content.find_all(class_='c-crisiswatch-entry u-pr')

                data_details = []

                for detail in details:
                    try:
                        strong_text = detail.find('strong').get_text(strip=True)
                    except:
                        strong_text = None

                    p_texts = [p.get_text(strip=True) for p in detail.find_all('p')]


                    detail_data = [strong_text, p_texts]
                    data_details.append(detail_data)

                all_stat = []

                stat = content.find_all('div', class_='o-crisis-states u-df')
                for status_content in stat:
                    status = status_content.find_all('div', class_='u-pad-l10 u-tal')
                    status_date = status_content.find_all('div', class_='u-df u-width-100 u-mar-t5 u-gray--primary')

                    status_data = []

                    for stat, date_stat in zip(status, status_date):
                        stat_text = stat.get_text(strip=True)
                        date_stat_text = date_stat.get_text(strip=True)
                        status_data.append(
                            {
                                'status': stat_text,
                                'status_date': date_stat_text
                            }
                        )
                    all_stat.append(status_data)

                separator = ', '


                for date, region, countries, detail_raw, status_raw  in zip(h1, h2, h3, data_details, all_stat):

                    file_name = f'{countries.text}_{date.text}.json'

                    file_name = file_name.strip().replace('\n','').lower().replace('  ','').replace(' ','')

                    result = {
                            'link':'https://www.crisisgroup.org/',
                            'tag': ['crisis'],
                            'domain':'crisisgroup.org',
                        'region': region.text.strip().replace('\n',''),
                        'countries': countries.text.strip().replace('\n',''),
                        'date': date.text.strip().replace('\n',''),
                        'status': status_raw,
                        'title':detail_raw[0],
                        'content': separator.join(detail_raw[1]),
                        'file_name':file_name,
                        'path_data_raw': f's3://ai-pipeline-statistics/data/data_raw/Divtik/crisis group/global conflict tracker/json/{file_name}',
                        'path_data_clear': f's3://ai-pipeline-statistics/data/data_clean/Divtik/crisis group/global conflict tracker/json/{file_name}',
                        "crawling_time": format_ymd_hms,
                        "crawling_time_epoch": int(time.time())
                    }

                    S3().send_json_s3_v2(result, result['path_data_raw'], result['file_name'])
                print(f'\nnext page number {ii} in page country {i}\n')
    print(f'\nnext page country {i}\n')


