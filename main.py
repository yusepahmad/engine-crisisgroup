import requests
import time
import json

from bs4 import BeautifulSoup
from datetime import datetime
from loguru import logger
from kafka.producer import KafkaProducer

sekarang = datetime.now()
format_ymd_hms = sekarang.strftime("%Y-%m-%d %H:%M:%S")


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

import datetime

# Daftar nama bulan dalam bahasa Inggris
months = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December"
}


now = datetime.datetime.now()
current_month = now.month
month_name = months[current_month -1 ]


for data in response:
    i = data['tid']
    response = requests.get(f'https://www.crisisgroup.org/crisiswatch/database?location[]={i}')
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
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
                    if date_stat_text == (dateCode := month_name + ' ' + str(now.year)): 
                        status_data.append(
                            {
                                'status': stat_text,
                                'status_date': date_stat_text
                            }
                        )
                all_stat.append(status_data)

            separator = ', '


            for date, region, countries, detail_raw, status_raw  in zip(h1, h2, h3, data_details, all_stat):
                if dateCode == (dateHTML := date.text.strip().replace('\n','')): 
                    file_name = f'{countries.text}_{date.text}.json'

                    file_name = file_name.strip().replace('\n','').lower().replace('  ','').replace(' ','')
                    result = {
                            'link':f'https://www.crisisgroup.org/crisiswatch/database?location[]={i}',
                            'tag': ['crisis', 'crisisgroup.org'],
                            'domain':'crisisgroup.org',
                            'region': region.text.strip().replace('\n',''),
                            'countries': countries.text.strip().replace('\n',''),
                            'date': dateHTML,
                            'status': status_raw[0]['status'],
                            'title':detail_raw[0],
                            'content': separator.join(detail_raw[1]),
                            "crawling_time": format_ymd_hms,
                            "crawling_time_epoch": int(time.time())
                    }

                    def kafka_producer():
                        producer = KafkaProducer(
                            bootstrap_servers="kafka01.research.ai:9092,kafka02.research.ai:9092,kafka03.research.ai:9092"
                        )
                        return producer
                    
                    serialized_message = json.dumps(result).encode('utf-8')
                    
                    try:
                        producer = kafka_producer()
                        producer.send("data-knowledge-repo-general_1", value=serialized_message)
                        logger.success(f"[!!] Data Berhasil di produce {result['date']}")
                        print(format_ymd_hms)
                    except Exception as e:
                        logger.error(f"[!!] {e}")
                    
    print(f'\nnext page country {i}\n')





