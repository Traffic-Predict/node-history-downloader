# import sqlite3
# import json
#
# # open daejeon_node_xy.sqlite
# conn = sqlite3.connect('daejeon_node_xy.sqlite')
# cur = conn.cursor()
#
# # check table
# cur.execute('SELECT name FROM sqlite_master WHERE type="table"')
# print(cur.fetchall())


import requests
import os
import json
import datetime
import csv

base_url = 'https://www.its.go.kr/opendata/fileDownload/traffic/[YYYY]/[YYYY][MM][DD]_5Min.zip'

start_date_string = input('Enter start date (YYYYMMDD): ')
end_date_string = input('Enter end date (YYYYMMDD): ')

start_date = datetime.datetime.strptime(start_date_string, '%Y%m%d')
end_date = datetime.datetime.strptime(end_date_string, '%Y%m%d')

os.makedirs('tmp', exist_ok=True)
os.makedirs('converted', exist_ok=True)

document = csv.writer(open(f'converted/{start_date_string}_{end_date_string}.csv', 'w'))
error = csv.writer(open('error.txt', 'w'))

for date in [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
    date_string = date.strftime('%Y%m%d')
    url = (base_url.replace('[YYYY]', date.strftime('%Y')).replace('[MM]', date.strftime('%m'))
           .replace('[DD]', date.strftime('%d')))
    print(f'Downloading {date_string}...')
    response = requests.get(url)
    with open(f'tmp/{date_string}.zip', 'wb') as f:
        f.write(response.content)
    os.system(f'unzip -o tmp/{date_string}.zip -d tmp')
    os.system(f'rm tmp/{date_string}.zip')
    print(f'Downloaded {date_string}. Start processing...')
    file_list = os.listdir('tmp')
    for file in file_list:
        if file.endswith('.csv'):
            with open(f'tmp/{file}', 'r') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    loccode = row[2][:3]
                    try:
                        loccode = int(loccode)
                    except ValueError:
                        error.writerow(row)
                        continue
                    if 183 <= loccode <= 187:
                        document.writerow(row)
    os.system('rm tmp/*')
    print(f'Processed {date_string}')
print('Done!')