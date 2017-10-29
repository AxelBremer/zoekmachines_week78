from elasticsearch import Elasticsearch
from datetime import datetime
import json
import nltk
from nltk.corpus import stopwords
import re
import os
es = Elasticsearch(hosts=['axelbremer.ddns.net:9200'])

def clear():
    os.system( 'cls' )

def fix_date(date_time):
    date, time = date_time.split()

    day, month, year = date.split('-')

    if month == 'JAN':
        month = '01'
    elif month == 'FEB':
        month = '02'
    elif month == 'MAR':
        month = '03'
    elif month == 'APR':
        month = '04'
    elif month == 'MAY':
        month = '05'
    elif month == 'JUN':
        month = '06'
    elif month == 'JUL':
        month = '07'
    elif month == 'AUG':
        month = '08'
    elif month == 'SEP':
        month = '09'
    elif month == 'OCT':
        month = '10'
    elif month == 'NOV':
        month = '11'
    elif month == 'DEC':
        month = '12'

    newdate = '-'.join((day, month, year))

    return newdate

filelist = os.listdir('json-data')

totfiles = len(filelist)

j = 0

for name in filelist:
    filename = 'json-data/' + name

    j += 1

    with open(filename) as json_data:
        json_docs = json.load(json_data)

        total = len(json_docs)
        i = 0
        for json_doc in json_docs:
            i += 1
            print j, '/', totfiles
            print i, '/', total
            my_id = json_doc['attrs']['newid']
            if json_doc['attrs']['topics'] == "YES":
                json_doc['hastopics'] = True
            else:
                json_doc['hastopics'] = False
            json_doc.pop('attrs')

            old_date = json_doc['date'].split('.')[0]

            old_date = fix_date(old_date)

            d = datetime.strptime(old_date, '%d-%m-%Y')

            new_date = d.strftime('%Y-%m-%d')

            json_doc['date'] = new_date

            body = re.sub(r'\d+', '', json_doc['body'])

            bodytoken = re.compile('\w+').findall(body)

            bodytoken = [word for word in bodytoken if word not in stopwords.words('english')]

            json_doc['bodytoken'] = bodytoken

            es.index(index='reuters2', doc_type='generated', id=my_id, body=json.dumps(json_doc))
            clear()
        print 'done'
