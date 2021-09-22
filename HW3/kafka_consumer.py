#! /usr/bin/env python

from kafka import KafkaConsumer
import pandas as pd
import json
import os.path

consumer = KafkaConsumer(
    'sampleTopic2',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     consumer_timeout_ms=1000,
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

file_csv = 'price_top10.csv'

if os.path.isfile(file_csv):
    df = pd.read_csv(file_csv, delimiter =';')
else:
    df = pd.DataFrame()

cnt = 0

for message in consumer:
    row = pd.DataFrame(pd.json_normalize(message.value['data']))
    row['event'] = message.value['event']
    df = df.append(row, ignore_index=True)
    cnt += 1

consumer.close()

if (cnt > 0):
    df2 = df.sort_values('price', ascending=False)
    df3 = df2.head(10)
    df3.to_csv(file_csv, index=False, sep =';')
    df4 = pd.DataFrame(df3, columns=['id', 'datetime', 'amount', 'price'])
    print(df4)
else:
    if os.path.isfile(file_csv):
        df4 = pd.DataFrame(df, columns=['id', 'datetime', 'amount', 'price'])
        print(df4)
