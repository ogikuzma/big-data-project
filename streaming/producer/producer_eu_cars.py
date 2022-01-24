import os
import time
from kafka import KafkaProducer
import kafka.errors
from hdfs import InsecureClient
import json
import sys

HDFS_NAMENODE = os.environ["HDFS_NAMENODE"]
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
TOPIC = "eu_cars"

def convert_line_to_json(schema, line):
    row = '{'
    columns = schema.split(',')

    # description je dat sa navodnicima "blabla,aaa,sdsdas", pa ga je neophodno sacuvati i 
    # transformisati u 'x' da bi split po zarezima radio kako treba
    start = '"'
    end = '"'    
    description = line[line.find(start) + len(start): line.rfind(end)]
    line = line.replace(start + description + end, 'x')
    items = line.split(',')   

    for i in range(len(columns)):
        if columns[i] == 'description':
            row += '"' + columns[i] + '":' + '"' + description.strip() + '"'
        else:
            row += '"' + columns[i].strip() + '":' + '"' + items[i].strip() + '"'
        row += ','

    if row[len(row) - 1] == ',':
        row = row[:-1]
    row += '}'

    print(row)

    return row

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

client = InsecureClient(HDFS_NAMENODE)

file_location = '/user/root/data-lake/raw/streaming-join-dataset.csv'
schema = None

while True:
    try:
        client.status(file_location)
        break
    except Exception as e:
        print(file_location + " does not exist")
        time.sleep(3)

with client.read(file_location, encoding='utf-8', delimiter='\r') as reader:
    for index, line in enumerate(reader):
        if(index == 0):
            schema = line
            continue
        id = line.split(",")[0]

        try:
            row = convert_line_to_json(schema, line)
            print('sending data to topic...')
            print(row)
            producer.send(TOPIC, key=bytes(id, 'utf-8'), value=bytes(row, 'utf-8'))
            time.sleep(1.5)
        except Exception as e:
            print('Exception...')
            pass

        