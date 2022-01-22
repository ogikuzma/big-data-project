import requests
import time

r_batch = None
r_children = None

while True:
    try:
        r_batch = requests.get("http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/batch-dataset.csv?op=OPEN")
        r_children = requests.get("http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/illinois-children.csv?op=OPEN")
        break
    except Exception as e:
        print("cannot connect to namenode...")
        time.sleep(3)

if r_batch.status_code == 404:
    while True:
        r = requests.put('http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/batch-dataset.csv?op=CREATE&overwrite=true', allow_redirects=False)
        if 'Location' not in r.headers:
            print("cannot upload to namenode...")
            time.sleep(3)
        else:
            break

    print(r.status_code)
    print(r.headers['Location'])

    print('uploading batch dataset ...')

    file = open('/datasets/CIS_Automotive_Kaggle_Sample.csv', 'rb')
    while True:
        try:
            r = requests.put(r.headers['Location'], data=file)
            print(r.status_code)
            break
        except Exception as e:
            print("cannot upload a file...")
            time.sleep(3)

elif r_batch.status_code == 200:
    print('file already uploaded')


if r_children.status_code == 404:
    while True:
        r = requests.put('http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/illinois-children.csv?op=CREATE&overwrite=true', allow_redirects=False)
        if 'Location' not in r.headers:
            print("cannot upload to namenode...")
            time.sleep(3)
        else:
            break

    print(r.status_code)
    print(r.headers['Location'])

    print('uploading column with num of children born ...')

    file = open('/datasets/illinois-children.csv', 'rb')
    while True:
        try:
            r = requests.put(r.headers['Location'], data=file)
            print(r.status_code)
            break
        except Exception as e:
            print("cannot upload a file...")
            time.sleep(3)

elif r_children.status_code == 200:
    print('file already uploaded')

    
