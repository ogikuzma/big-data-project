import requests
import time

while True:
    try:
        r = requests.get("http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/streaming-dataset.csv?op=OPEN")
        break
    except Exception as e:
        time.sleep(3)

r = requests.get("http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/streaming-dataset.csv?op=OPEN")

if r.status_code == 404:
    while True:
        r = requests.put('http://namenode:9870/webhdfs/v1/user/root/data-lake/raw/streaming-dataset.csv?op=CREATE&overwrite=true', allow_redirects=False)
        if 'Location' not in r.headers:
            print("cannot connect with namenode")
            time.sleep(5)
        else:
            break

    print(r.status_code)
    print(r.headers['Location'])

    print('uploading streaming file...')
    file = open('/dataset/vehicles.csv', 'rb')
    r = requests.put(r.headers['Location'], data=file)
    print(r.status_code)

elif r.status_code == 200:
    print('file already uploaded')
