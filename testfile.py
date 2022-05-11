from operator import le
import requests


url = "http://127.0.0.1:23333/testfile0"
query = ''
ret = requests.get(url)
with open("./testDB/memory/0.block", "wb") as f:
    f.write(ret.content)

url = "http://127.0.0.1:23333/testfile1"
query = ''
ret = requests.get(url)
with open("./testDB/memory/header.hd", "wb") as f:
    f.write(ret.content)
    
url = "http://127.0.0.1:23333/testfile2"
query = ''
ret = requests.get(url)
with open("./testDB/catalog/table_schema.minisql", "wb") as f:
    f.write(ret.content)

url = "http://127.0.0.1:23333/testfile3"
query = ''
ret = requests.get(url)
with open("./testDB/log.txt", "wb") as f:
    f.write(ret.content)