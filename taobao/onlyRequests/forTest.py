import requests,re
from pyquery import PyQuery as pq

res = requests.get('http://127.0.0.1:5000/get').text
print(res)



