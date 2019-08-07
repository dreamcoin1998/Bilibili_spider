import requests
import json
 
res = requests.get('http://api.bilibili.com/x/reply?type=1&oid=2&pn=118&nohot=1&sort=0')
data = json.loads(res.content)

replys = data['data']['replies']

for reply in replys:
    print(reply['content']['message'])
