from os import access
import requests
import json

from datetime import datetime
from datetime import timedelta
from dateutil import parser

# Remote configuration
base_url = "https://apiservice.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False

startTime = parser.parse("2023-03-01T08:15Z")
endTime = parser.parse("2023-03-04T08:15Z")

meter_serials = ['0123995130','0123320617']
channel_keys = ['energy.interval.wh.q14']

# Utils
def to_epoch_milli(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    
def print_req(r,compact=True):
    print("  > "+r.request.url)
    if(compact):
        print("  <[%d:%d] %s" % (r.status_code, len(r.text), str(r.text)[:str_max]))
    else:
        print("  <[%d:%d" % (r.status_code, len(r.text)))
        print("    h: %s" % (str(r.headers)))
        print("    b: %s" % (str(r.text)[:str_max*2]))

print("#### Query historical data from multiple meters ####")

# Login
print('> Logging in to '+base_url+' with '+username+'...')
r = requests.post(base_url+'/login', auth=(username,password))
print_req(r, False)
access_token = r.json()['access_token']
access_cookies = r.cookies

print('> Querying facilities...')
r = requests.get(base_url+'/facilities', headers={'token': access_token}, cookies=access_cookies)
print_req(r, False)

#dump it to a file
with open("facilities.json", "w") as f1:
    f1.write(json.dumps(r.json(), indent=2))

print('> Querying meters...')
r = requests.get(base_url+'/meters', headers={'token': access_token}, cookies=access_cookies)
print_req(r)

#dump it to a file
with open("meters.json", "w") as f1:
    f1.write(json.dumps(r.json(), indent=2))

print('> Querying hist...')
r = requests.get(base_url+'/history',
    headers={
        'token': access_token},
    params={
        'meterSerials': meter_serials,
        'channelKeys': channel_keys,
        'start': to_epoch_milli(startTime),
        'end': to_epoch_milli(endTime),
    }, cookies=access_cookies)
print_req(r)

#dump it to a file
with open("hist.json", "w") as f1:
    f1.write(json.dumps(r.json(), indent=2))

# Logout
print('> Logging out...')
r = requests.get(base_url+'/logout', headers={'token': access_token}, cookies=access_cookies)
print_req(r)
