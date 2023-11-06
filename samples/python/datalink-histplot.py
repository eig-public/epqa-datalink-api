from os import access
import requests
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json

from datetime import datetime
from datetime import timedelta
from dateutil import parser

# Remote configuration
base_url = "https://apiservice.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False
str_max = 200

startTime = parser.parse("2023-03-01T08:15Z")
endTime = parser.parse("2023-03-04T08:15Z")

mser = '0123995130'
meter_serials = [mser]
chan = 'energy.interval.wh.q14'
channel_keys = [chan]

# Utils
def to_epoch_milli(datetime: datetime):
    return int(datetime.timestamp() * 1000)

def print_req(r,compact=True):
    print("  > "+r.request.url)
    if(compact):
        print("  <[%d:%d] %s" % (r.status_code, len(r.text), str(r.text)[:str_max]))
    else:
        print("  <[%d:%d" % (r.status_code, len(r.text)))
        print("    h: %s" % (str(r.headers)))
        print("    b: %s" % (str(r.text)[:str_max*2]))

print("#### Query historical data from meter, massage into data arrays, and plot ####")

# Login
print('> Logging in to '+base_url+' with '+username+'...')
r = requests.post(base_url+'/login', auth=(username,password))
access_token = r.json()['access_token']
access_cookies = r.cookies
print_req(r,False)

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

#v0
#data_arr = r.json()['meters'][mser]['channels'][chan]['data']
#v1
data_arr = r.json()

# Logout
print('> Logging out...')
r = requests.get(base_url+'/logout', headers={'token': access_token}, cookies=access_cookies)
print_req(r)

meter_data = { mser: { chan: [] } }
for i in range(0, len(data_arr)-1):
    rec = data_arr[i]
    m = rec['meter_serial']
    c = rec['channel_key']
    meter_data[m][c].append(rec)
    

xpoints = []
ypoints = []
sformat = "%Y-%m-%dT%H:%M:%S"
for i in range(0,len(meter_data[mser][chan])-1):
    rec = meter_data[mser][chan][i]
    dt = datetime.strptime(rec['time'], sformat)
    xpoints.append(dt)
    ypoints.append(rec['amount'])

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
# plt.gca().xaxis.set_major_locator(mdates.DateLocator())
plt.plot(xpoints,ypoints)
plt.xticks(rotation=20)
plt.show()
plt.gcf().autofmt_xdate()










    
