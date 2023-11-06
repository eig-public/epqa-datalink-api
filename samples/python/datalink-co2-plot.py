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
str_max = 2000

startTime = parser.parse("2023-09-01T00:00Z")
endTime = parser.parse("2023-10-01T00:00Z")

facuid = '34d68111-d511-4a11-b711-8a1c16761111'
facs = [facuid]

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

print("#### Query co2 data from facility, massage into data arrays, and plot ####")

# Login
print('> Logging in to '+base_url+' with '+username+'...')
r = requests.post(base_url+'/login', auth=(username,password))
access_token = r.json()['access_token']
access_cookies = r.cookies
print_req(r,False)

print('> Querying co2...')
r = requests.get(base_url+'/co2',
    headers={
        'token': access_token},
    params={
        'facilityUids': facs,
        'start': to_epoch_milli(startTime),
        'end': to_epoch_milli(endTime),
    }, cookies=access_cookies)
print_req(r)

#v1
data_arr = r.json()
# { typename:"", time:"", facility_uid:"", amount:0 }

# Logout
print('> Logging out...')
r = requests.get(base_url+'/logout', headers={'token': access_token}, cookies=access_cookies)
print_req(r)

fac_data = { facuid: [] }
for i in range(0, len(data_arr)-1):
    rec = data_arr[i]
    m = rec['facility_uid']
    fac_data[m].append(rec)

xpoints = []
ypoints = []
sformat = "%Y-%m-%dT%H:%M:%S"
for i in range(0,len(fac_data[facuid])-1):
    rec = fac_data[facuid][i]
    dt = datetime.strptime(rec['time'], sformat)
    xpoints.append(dt)
    ypoints.append(rec['amount'])

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
# plt.gca().xaxis.set_major_locator(mdates.DateLocator())
plt.plot(xpoints,ypoints)
plt.xticks(rotation=20)
plt.show()
plt.gcf().autofmt_xdate()










    
