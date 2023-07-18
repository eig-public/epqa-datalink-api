from os import access
import requests
import json

from datetime import datetime
from datetime import timedelta

# Remote configuration
base_url = "https://apiservice-prod.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False
str_max = 200

startTime = datetime.fromisoformat("2023-01-01T00:00:00")
endTime = datetime.fromisoformat("2023-04-01T00:00:00")

meter_serials = ['0123995130','0123320617']

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

print("#### Query system events list from one meter ####")

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

print('> Querying system events...')
r = requests.get(base_url+'/events',
    headers={
        'token': access_token},
    params={
        'meterSerials': meter_serials,
        'start': to_epoch_milli(startTime),
        'end': to_epoch_milli(endTime),
    }, cookies=access_cookies)
print_req(r,False)


data_arr = None
if(r.status_code==200):
    #success retriving
    #dump it to a file
    with open("events.json", "w") as f1:
        f1.write(json.dumps(r.json(), indent=2))

    data_arr = r.json()
else:
    print("> Failed to query events")

# Logout
print('> Logging out...')
r = requests.get(base_url+'/logout', headers={'token': access_token}, cookies=access_cookies)
print_req(r)


#parse the results
if(not data_arr is None):
    print("> writing results to 'events.csv'")
    
    #note that we're sorting the meters here, and splitting the records up
    # by meter.  This is done to illustrate how to collect by meter
    events = {}
    for i in range(0, len(data_arr)-1):
        rec = data_arr[i]
        m = rec['meter_serial']
        if(not m in events):
            events[m] = []
        events[m].append(rec)
        
    with open("events.csv", "w") as f2:
        f2.write("time,meter,event,description\n")
        for m in events:
            for rec in events[m]:
                time = datetime.fromisoformat(rec['time'])
                meter = rec['meter_serial']
                evtype = ("[%d:%d]" % (rec['event_type'], rec['event_subtype']))
                desc = rec['description']
                f2.write("%s,%s,%s,%s\n" %
                    (str(time), meter, evtype, desc))









