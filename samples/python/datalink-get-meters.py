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
str_max = 200

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

print("#### Query the list of facilities and meters, and print them ############")

# Login
print('> Logging in to '+base_url+' with '+username+'...')
r = requests.post(base_url+'/login', auth=(username,password))
print_req(r, False)
#grab the access token for later use
access_token = r.json()['access_token']
access_cookies = r.cookies

#get the list of facilities
print('> Querying facilities...')
r = requests.get(base_url+'/facilities', headers={'token': access_token}, cookies=access_cookies)
print_req(r, False)
#dump it to a file
with open("facilities.json", "w") as f1:
    f1.write(json.dumps(r.json(), indent=2))
    
#create a dictionary of facilites that we'll slot the meters into later
facilities = {'':{'uid':'', 'name': 'unassigned', 'meters': {}}}
jlist = r.json()
for i in range(0, len(jlist)):
    uid = jlist[i]['uid']
    name = jlist[i]['name']
    facilities[uid] = {'uid':uid, 'name':name, 'meters': {}}

#get the list of meters
print('> Querying meters...')
r = requests.get(base_url+'/meters', headers={'token': access_token}, cookies=access_cookies)
print_req(r)
#dump it to a file
with open("meters.json", "w") as f1:
    f1.write(json.dumps(r.json(), indent=2))

#link up each meter to a facility
jlist = r.json()
for i in range(0, len(jlist)):
    name = jlist[i]['name']
    type = jlist[i]['type']
    serial = jlist[i]['serial']
    facility_uid = ''
    if('facility_uid' in jlist[i]):
        facility_uid = jlist[i]['facility_uid']
    
    #add it
    facilities[facility_uid]['meters'][serial] = {'name':name, 'type': type, 'serial':serial}

#print out the results
print("Facilities:")
for f in facilities:
    print("  #%s [%s]" % (facilities[f]['name'].ljust(32,' '), facilities[f]['uid']))
    for mk in facilities[f]['meters']:
        m = facilities[f]['meters'][mk]
        print("    -%s [%s : %s]" % (m['name'].ljust(30,' '), m['serial'], m['type']))


# Logout
print('> Logging out...')
r = requests.post(base_url+'/logout', headers={'token': access_token}, cookies=access_cookies)
print_req(r)

