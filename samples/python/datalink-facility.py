from os import access
import requests
import json
import time

from datetime import datetime
from datetime import timedelta
import datalink_utils as dlapi


print("#### Query historical records for all meters in a facility ###############")
print("##   Illustrates using the facilities and meters lists together to pull data")


# Remote configuration
base_url = "https://apiservice.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False

startTime = datetime.fromisoformat("2023-06-01T00:00:00")
endTime = datetime.fromisoformat("2023-06-04T00:00:00")

facility = 'Electro Industries 1800'
meter_serials = [[]]
channel_keys = ['energy.interval.wh.q14']

# Utils
comm_config = dlapi.CommConfig()
comm_config.base_url = base_url
comm_config.username = username
comm_config.password = password

# Login
dlapi.login(comm_config, username, password)

#get the facilities
facilities = dlapi.query_facilities(comm_config)

#get the meters
meters = dlapi.query_meters(comm_config)

#find the facility
fac = None
for f in facilities.list:
    if(facilities.list[f].name == facility):
        fac = facilities.list[f]
if(fac is None):
    print("> Facility %s not found" % facility)
    exit

# collect the meters at a facility.  also shows breaking the list into multiple
#   queries to fit in the max meter query limit
active_list = meter_serials[0]
for k in meters.list:
    m = meters.list[k]
    if(m.facility_uid == fac.uid and not m.IsVirtual()):
        active_list.append(m.serial)
        if(len(active_list) >= 6):
            active_list = []
            meter_serials.append(active_list)

data_arr = []
print('> Performing %d queries' % (len(meter_serials)))
for j in range(0, len(meter_serials)):
    print('> Querying hist [%d]...' % (j))
    time_start = time.perf_counter()
    r = requests.get(comm_config.base_url+'/history',
        headers={
            'token': comm_config.access_token},
        params={
            'meterSerials': meter_serials[j],
            'channelKeys': channel_keys,
            'start': dlapi.to_epoch_milli(startTime),
            'end': dlapi.to_epoch_milli(endTime),
        }, cookies=comm_config.access_cookies)
    time_end = time.perf_counter()
    dlapi.print_req(r, False)

    if(r.status_code==200):
        print('> %d records returned in %fms' % (len(r.json()), (time_end-time_start)*1000))
        #success retriving
        #dump it to a file
        with open("hist_%d.json" % (j), "w") as f1:
            f1.write(json.dumps(r.json(), indent=2))

        data_arr.append(r.json())
    else:
        print("> Failed to query historical records")


# Logout
dlapi.logout(comm_config)


print('> %d Data arrays available' % (len(data_arr)))

#parse the results
for j in range(0,len(data_arr)):
    sub_arr = data_arr[j]
    print("> Results #[%d]" % (j))
    
    #sort by meter, channel, so we can count individual records
    meter_data = {}
    for i in range(0, len(sub_arr)):
        rec = sub_arr[i]
        m = rec['meter_serial']
        c = rec['channel_key']
        if(not m in meter_data):
            meter_data[m] = {}
        if(not c in meter_data[m]):
            meter_data[m][c] = []
        meter_data[m][c].append(rec)
    
    for m in meter_data:
        for c in meter_data[m]:
            print("  %s [%s] : %s - %d records" % 
                (meters.list[m].name, m, c, len(meter_data[m][c])))
            








