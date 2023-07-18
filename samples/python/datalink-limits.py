from os import access
import requests
import json
import time

from datetime import datetime
from datetime import timedelta
import datalink_utils as dlapi


print("#### Query limits records ###############################################")

# Remote configuration
base_url = "https://apiservice-prod.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False

startTime = datetime.fromisoformat("2022-08-24T00:00:00")
endTime = datetime.fromisoformat("2022-12-09T00:00:00")

meter_serials = ['0168006526']

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


print('> Querying limits...')
time_start = time.perf_counter()
r = requests.get(comm_config.base_url+'/limits',
    headers={
        'token': comm_config.access_token},
    params={
        'meterSerials': meter_serials,
        'start': dlapi.to_epoch_milli(startTime),
        'end': dlapi.to_epoch_milli(endTime),
    }, cookies=comm_config.access_cookies)
time_end = time.perf_counter()
dlapi.print_req(r, False)

data_arr = None
if(r.status_code==200):
    print('> %d records returned in %fms' % (len(r.json()), (time_end-time_start)*1000))
    #success retriving
    #dump it to a file
    with open("limits.json", "w") as f1:
        f1.write(json.dumps(r.json(), indent=2))

    data_arr = r.json()
else:
    print("> Failed to query limits records")

# Logout
dlapi.logout(comm_config)






