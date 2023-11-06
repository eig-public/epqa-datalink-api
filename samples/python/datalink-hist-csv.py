from os import access
import requests
import json
import time

from datetime import datetime
from datetime import timedelta
import datalink_utils as dlapi


print("#### Query historical records, and export to csv ##########################")
print("##   Illustrates parsing the historical records into a rectangular array ")
print("##   by meter, channel, and time;  as well as performing a lookup on the meters")
print("##   list for descriptive names")


# Remote configuration
base_url = "https://apiservice.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False

startTime = datetime.fromisoformat("2023-03-01T00:00:00")
endTime = datetime.fromisoformat("2023-04-01T00:00:00")

meter_serials = ['0123995130','0123320617','5000000000000014',
    '0224521824','0224521824','0154716529']
channel_keys = ['energy.interval.wh.q14','reading.voltage.an','reading.current.a',
    'reading.voltage.bn','reading.current.b',
    'reading.voltage.cn','reading.current.c',
    'frequency']

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


print('> Querying hist...')
time_start = time.perf_counter()
r = requests.get(comm_config.base_url+'/history',
    headers={
        'token': comm_config.access_token},
    params={
        'meterSerials': meter_serials,
        'channelKeys': channel_keys,
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
    with open("hist.json", "w") as f1:
        f1.write(json.dumps(r.json(), indent=2))

    data_arr = r.json()
else:
    print("> Failed to query historical records")

# Logout
dlapi.logout(comm_config)


#parse the results
if(not data_arr is None):
    print("> writing results to 'hist.csv'")
    
    #sort by meter, channel, column, and time.  we do this seperately to illustrate
    #   different approaches to slicing up the returned flat array
    meter_data = {}
    for i in range(0, len(data_arr)):
        rec = data_arr[i]
        m = rec['meter_serial']
        c = rec['channel_key']
        if(not m in meter_data):
            meter_data[m] = {}
        if(not c in meter_data[m]):
            meter_data[m][c] = []
        meter_data[m][c].append(rec)
    
    
    # illustrate using the meters list to pull up the descripive names for thigns
    col_names = {}
    meter_col = {}
    col = 1
    for m in meter_data:
        for c in meter_data[m]:
            k = m+":"+c
            meter_col[k] = col
            col = col + 1
            
            #look up the meter and channel.  refer to datalink_utils
            # MetersList and ChannelsList classes for json breakdown
            minfo = meters.list[m]
            if(c in minfo.channels.list):
                cinfo = minfo.channels.list[c]
                col_names[k] = minfo.name+" : "+cinfo.name
            else:
                col_names[k] = minfo.name+" : "+c
    
    # we build a slotted array of timestamps, so we can skip over blank entries
    #   between meters and channels
    times = {}
    for m in meter_data:
        for c in meter_data[m]:
            recs = meter_data[m][c]
            for i in range(0, len(recs)):
                t = datetime.fromisoformat(recs[i]['time'])
                if(not t in times):
                    times[t] = []
                times[t].append(recs[i])
    
    with open("hist.csv", "w") as f2:
        f2.write("time,")
        for k in meter_col:
            f2.write(col_names[k]+",")
        f2.write('\n')
            
        for t in sorted(times.keys()):
            # build the line of entries
            line = ['' for x in range(0, len(meter_col)+1)]
            line[0] = str(t)+","
            for rec in times[t]:
                k = rec['meter_serial']+":"+rec['channel_key']
                if(k in meter_col):
                    col = meter_col[k]
                    if(col >= len(line)):
                        print("Error with %s col %d" % (k,col))
                    line[col] = '%f,' % (rec['amount'])
            
            f2.writelines(line)
            f2.write('\n')
            








