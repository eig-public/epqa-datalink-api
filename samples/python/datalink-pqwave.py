from os import access
import requests
import matplotlib.pyplot as plt
import json
import time

from datetime import datetime
from datetime import timedelta
import datalink_utils as dlapi


print("#### Query the list of pq records, and use that to query specific #######")
print("##   waveform captures.  Note that a fuzzy search needs to be done for the")
print("##   waveform, so multiples could be returned.")

# Remote configuration
base_url = "https://apiservice.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"
verify_ssl = False

startTime = datetime.fromisoformat("2023-01-01T00:00:00")
endTime = datetime.fromisoformat("2023-04-01T00:00:00")

meter_serials = ['0123995130']

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


print('> Querying pq list...')
time_start = time.perf_counter()
r = requests.get(comm_config.base_url+'/pq',
    headers={
        'token': comm_config.access_token},
    params={
        'meterSerials': meter_serials,
        'start': dlapi.to_epoch_milli(startTime),
        'end': dlapi.to_epoch_milli(endTime),
    }, cookies=comm_config.access_cookies)
time_end = time.perf_counter()
dlapi.print_req(r, False)

data_arr_pq = None
if(r.status_code==200):
    print('> %d pq records returned in %fms' % (len(r.json()), (time_end-time_start)*1000))
    #success retriving
    #dump it to a file
    with open("pq.json", "w") as f1:
        f1.write(json.dumps(r.json(), indent=2))

    data_arr_pq = r.json()
else:
    print("> Failed to query pq records")
    dlapi.logout(comm_config)
    exit()


wtime_start = datetime.fromisoformat(data_arr_pq[0]['time']) - timedelta(seconds=1)
wtime_end = datetime.fromisoformat(data_arr_pq[0]['time']) + timedelta(seconds=1)

print('> Querying waveforms between [%s - %s]' % (wtime_start, wtime_end))

time_start = time.perf_counter()
r = requests.get(comm_config.base_url+'/wave',
    headers={
        'token': comm_config.access_token},
    params={
        'meterSerials': meter_serials,
        'start': dlapi.to_epoch_milli(wtime_start),
        'end': dlapi.to_epoch_milli(wtime_end),
    }, cookies=comm_config.access_cookies)
time_end = time.perf_counter()
dlapi.print_req(r, False)

data_arr_wave = None
if(r.status_code==200):
    print('> %d records returned in %fms' % (len(r.json()), (time_end-time_start)*1000))
    #success retriving
    #dump it to a file
    with open("wave.json", "w") as f1:
        f1.write(json.dumps(r.json(), indent=2))

    data_arr_wave = r.json()
else:
    print("> Failed to query waveform records")
    dlapi.logout(comm_config)
    exit()
    
if(len(data_arr_wave) == 0):
    print("> No waveform events found")
    dlapi.logout(comm_config)
    exit()

# Logout
dlapi.logout(comm_config)


# process the waveform
waveobj = data_arr_wave[0]

# parse out the sub arrays for the samples
ypoints = { 
    'va':json.loads(waveobj['s_van_vab']),
    'vb':json.loads(waveobj['s_vbn_vbc']),
    'vc':json.loads(waveobj['s_vcn_vca'])
}
time_per_sample = waveobj['t_sample_int']
xpoints = [i*time_per_sample for i in range(0, len(ypoints['va']))]

plt.plot(xpoints,ypoints['va'], label='va')
plt.plot(xpoints,ypoints['vb'], '-.', label='vb')
plt.plot(xpoints,ypoints['vc'], '-.', label='vc')


wstart = dlapi.from_epoch_milli(waveobj['start_time'])
wend = dlapi.from_epoch_milli(waveobj['end_time'])
wtrig = dlapi.from_epoch_milli(waveobj['t_trigger_time'])
trig_x = int(((wtrig-wstart).total_seconds()*1000))

plt.xlabel("Sample Time (ms)")
plt.ylabel("Sample Magnitude (v)")
plt.legend()
plt.suptitle("Waveform @ %s [%fms]" % (str(wstart), 
    (wend-wstart).total_seconds() * 1000), fontsize=14)
plt.title("Trigger %s @ %s" % (waveobj['t_wave_trigger'], 
    str(wtrig)), fontsize=9)
#add the trigger line
plt.axvline(x=trig_x, color='b', linestyle='--')
plt.show()


