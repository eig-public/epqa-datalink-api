from os import access
import requests
import json
import time

from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import datalink_utils as dlapi




print("#### Get list of all physical meters, organize by facility, get ##########")
print("##   list of all channels, and query 2 years of data for each, by month.")
print("##   Print to csv, and print out the statistics at the end.")


# Remote configuration
base_url = "https://apiservice.energypqa.com/v1"
username = "75d48e47f6e946ec9def97becddf0218_datalink"
password = "1234"

verify_ssl = False
MAX_METERS = 6

time_end = datetime.now()
time_end = datetime(time_end.year, time_end.month, 1)
time_start = datetime(time_end.year-2, time_end.month, 1)

meter_serials = [[]]
channel_keys = []
facility_tree = {'':{'fac':dlapi.Facility,'meters':{}}}


stats_file = "datalink-scrape_%s.query-stats" % (datetime.now().strftime('%Y%m%d%H%M%S'))
with open(stats_file, 'w') as f_s:
    f_s.write("query,meters,channels,time start,time end,ec,bytes,count,query time (ms)\n")

def print_stats(j,jmax,ji, meters, channels, tstart, tend, ec, bytes, count, query_time):
    with open(stats_file, 'a') as f_s:
        f_s.write('[%d/%d:%d],{%s},[%d],%s,%s,%d,%d,%d,%f\n' %
            (j,jmax,ji,
             ':'.join(meters), len(channels),
             str(tstart), str(tend),
             ec, bytes, count, query_time))


# Utils
dlapi.init(clfe=True, clce=False, sm=200)
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

# collect the list of all meters, and sort by facility.  also shows breaking the 
#   list into multiple queries to fit in the max meter query limit
active_list = meter_serials[0]
for k in meters.list:
    m = meters.list[k]
    fac = facilities.GetFacility_for_Meter(m)
    if(not m.IsVirtual()):
        #only include the non-virtual meters, and build batches to query in
        active_list.append(m.serial)
        if(len(active_list) >= MAX_METERS):
            active_list = []
            meter_serials.append(active_list)
        #build the facility list
        if(fac is None):
            facility_tree['']['meters'][m.serial] = m
            m.facility_uid = ''
        else:
            if(not fac.uid in facility_tree):
                facility_tree[fac.uid] = {'fac':fac, 'meters':{}}
            facility_tree[fac.uid]['meters'][m.serial] = m
        m.rec_count = 0
        m.data_start = datetime(2100,1,1)
        m.data_end = datetime(2000,1,1)
        m.j = len(meter_serials)-1
        
        #build the list of channels
        for c in m.channels.list:
            if(not c in channel_keys):
                channel_keys.append(c)


def query_and_print(comm_config, j,jmax,ji, meters, channels, tstart, tend, meter_list):
    print('[%d/%d:%d]> Querying data [%dmx%dc] for range [%s - %s]...' % 
        (j,jmax,ji, len(meters), len(channels), str(tstart), str(tend)))
    time_start = time.perf_counter()
    r = requests.get(comm_config.base_url+'/history',
        headers={
            'token': comm_config.access_token},
        params={
            'meterSerials': meters,
            'channelKeys': channels,
            'start': dlapi.to_epoch_milli(tstart),
            'end': dlapi.to_epoch_milli(tend),
        }, cookies=comm_config.access_cookies)
    time_end = time.perf_counter()
    dlapi.print_req(r, False)
    
    duration = (time_end-time_start)*1000
    count = 0
    bytes = len(r.text)
    if(r.status_code==200):
        count = len(r.json())
    print_stats(j,jmax,ji, meters, channels, tstart, tend, r.status_code, bytes, count, duration)

    data_arr = []
    if(r.status_code==200):
        print(' [%d/%d:%d]> %d records returned in %fms' % (j,jmax,ji, len(r.json()), duration))
        #success retriving
        #dump it to a file
        with open("hist_%d_%s_%s.json" % (j, 
            tstart.strftime('%Y%m%d'), tend.strftime('%Y%m%d')),
            "w") as f1:
            f1.write(json.dumps(r.json(), indent=2))

        data_arr = r.json()
    else:
        print(" [%d/%d:%d]> Failed to query historical records in %fms" % (j,jmax,ji, duration))
        return duration, r.status_code, r.text
    
    export_results(j,jmax,ji, meters, channels, tstart, tend, meter_list, data_arr)
    return duration, r.status_code, r.text

def export_results(j,jmax,ji, meters, channels, tstart, tend, meter_list, data_arr):

    filename = "hist_%d_%s_%s.csv" % (j, tstart.strftime('%Y%m%d'), tend.strftime('%Y%m%d'))
    
    #parse the results
    print('[%d/%d:%d]> Writing results [%dmx%dc] for range [%s - %s] to \'%s\'...' % 
        (j,jmax,ji, len(meters), len(channels), str(tstart), str(tend), filename))
    
    #sort by meter, channel, column, and time.  we do this seperately to illustrate
    #   different approaches to slicing up the returned flat array
    meter_data = {}
    for i in range(0, len(data_arr)):
        rec = data_arr[i]
        # print("----- %s" % (str(rec)))
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
            minfo = meter_list.list[m]
            if(c in minfo.channels.list):
                cinfo = minfo.channels.list[c]
                col_names[k] = minfo.name+" : "+cinfo.name
            else:
                col_names[k] = minfo.name+" : "+c
    
    # we build a slotted array of timestamps, so we can skip over blank entries
    #   between meters and channels
    times = {}
    for m in meter_data:
        minfo = meter_list.list[m]
        for c in meter_data[m]:
            recs = meter_data[m][c]
            for i in range(0, len(recs)):
                t = datetime.fromisoformat(recs[i]['time'])
                if(not t in times):
                    times[t] = []
                times[t].append(recs[i])
                minfo.rec_count = minfo.rec_count+1
                if(t<minfo.data_start):
                    minfo.data_start = t
                if(t>minfo.data_end):
                    minfo.data_end = t
    
    with open(filename,
        "w") as f2:
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
    
        
query_time_sum = 0
query_time_count = 0
query_time_max = 0
query_time_min = 10000000000
err_count = 0
err_list = []
err_seq = 0
good_seq = 0

# meter_serials = [meter_serials[0]]
jmax = len(meter_serials)

print('> Performing %d queries' % (jmax))
for j in range(0, jmax):
    tstart = time_start
    ji = 0
    while(tstart<time_end):
        tend = tstart+relativedelta(months=1)
        query_time, ec, body = query_and_print(comm_config, j,jmax,ji, meter_serials[j], channel_keys, tstart, tend, meters)
        query_time_sum = query_time_sum + query_time
        query_time_count = query_time_count+1
        
        if(query_time > query_time_max):
            query_time_max = query_time
        if(query_time < query_time_min):
            query_time_min = query_time
        
        if(ec!=200):
            good_seq = 0
            err_seq = err_seq+1
            err_count = err_count+1
            err_list.append("[%d/%d:%d %s - %s]: %s" % (j,jmax,ji, str(tstart), str(tend), body))
        else:
            good_seq = good_seq+1
            if(good_seq>2):
                err_seq = 0
        
        tstart = tend
        ji = ji+1
        
        if(err_seq>=8):
            msg = "!!!! [%d/%d:%d]: 8 errors in a row occurred, cancelling scrape" % (j,jmax,ji)
            print(msg)
            err_list.append(msg)
            tstart = tend
            j=jmax
            break
            


# Logout
dlapi.logout(comm_config)


#print the statistics
print("Query statistics:")
print("  Groups:            %d" % jmax)
print("  Count:             %d queries" % query_time_count)
print("  Total Time:        %dms" % query_time_sum)
print("  Avg Time:          %dms" % ((query_time_sum*1.0)/query_time_count))
print("  Min Time:          %dms" % query_time_min)
print("  Max Time:          %dms" % query_time_max)

print("Errors [%d]:" % (err_count))
for e in err_list:
    print("  -%s" % (e))

print("Facilities [%d]:" % (len(facility_tree)))
for f in facility_tree:
    print("  #%s [%s] {%d meters}" % (facility_tree[f]['fac'].name.ljust(32,' '),
        facility_tree[f]['fac'].uid, len(facility_tree[f]['meters'])))
    for mk in facility_tree[f]['meters']:
        m = facility_tree[f]['meters'][mk]
        print("    -[%d] %s [%s : %s]" % (m.j, m.name.ljust(30,' '), m.serial, m.type))
        print("      %d records [%s - %s]" % (m.rec_count, str(m.data_start), str(m.data_end)))

with open('query_mapping.txt', 'w') as f3:
    f3.write("Query statistics:\n")
    f3.write("  Groups:            %d\n" % jmax)
    f3.write("  Count:             %d queries\n" % query_time_count)
    f3.write("  Total Time:        %dms\n" % query_time_sum)
    f3.write("  Avg Time:          %dms\n" % ((query_time_sum*1.0)/query_time_count))
    f3.write("  Min Time:          %dms\n" % query_time_min)
    f3.write("  Max Time:          %dms\n" % query_time_max)

    f3.write("Errors [%d]:\n" % (err_count))
    for e in err_list:
        f3.write("  -%s\n" % (e))

    f3.write("Facilities [%d]:\n" % (len(facility_tree)))
    for f in facility_tree:
        f3.write("  #%s [%s] {%d meters}\n" % (facility_tree[f]['fac'].name.ljust(32,' '),
            facility_tree[f]['fac'].uid, len(facility_tree[f]['meters'])))
        for mk in facility_tree[f]['meters']:
            m = facility_tree[f]['meters'][mk]
            f3.write("    -[%d] %s [%s : %s]\n" % (m.j, m.name.ljust(30,' '), m.serial, m.type))
            f3.write("      %d records [%s - %s]\n" % (m.rec_count, str(m.data_start), str(m.data_end)))
    








