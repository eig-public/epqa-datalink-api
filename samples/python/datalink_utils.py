
from datetime import datetime,timezone
from datetime import timedelta

import requests
import json

global commlog_file
global commlog_filename
global commlog_file_enabled
global commlog_console_enabled
global str_max

commlog_file = None
commlog_filename = None
commlog_file_enabled = True
commlog_console_enabled = True
str_max = 200

def init(clfe=True, clce=True, sm=200):
    global commlog_file
    global commlog_filename
    global commlog_file_enabled
    global commlog_console_enabled
    global str_max
    commlog_file = None
    commlog_filename = None
    commlog_file_enabled = clfe
    commlog_console_enabled = clce
    str_max = sm

class CommConfig:
    base_url = "https://apiservice-dev.energypqa.com/v1"
    username = None
    password = None
    
    access_token = None
    access_cookies = None
        
    

class Facility:
    name = "Unassigned"
    uid = ""
    def __init__(self, jrec=None):
        if(jrec is None):
            self.uid = ""
            self.name = "Unassigned"
        else:
            self.uid = jrec['uid']
            self.name = jrec['name']

class FacilityList:
    list = {}
    
    def GetFacility_for_Meter(self, meter):
        if(meter.facility_uid is None):
            return None
        if(meter.facility_uid in self.list):
            return self.list[meter.facility_uid]
        else:
            return None
    
    def __init__(self, jrec):
        self.list = {}
        for i in range(0, len(jrec)):
            fac = Facility(jrec[i])
            self.list[fac.uid] = fac

class Channel:
    def __init__(self, jrec):
        self.key = jrec['key']
        self.name = jrec['name']

class ChannelList:
    list = {}
    
    def __init__(self, jrec):
        self.list = {}
        for i in range(0, len(jrec)):
            chan = Channel(jrec[i])
            self.list[chan.key] = chan

class Meter:
    
    def IsVirtual(self):
        if(self.type=="Water"):
            return True
        if(self.type=="Air"):
            return True
        if(self.type=="Gas"):
            return True
        if(self.type=="Steam"):
            return True
        if(self.type=="totalizer"):
            return True
        return False
            
    def __init__(self, jrec):
        self.uid = jrec['uid']
        self.name = jrec['name']
        self.type = jrec['type']
        self.serial = jrec['serial']
        if('facility_uid' in jrec):
            self.facility_uid = jrec['facility_uid']
        else:
            self.facility_uid = None
        if('last_update' in jrec):
            try:
                self.last_update = datetime.fromisoformat(jrec['last_update'])
            except:
                print(" #> Invalid ISO8601 for [%s:%s]: %s" % (self.name, self.uid, jrec['last_update']))
                self.last_update = datetime(1970,1,1)
        else:
            self.last_update = datetime(1970,1,1)
        self.channels = ChannelList(jrec['channels'])
        self.settings = jrec['settings']            

class MetersList:
    list = {}
    
    def __init__(self, jrec):
        self.list = {}
        for i in range(0, len(jrec)):
            m = Meter(jrec[i])
            self.list[m.serial] = m

def to_epoch_milli(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def from_epoch_milli(dt_ms):
    return datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(milliseconds=dt_ms)


def open_commlog_file():
    global commlog_file
    global commlog_filename
    if(commlog_file is None):
        if(commlog_filename is None):
            commlog_filename = "commlog_%s.log" % (datetime.now().strftime('%Y%m%d%H%M%S'))
        commlog_file = open(commlog_filename, 'w')
    return commlog_file
    
def print_req(r,compact=True):
    
    global commlog_console_enabled
    global commlog_file_enabled
    
    if(compact):
        msg = "  <[%d:%d] %s" % (r.status_code, len(r.text), str(r.text)[:str_max])
    else:
        msg =         "  <[%d:%d" % (r.status_code, len(r.text))
        msg = msg + "\n    h: %s" % (str(r.headers))
        msg = msg + "\n    b: %s" % (str(r.text)[:str_max*2])
    
    if(commlog_console_enabled):
        print("  > [%s] %s" % (r.request.method, r.request.url))
        print(msg)
    if(commlog_file_enabled):
        f = open_commlog_file()
        f.write("{%s} > [%s] %s\n" % (datetime.now().strftime('%H:%M:%S.%f')[:-3], r.request.method, r.request.url))
        f.write(msg+'\n')


def query_facilities(comm_config, dump_json=True):
    print('> Querying facilities...')
    r = requests.get(comm_config.base_url+'/facilities', headers={'token': comm_config.access_token}, cookies=comm_config.access_cookies)
    print_req(r)
    if(dump_json):
        #dump it to a file
        with open("facilities.json", "w") as f1:
            f1.write(json.dumps(r.json(), indent=2))
    ret = FacilityList(r.json())
    return ret

def query_meters(comm_config, jdata=None, dump_json=True):
        
    print('> Querying meters...')
    r = requests.get(comm_config.base_url+'/meters', headers={'token': comm_config.access_token}, cookies=comm_config.access_cookies)
    print_req(r)
    if(dump_json):
        #dump it to a file
        with open("meters.json", "w") as f1:
            f1.write(json.dumps(r.json(), indent=2))
    
    jdata = r.json()
    ret = MetersList(jdata)
    return ret
    
def login(comm_config, username, password):
    print('> Logging in to '+comm_config.base_url+' with '+username+'...')
    r = requests.post(comm_config.base_url+'/login', auth=(username,password))
    print_req(r, False)
    comm_config.access_token = r.json()['access_token']
    comm_config.access_cookies = r.cookies
    return comm_config.access_token

def logout(comm_config):
    print('> Logging out...')
    r = requests.post(comm_config.base_url+'/logout', headers={'token': comm_config.access_token}, cookies=comm_config.access_cookies)
    print_req(r)
    


















