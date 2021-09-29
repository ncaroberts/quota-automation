#!/usr/bin/env python3
#This is the backend wich is run via cron every hour

import sqlite3 as SQL
from quotalib import * 
import datetime
from syslog import syslog

#check dir exists: /glade/u/hsg/quota-automation/
if not os.path.isdir('/glade/u/hsg/quota-automation/'):
    print('/glade/u/hsg/quota-automation/ does not exist, Exiting!')
    exit(1)


#Only run with lock                                                                                                                                                  
mylock = open('/glade/u/hsg/quota-automation/quota.lock', 'w+')
max_lock_wait = 60             # 60 seconds
lock_waited = 0                 #seconds
lock_check_interval = 5         #seconds

while True:
    try:
        fcntl.flock(mylock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        break
    except IOError as e:
        if lock_waited >= max_lock_wait:
            print('Can not get lock... Waited %s seconds' % (max_lock_wait))
            exit(1)
        else:
            print("Waiting for lock up to %s seconds..." % (max_lock_wait))
            time.sleep(lock_check_interval)     #Try again every check_interval seconds
            lock_waited += lock_check_interval

timestamp = datetime.datetime.now().isoformat() #ISO8601

########### DEV DUMMY DATA #############

#variable = 'data'
username = 'robertsj'       #?*the user who's quota is to be increased
quotalimit = '10'	    #?*do we want a before/after?
ticketnumber = 'RC-12345'   #?*not an optional variable
enddate = '09-01-2022'	    #?*do we want a date or # of days here (we can format ++ days out)
addedby = 'robertsj'	    #csg/hsg user who added a quota increase/decrease/change. Enter as email addr? Can strip '@...' off.
active = True
expirenotice = False
########################################

#THIS SHOULD BE IN FIRST RUN IN USER INTERFACE AND PROCESSQUOTA.PY: if nolocal exists on login nodes, exit
check_nolocal()

#THIS CALL SHOULD BE FIRST RUN IN USER INTERFACE: build db if doesn't exist
builddb()

#FOR DB TESTING: call add_entry() 
add_entry(timestamp,username,quotalimit,enddate,ticketnumber,addedby)

# Call below to process quotas table entries
if process_entries() is False:
    print('Nothing to process, Exiting!')
    process_enddate()
    exit()

process_enddate()

