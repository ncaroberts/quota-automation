#!/usr/bin/env python3

import sqlite3 as SQL
import os
import fcntl
import time
import logging
import datetime
import pwd  #to validate user
import re
from subprocess import Popen, PIPE

maxquota = 1024	    #1PB max quota (in TB)
timestamp = datetime.datetime.now().isoformat() #ISO8601
process_err = 0
email_err = []


def process_enddate():
    today = time.strftime("%m-%d-%Y")
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''SELECT * from history WHERE current = 1 AND expirenotice = 0''')
        table = cur.fetchall()
        for row in table:
            dbusername,dbenddate,dbticketnumber = row[2],row[4],row[5]
            d1 = datetime.datetime.strptime(dbenddate, '%m-%d-%Y').date()
            d2 = datetime.datetime.strptime(today, '%m-%d-%Y').date()
            if d1 <=  d2:
                update_history(dbusername,updatevalue=True,updatewhat='expirenotice')
                email_body = 'csg,\n\nThe scratch storage quota for user \'%s\' has reached the defined end date. Please update user quota.' % (dbusername) 
                send_email(dbusername,dbticketnumber,email_body,process_failure=False)


def update_history(dbusername,updatevalue,updatewhat):    
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''UPDATE history SET {0} = ? WHERE username = ?'''.format(updatewhat), (updatevalue,dbusername))
    log_log(': Auto Process: Updated history table for dbusername \'%s\' %s: %s' %(dbusername,updatewhat,updatevalue))

def make_quota_update(dbusername,dbquotalimit):
    send_cmd = "df"	#for test/dev
    #send_cmd = "sudo /nfs/home/dasg/apps/bin/glade_setquota set scratch {0} {1}t".format(dbusername,dbquotalimit)
    p = Popen(send_cmd, stdout=PIPE, shell=True)
    output,err = p.communicate()
    updated_quota = output.decode('utf-8')
    if p.returncode != 0:
        return ['', 1]    
    else:        
        log_log(': Auto Process: make_quota_update(): Succeeded for dbusername \'%s\'' % (dbusername))
        return [updated_quota, 0]


def get_quota(dbusername):
    #send_cmd = "df"	#for test/dev
    send_cmd = "sudo /nfs/home/dasg/apps/bin/glade_setquota get scratch {0}".format(dbusername)
    p = Popen(send_cmd, stdout=PIPE, shell=True)
    output,err = p.communicate()
    user_quota = output.decode('utf-8')
    if p.returncode != 0:
        return ['', 1]
    else:
        log_log(': Auto Process: get_quota(): Succeeded for dbusername \'%s\'' % (dbusername))
        return [user_quota, 0]


def send_email(dbusername,dbticketnumber,email_body,process_failure):
    # Import smtplib for the actual sending function
    import smtplib
    # Import the email modules needed
    from email.message import EmailMessage

    email_footer = '\n\nhttps://helpdesk.ucar.edu/browse/%s' % (dbticketnumber)
    
    if process_failure == True:
        msg = EmailMessage()
        msg['From'] = 'hsg@ucar.edu'
        msg['To'] = 'robertsj@ucar.edu,robertsj@ucar.edu'
        msg['Subject'] = 'ERROR: Quota Update: %s' %(dbticketnumber)
        email_header = 'csg,\n\nAn error has occured while processing %s. Please verify input data and resubmit.\n\n' % (dbticketnumber)
        email_body = email_header + email_body + email_footer
        msg.set_content(email_body)
    else:
        msg = EmailMessage()
        msg['From'] = 'hsg@ucar.edu'
        msg['To'] = 'robertsj@ucar.edu'
        msg['Subject'] = 'Quota Update: %s' %(dbticketnumber)
        email_body = email_body + email_footer
        msg.set_content(email_body)

    # Send email
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()
    log_log(': Auto Process: send_email(): Email sent for dbusername \'%s\'' % (dbusername))


def validate_process_data(dbid,dbticketnumber,dbusername,dbquotalimit,dbenddate,dbaddedby):
    errcount = 0
    try:
        pwd.getpwnam(dbusername)
        log_log(': Auto Process: Validated dbusername \'%s\'' % (dbusername))
    except KeyError:
        log_log(': Auto Process: ERROR: Could not validate dbusername \'%s\'. User not found.' % (dbusername))
        email_err.append('Auto Process: ERROR: Could not validate dbusername \'%s\'. User not found.' % (dbusername))
        errcount += 1

    try:
        pwd.getpwnam(dbaddedby)
        log_log(': Auto Process: Validated dbaddedby \'%s\'' % (dbaddedby))
    except KeyError:
        log_log(': Auto Process: ERROR: Could not validate dbaddedby \'%s\'. User not found.' % (dbaddedby))
        email_err.append('Auto Process: ERROR: Could not validate dbaddedby \'%s\'. User not found.' % (dbaddedby))
        errcount += 1

    try:
        datetime.datetime.strptime(dbenddate, '%m-%d-%Y')
        log_log(': Auto Process: Validated dbenddate \'%s\'' % (dbenddate))
    except ValueError:
        log_log(': Auto Process: ERROR: Could not validate dbenddate \'%s\'. Check date format.' % (dbenddate))
        email_err.append('Auto Process: ERROR: Could not validate dbenddate \'%s\'. Check date format.' % (dbenddate))
        errcount += 1

    if not isinstance(dbquotalimit, int):
        log_log(': Auto Process: ERROR: Could not validate dbquotalimit. Looks to be a %s, but must be an integer' % (type(dbquotalimit)))
        email_err.append('Auto Process: ERROR: Could not validate dbquotalimit. Looks to be a %s, but must be an integer' % (type(dbquotalimit)))
        errcount += 1

    if isinstance(dbquotalimit, int) and dbquotalimit > maxquota:
        log_log(': Auto Process: ERROR: Could not validate dbquotalimit. %s > %s (maxquotalimit)' % (dbquotalimit,maxquota))
        email_err.append('Auto Process: ERROR: Could not validate dbquotalimit. %s > %s (maxquotalimit)' % (dbquotalimit,maxquota))
        errcount += 1
    else:
        log_log(': Auto Process: Validated dbquotalimit \'%s\'' % (dbquotalimit))

    if dbticketnumber is '':
        log_log(': Auto Process: ERROR: Could not validated dbticketnumber \'%s\'' % (dbticketnumber))
        email_err.append(': Auto Process: ERROR: Could not validated dbticketnumber \'%s\'' % (dbticketnumber))
        errcount += 1
    else:
        log_log(': Auto Process: Validated dbticketnumber \'%s\'' % (dbticketnumber))

    return errcount

    #dbquotalimit = ''.join(dbquotalimit)
    #dbquotalimit_list = re.findall('(\d+|[A-Za-z]+)', dbquotalimit) #separate numbers and letters
    #quotalimit,multiple = dbquotalimit_list
    #print(quotalimit, multiple)


#delete entry from quotas table after processing
def del_entry(dbid):
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''DELETE FROM quotas WHERE id = ?''', (dbid,))
        log_log(': Auto Process: Deleted dbid %s from quotas table' % (dbid))
        #print('Deleted dbid: %s' % (dbid))  #dev jon


#get entries in quota table, process
def process_entries():
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''SELECT * FROM quotas''')
        table = cur.fetchall()
        if len(table) == 0:
            log_log(': Auto Process: Nothing to process from quotas table')	#Do we want to log this?
            return False
        else:
            for row in table:
                email_err.clear()
                #log_log(': =======================')
                dbid, dbtimestamp,dbusername,dbquotalimit,dbenddate,dbticketnumber,dbaddedby = row
                log_entry = ": Auto Process: Processing: dbid %s : %s %s %s %s %s %s" % (dbid,dbtimestamp,dbusername,dbquotalimit,dbticketnumber,dbenddate,dbaddedby)
                log_log(log_entry)
                errcount = validate_process_data(dbid,dbticketnumber,dbusername,dbquotalimit,dbenddate,dbaddedby)
                del_entry(dbid)
                if errcount == 0:

                    user_quota_pre = get_quota(dbusername)	#get two vars in list, break apart, use
                    user_quota_pre,process_err = user_quota_pre
                    if process_err != 0:                  
                        log_log(': Auto Process: ERROR: Could not process get_quota() for user \'%s\' before update, Exiting!' %(dbusername))
                        email_err.append('Auto Process: ERROR: Could not process get_quota() for user \'%s\' before update, Exiting!' %(dbusername))
                        email_body = "\n".join(email_err)
                        send_email(dbusername,dbticketnumber,email_body,process_failure=True) 
                        break

                    updated_quota = make_quota_update(dbusername,dbquotalimit)
                    updated_quota,process_err = updated_quota	#capture output=updated_quota if needed later
                    if process_err != 0:
                        log_log(': Auto Process: ERROR: Could not process make_quota_update() for user \'%s\'' % (dbusername))
                        email_err.append('Auto Process: ERROR: Could not process make_quota_update() for user \'%s\'' % (dbusername))
                        email_body = "\n".join(email_err)
                        send_email(dbusername,dbticketnumber,email_body,process_failure=True) 
                        break

                    user_quota_post = get_quota(dbusername)
                    user_quota_post,process_err = user_quota_post
                    if process_err != 0:
                       log_log(': Auto Process: ERROR: Could not process get_quota() for user \'%s\' after update!' % (dbusername))
                       email_err.append('Auto Process: ERROR: Could not process get_quota() for user \'%s\' after update!' % (dbusername))
                       email_body = "\n".join(email_err)
                       send_email(dbusername,dbticketnumber,email_body,process_failure=True) 
                       break

                    email_header = 'csg,\n\nThe scratch storage quota update for user \'%s\' has been processed.\n\n' % (dbusername)
                    email_body = "%sBEFORE UPDATE:\n%s\n\nAFTER UPDATE:\n%s" % (email_header,user_quota_pre,user_quota_post)
                    update_history(dbusername,updatevalue=False,updatewhat='current')
                    log_history(timestamp,dbusername,dbquotalimit,dbenddate,dbticketnumber,dbaddedby,current=True,expirenotice=False)
                    send_email(dbusername,dbticketnumber,email_body,process_failure=False)
                    log_entry = ": Auto Process: Processed: dbid %s : %s %s %s %s %s %s" % (dbid,dbtimestamp,dbusername,dbquotalimit,dbticketnumber,dbenddate,dbaddedby)
                    log_log(log_entry)

                else:
                    log_entry = ": Auto Process: ERROR: DB data could not be validated, Exiting!"
                    email_err.append("Auto Process: ERROR: DB data could not be validated, Exiting!")
                    log_log(log_entry)
                    email_body = "\n".join(email_err) 
                    send_email(dbusername,dbticketnumber,email_body,process_failure=True)


#FOR DEV: add new entry to be processed. This is a function for front-end 
def add_entry(timestamp,username,quotalimit,enddate,ticketnumber,addedby):
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''INSERT INTO quotas(
                       timestamp,username,quotalimit,enddate,ticketnumber,addedby)
                       VALUES(?, ?, ?, ?, ?, ?)''',
                       (timestamp,username,quotalimit,enddate,ticketnumber,addedby))


#log everything to logfile
def log_log(log_entry):
    date = time.strftime("%Y-%m-%d") 
    logfile = '/glade/u/hsg/quota-automation/processquotas_logs/quota.%s.log' % (date)
    logging.basicConfig(filename=logfile, format='%(asctime)s %(message)s', level=logging.INFO)
    logging.info(log_entry)


#log successful updates to history table
def log_history(date,username,quotalimit,enddate,ticketnumber,addedby,current,expirenotice): 
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''INSERT INTO history(
                       timestamp,username,quotalimit,enddate,ticketnumber,addedby,current,expirenotice)
                       VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
                       (timestamp,username,quotalimit,enddate,ticketnumber,addedby,current,expirenotice))
    log_log(': Auto Process: Updated history table with new processed entry for dbusername \'%s\'' % (username))
    return


#if nolocal exists on login nodes, exit
def check_nolocal():                                                                                                                                                      
    if os.path.isfile('/etc/nolocal'):                                                                                                                                                
        print("/etc/nolocal exists, Exiting!")
        exit(1)


#build db if doesn't exist
def builddb():
    con = SQL.connect('/glade/u/hsg/quota-automation/quota.sqlite')
    with con:
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS quotas(
                       id INTEGER PRIMARY KEY,
                       timestamp TEXT NOT NULL,
                       username TEXT NOT NULL,
                       quotalimit INTEGER NOT NULL,
		       enddate TEXT NOT NULL,
                       ticketnumber TEXT NOT NULL,
                       addedby TEXT NOT NULL)''')

        cur.execute('''CREATE TABLE IF NOT EXISTS history(
                       id INTEGER PRIMARY KEY,
                       timestamp TEXT NOT NULL,
                       username TEXT NOT NULL,
                       quotalimit INTEGER NOT NULL,
		       enddate TEXT NOT NULL,
                       ticketnumber TEXT NOT NULL,
                       addedby TEXT NOT NULL,
		       current BOOL NOT NULL,
		       expirenotice BOOL NOT NULL)''')




