#!/ssg/bin/proposed/quotaproject/v1/bin/python3

"""
Processes scratch request entry for users
"""

import argparse
import datetime
import fcntl
import logging
import os
import pwd
import re
import sqlite3
import sys
from datetime import time
from typing import Union

from prettytable import from_db_cursor


class DateRangeError(BaseException):
    def __init__(self, message: str):
        super().__init__(message)

def check_nolocal():
    """
    Check nolocal file
    """
    path = '/etc/nolocal'
    if os.path.isfile(path):
        log_log(f"{path} exists, Existing")
        sys.exit(f'{path} exists, Exiting!')


# check dir exists: /glade/u/hsg/bin/
if not os.path.isdir('/glade/u/hsg/bin/'):
    print('/glade/u/hsg/quota-automation/ does not exist, Exiting!')
    #exit(1)


# Only run with lock
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


def builddb(db='quota.sqlite'):
    """
    Builds databases for quota and log
    """
    con = sqlite3.connect(r"/glade/u/hsg/quota-automation/" + db)
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


def add_to_db(timestamp, username, quotalimit, enddate, ticket_number, addedby):
    """
    Inserts quota request data into the database
    :param: required timestamp argument
    :param: required username argument
    :param: required quotalimit argument
    :param: required enddate argument
    :param: required ticket_number argument
    :param: required addedby argument
    """
    con = sqlite3.connect(r"/glade/u/hsg/bin/quota.sqlite")
    with con:
        cur = con.cursor()
    data = ('INSERT INTO quotas (timestamp, username, quotalimit, enddate, ticketnumber, addedby) VALUES '
            f'("{timestamp}", "{username}", {quotalimit}, "{enddate}", "{ticket_number}", "{addedby}")')
    if data:
        print(f"Quota update submitted.")
    log_log(': ********')
    log_log(data)
    cur.execute(data)
    con.commit()
    log_log(': ********')


def check_username(username: str) -> str:
    """
    Check username against '/etc/passwd' file
    :param: takes argument username
    :return: return username if meets condition
    """
    try:
        username = username.lower().strip()
        usernames = [i[0] for i in pwd.getpwall()]
        if not username in usernames:
            print(f"Invalid username: {username}")
            log_log(': ========')
            log_log(f"{username} username doesn't exist in '/etc/passwd' entered by {addedby}")
            exit(1)
    except Exception:
        print(f"Invalid username: {username}")
        log_log(f"Invalid username entry. {username} by {addedby}")
        exit(1)
    return username


def check_quota(quota: int) -> int:
    """
    Validates the quota param
    :param quota: required argument for the function
    :return: returns quota if valid otherwise raises an error
    """
    try:
        if not int(quota):
            log_log(f"{quota} is not integer")
            raise TypeError(f"Invalid quota entry: {quota} --Quota must be integers only..")
        if quota > 1024 or quota < 10:
            print(f"Invalid quota entry: {quota} --Quota must be between 10TB and 1024TB.)")
            log_log(f"Out of range quota {quota} request by {addedby}")
            exit(1)
    except KeyError:
        print(f"Invalid quota entry: {quota} --Quota must be between 10TB and 1024TB.")
        log_log(f"Invalid quota entry {quota}")
        exit(1)
    return quota


def check_enddate(date_string: str) -> Union[str, None]:
    """
    Validates quota end date
    :param: data_string required argument for the function
    :return:
    """
    try:
        now = datetime.datetime.now()
        enddate= datetime.datetime.strptime(date_string, '%m-%d-%Y')
        if enddate <= now:
            log_log(f"Out of range quota end date {enddate} requested by {addedby}.")
            raise DateRangeError(f"End date must be in future date. {enddate}")
        return enddate.strftime('%m-%d-%Y')
    except ValueError:
        print(f"Invalid date format or entry. Run 'update_scratch_quota.py --help' for help.")
        exit(1)
    except DateRangeError:
        print(f"Your end date must be in future date.")
        exit(1)

def check_ticketnumber(ticket: str) -> str:
    """
    Validate ticket number
    :param: requires argument for the function
    :return: returns the required date format
    """
    pattern = re.compile('(RC)|(rc)-(\\d{5})')
    if not re.match(pattern, ticket):
        log_log(f"Invalid ticket format {ticket} entered by {addedby}")
        print(f"Enter helpdesk ticket number starts with (RC-01234). You entered {ticket}")
        exit(1)
    else:
        return ticket


def log_log(log_entry):
    """
    logs invalid and successful entries to log file
    """
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    #logfile = 'update_scratch_quota.%s.log' % (date)
    logfile = r"/glade/u/hsg/quota-automation/update_scratch_quota_logs/"'update_scratch_quota.%s.log' % (date)
    logging.basicConfig(filename=logfile, format='%(asctime)s %(message)s', level=logging.INFO)
    logging.info(log_entry)


def view_pending():
    """
    shows pending scratch quota update requests
    :param: takes no argument
    :param: prints out the database
    """
    con = sqlite3.connect(r"/glade/u/hsg/quota-automation/quota.sqlite")
    with con:
        cur = con.cursor()
    print(f"=== Cron job schedule for processing: Mon-Sun 08:00-17:00 every hour on the hour. ===")
    cur.execute("SELECT id, timestamp, username, quotalimit, enddate, ticketnumber, addedby FROM quotas")
    data = from_db_cursor(cur)
    print(data)


def view_history():
    """
    shows history of approved scratch quota updates
    :param: takes no argument
    """
    con = sqlite3.connect(r"/glade/u/hsg/quota-automation/quota.sqlite")
    with con:
        cur = con.cursor()
    print(f" === History of approved scratch quota update requests ===")
    cur.execute("SELECT id, timestamp, username, quotalimit, enddate, ticketnumber, addedby, current, expirenotice FROM history")
    data = from_db_cursor(cur)
    print(data)

if __name__ =="__main__":
    parser = argparse.ArgumentParser(prog='update_scratch_quota')
    subparser = parser.add_subparsers(title='subcommands', dest='subcommand')

    insert_parser = subparser.add_parser('insert', help='insert takes 4 required arguments for updating scratch quota.\
    for help: insert --help')
    insert_parser.add_argument('-u', "--username", help="Enter username", required=True, type=str)
    insert_parser.add_argument('-q', "--quotalimit", help="Enter quota amount in TB (digits only)", required=True, type=int)
    insert_parser.add_argument('-e', "--enddate", help="Enter quota end date in 'mm-dd-yyyy' format", required=True, type=str)
    insert_parser.add_argument('-t', "--ticketnumber", help="Enter ticket number in 'RC-12345' format", required=True, type=str)

    pending_parser = subparser.add_parser('pending', help='shows pending scratch quota update requests. view command takes no arguments')
    history_parser = subparser.add_parser('history', help='shows history of approved scratch quota update requests. \
    history command takes no arguments')

    args = parser.parse_args()
    timestamp = datetime.datetime.now().isoformat()
    addedby = os.getlogin().lower()
    check_nolocal()
    db = 'quota.sqlite'
    builddb(db)

    if args.subcommand == 'insert':
        username = check_username(args.username)
        quotalimit = check_quota(args.quotalimit)
        enddate = check_enddate(args.enddate)
        ticket_number = check_ticketnumber(args.ticketnumber).upper().strip()
        add_to_db(timestamp=timestamp, username=username, quotalimit=quotalimit,
                  enddate=enddate, ticket_number=ticket_number, addedby=addedby)
    elif args.subcommand == 'pending':
        view_pending()
    elif args.subcommand == 'history':
        view_history()

