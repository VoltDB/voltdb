#!/usr/bin/python

# Written by Ruth Morgenstein
#
# This program compares the text extracted from the release notes to the release note field
# in JIRA tickets. By default, the program writes information, including any errors,
# to the terminal. The -w,--write argument updates the tickets in JIRA.
#
# REQUIRED PACKAGES:
# - JIRA  1.0.10
# - fuzzywuzzy
#
# HOW TO USE:
# - in the voltdb-docs repo, make sure you've checked out the latest docs
# - in the voltdb repo:
#
#   cd tools
#   1. Do a dry-run of the jira inserts
#       python ./relnotesupdater.py ~/workspace/voltdb-doc/userdocs/releasenotes.xml
#   2. Review the errors on your screen - see if they make sense
#   3. Fix anything in the xml and rerun steps 1 and 2
#   4. Run and actually update
#       python ./relnotesupdater.py ~/workspace/voltdb-doc/userdocs/releasenotes.xml -w
#   5. Scan the issues and make sure the updates are in the right place.
#      Here's a search that finds all issues with non-empty relnotes field that were updated today:
#
#   https://issues.voltdb.com/issues/?jql=project%20%3D%20ENG%20and%20%22Release%20Note%22%20%20is%20not%20empty%20and%20updated%20%3E%20startOfDay()%20order%20by%20updated%20DESC
#

# History:
#  Oct 27, 2020 - ajgent -- Allow entries that already have project prefix. Only
#   prepend "ENG-" if it isn't already a valid JIRA ID.
# May 13 2022 - ajgent -- add parentheses to print statements to work in Python 3.
#  add a local catalog of tickets that have already been checked
#  and a -b (brief) mode to only check new tickets
# May 15, 2022 - ajgent - add help text to arguments.

import argparse
import csv
import getpass
from jira import JIRA
import requests
import sys
import textwrap
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import time
import os

import relnotesparser


# Each row in results is a tuple containing
#  (status, issueID, relnote text, extended status)
results=[]
retval=0


timebuffer = []
def timer(init=False):
    global timebuffer
    t = int(time.time()*1000)
    if init: timebuffer = []
    timebuffer.append(t)
    return t - timebuffer[0]

def printresults(results):
    print( '===============================================================================')
    for (status, issue, relnote, extstatus) in sorted(results):
        print( '%-10s %-6s %s' % (issue, status, extstatus))
        print('\t%s\n' % relnote)

# LOCAL HISTORY
# Save a list of all the tickets that have been updated or verified.
# for brief mode, this avoids having to look up tickets that have already been
# written.
local_history ={}
LOCAL_HISTORY_FILE = os.path.expanduser("~/") + ".voltrelnotes.dat"
def save_local_history():
    global local_history, LOCAL_HISTORY_FILE
    with open(LOCAL_HISTORY_FILE,"w") as fid:
        for key in local_history.keys():
            fid.write(key + "\n")

def read_local_history():
    global local_history, LOCAL_HISTORY_FILE
    if os.path.exists(LOCAL_HISTORY_FILE):
        with open(LOCAL_HISTORY_FILE, "r") as fid:
            for line in fid.readlines():
                if len(line.strip()) > 0: local_history[line.strip()] = True

# Load the local history if it exists
read_local_history()

#Turn off urllib3 spam for InsercurePlatformWarning and SNIMissingWarning
requests.packages.urllib3.disable_warnings()

parser = argparse.ArgumentParser(description="Compare release notes with JIRA tickets and store release note text in the JIRA ticket." +
   " The default mode is dry run. Use -w to update the JIRA tickets.")
parser.add_argument('-u', dest='username', action="store", help="your JIRA username")
parser.add_argument('-p', dest='password', action="store", help="your JIRA password")
parser.add_argument('-w', '--write', dest='dryrun', action='store_false', help="update the JIRA tickets (default mode is a dry run)")
parser.add_argument('-e', '--errors', dest='errors', action='store_true', help="only list the errors")
parser.add_argument('-b', '--brief', dest='brief', action='store_true', help="only check new release notes")
parser.add_argument ('file')
args = parser.parse_args()

username = args.username
password = args.password
errors_only = False
if (args.errors): errors_only = True
if (args.errors and not args.dryrun):
    sys.exit('FATAL: --errors and --dryrun are mutually exclusive.')

#exit(1)

if (not args.username):
    username = getpass.getuser()
if (not args.password):
    password = getpass.getpass('Enter your Jira password: ')

timer(True)

jira_url = 'https://issues.voltdb.com/'

try:
    jira = JIRA(jira_url, basic_auth=(username, password),options=dict(verify=False))
except:
    sys.exit('FATAL: Unable to log in ' + username)

#Get Release Note field id
relnote_field = [f['id'] for f in jira.fields() if 'Release Note' in f['name'] ][0]


def is_valid_jid(jid):
    #Return true of it looks like a valid ticket
    #return jid.split('-')[1].isdigit() and len(jid.split('-')) == 2
    if not len(jid.split('-')) == 2: return False
    return jid.split('-')[1].isdigit()

def cleanstring(str):
    return ' '.join(str.strip().split())

# Get the release notes from relnotesparser as a two column array
reader = relnotesparser.parsefile(args.file)

for row in reader:
    #print row
    fields = len(row)
    if fields < 2:
        results.append(('ERROR', 'NULL', row, 'Not a valid row, too few columns.'))
        continue
    if fields > 2:
        results.append(('ERROR', 'NULL', row, 'Not a valid row, too many columns.'))
        continue
    text = row[1]
    keys = row[0].split(",")
    for key in keys:

        if args.brief and key in local_history.keys():
            # skip already validated tickets
            continue

        # Check to see if it is already a valid JID
        # If so, upcase
        # If not, add a default project ID
        jid = key.strip().upper()
        if not is_valid_jid(jid):
            jid = 'ENG-%s' % (cleanstring(key))
        #print jid

        #Is it valid number?
        if not is_valid_jid(jid):
            results.append(('ERROR', jid, text, 'Not a valid issue #'))
            continue

        #Does it exist in Jira?
        try:
            issue = jira.issue(jid)
        except:
            results.append(('ERROR', jid, text, 'Issue does not exist'))
            continue

        #Get the release note and decide what to do with it
        existing_relnote = None
        try:
            existing_relnote = getattr(issue.fields, relnote_field)
        except Exception as e:
            results.append(('ERROR', jid, text, "Cannot apply changes because " + str(e)))
            continue


        #Has a release note?
        if existing_relnote:
            #Are they the same?
            # Calculate score
            fuzzyscore = fuzz.ratio(cleanstring(existing_relnote), cleanstring(text))
            if fuzzyscore == 100:
                ##release notes are the same. Nothing to do
                local_history[key] = True
                pass
            elif 90 <= fuzzyscore <= 99:
                if (errors_only): continue
                #print "Text comparison SCORE", fuzzyscore
                #print "updating JIRA release note with voltdb-doc release note"
                #infostr = 'Inserted'
                infostr = "Updated from release note (SCORE %s)" % (str(fuzzyscore)+'%')
                if not args.dryrun:
                    issue.update(fields={relnote_field : text})
                    local_history[key] = True  # save in local history if we update
                else:
                    infostr += '-- dry run, no updates done'
                    results.append(('INFO', jid, text, infostr))

            #if (cleanstring(existing_relnote) == cleanstring(text)):
                #Okay - it exists and it matches what is there. Nothing to do
                #pass
            else:
                #Hmm - something else is there. and it is alot different
                #print "Text comparison SCORE", fuzzyscore
                errorstr = 'Another release note already exists (SCORE %s):\n\told note:\n\t%s' % (str(fuzzyscore)+'%',existing_relnote )
                results.append(('ERROR', jid, "new note:\n\t" + text, errorstr))
        else:
            if (errors_only): continue
            infostr = 'Inserted'
            if not args.dryrun:
                issue.update(fields={relnote_field : text})
                local_history[key] = True  # save in local history if we update

            else:
                infostr += '-- dry run, no updates done'
            results.append(('INFO', jid, text, infostr))

if (errors_only and len(results) == 0): print("SUCCESS - No errors found.")
printresults(results)
if args.dryrun:
    print('!!!No work done - this is a dry run!!!')

# Save the local history
save_local_history()

print( str(timer()/1000.0) + " seconds elapsed time.")