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

import argparse
import csv
import getpass
from jira import JIRA
import requests
import sys
import textwrap
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import relnotesparser


# Each row in results is a tuple containing
#  (status, issueID, relnote text, extended status)
results=[]
retval=0

def printresults(results):
    print '==============================================================================='
    for (status, issue, relnote, extstatus) in sorted(results):
        print '%-10s %-6s %s' % (issue, status, extstatus)
        print '\t%s\n' % relnote

#Turn off urllib3 spam for InsercurePlatformWarning and SNIMissingWarning
requests.packages.urllib3.disable_warnings()

parser = argparse.ArgumentParser()
parser.add_argument('-u', dest='username', action="store")
parser.add_argument('-p', dest='password', action="store")
parser.add_argument('-w', '--write', dest='dryrun', action='store_false')
parser.add_argument('-e', '--errors', dest='errors', action='store_true')
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
    password = getpass.getpass('Enter your Jira password: ')

jira_url = 'https://issues.voltdb.com/'


try:
    jira = JIRA(jira_url, basic_auth=(username, password),options=dict(verify=False))
except:
    sys.exit('FATAL: Unable to log in ' + username)

#Get Release Note field id
relnote_field = [f['id'] for f in jira.fields() if 'Release Note' in f['name'] ][0]



def is_valid_jid(jid):
    #Return true of it looks like a valid ticket
    return jid.split('-')[1].isdigit() and len(jid.split('-')) == 2

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
        #key = row[i]
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
        existing_relnote = getattr(issue.fields, relnote_field)
        #Has a release note?
        if existing_relnote:
            #Are they the same?
            # Calculate score
            fuzzyscore = fuzz.ratio(cleanstring(existing_relnote), cleanstring(text))
            if fuzzyscore == 100:
                ##release notes are the same. Nothing to do
                pass
            elif 90 <= fuzzyscore <= 99:
                if (errors_only): continue
                #print "Text comparison SCORE", fuzzyscore
                #print "updating JIRA release note with voltdb-doc release note"
                #infostr = 'Inserted'
                infostr = "Updated from release note (SCORE %s)" % (str(fuzzyscore)+'%')
                if not args.dryrun:
                    issue.update(fields={relnote_field : text})
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
            else:
                infostr += '-- dry run, no updates done'
            results.append(('INFO', jid, text, infostr))

if (errors_only and len(results) == 0): print("SUCCESS - No errors found.")
printresults(results)
if args.dryrun:
    print '!!!No work done - this is a dry run!!!'
