#!/usr/bin/python

import argparse
import csv
import getpass
from jira import JIRA
import requests
import sys
import textwrap
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


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
parser.add_argument ('file')
args = parser.parse_args()

username = args.username
password = args.password

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



with open (args.file) as csvfile:
    reader = csv.reader(csvfile, escapechar='\\')
    for row in reader:
        #print row
        fields = len(row)
        if fields < 2:
            results.append(('ERROR', 'NULL', row, 'Not a valid row'))
            continue
        text = row[-1]
        #print text
        #
        for i in range(0, fields - 1):
            key = row[i]
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
                    print "SCORE", fuzzyscore
                    print "updating JIRA release note with voltdb-doc release note"
                    infostr = 'Inserted'
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
                    print "SCORE", fuzzyscore
                    errorstr = 'Another release note already exists:\n\told note:\n\t%s' % (existing_relnote)
                    results.append(('ERROR', jid, "new note:\n\t" + text, errorstr))
            else:
                infostr = 'Inserted'
                if not args.dryrun:
                    issue.update(fields={relnote_field : text})
                else:
                    infostr += '-- dry run, no updates done'
                results.append(('INFO', jid, text, infostr))

printresults(results)
if args.dryrun:
    print '!!!No work done - this is a dry run!!!'
