# Created March 8, 2021
#
# Review the release notes tagged with JIRA ticket numbers to see how many 
# mismatches exist before processing against JIRA itself.
#
# A companion to relnotesparser and relnotesupdater
#
# This program allows you to specify multiple files, directories and/or wildcards.

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

import time


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
parser.add_argument('-v', '--verbose', dest='verbose', action="store_true")
parser.add_argument('-q', '--quiet', dest='quiet', action="store_true")
parser.add_argument('--debug', dest='debug', action="store_true")

#parser.add_argument('-p', dest='password', action="store")
#parser.add_argument('-w', '--write', dest='dryrun', action='store_false')
#parser.add_argument('-e', '--errors', dest='errors', action='store_true')

parser.add_argument ('file',nargs='+')
args = parser.parse_args()

for f in args.file:
    if args.debug: print str(f)
    

def is_valid_jid(jid):
    #Return true of it looks like a valid ticket
    #return jid.split('-')[1].isdigit() and len(jid.split('-')) == 2
    if not len(jid.split('-')) == 2: return False
    return jid.split('-')[1].isdigit()

def cleanstring(str):
    return ' '.join(str.strip().split())
    
def add_mismatch(jid):
    global mismatches
    c = 1
    if jid in mismatches.keys(): 
        c = mismatches[jid] + 1
    mismatches[jid] = c

def add_match(jid):
    global good_matches
    c = 1
    if jid in good_matches.keys(): 
        c = good_matches[jid] + 1
    good_matches[jid] = c

def cleanstring(str):
    return ' '.join(str.strip().split())
    
mismatches = {}
good_matches = {}

print "Start..."
start_time = time.time()
consolidated = {}
references = {}

total_count = 0
unique_count = 0

# Get the release notes from relnotesparser as a two column array
for f in args.file:
    reader = relnotesparser.parsefile(f)

    for row in reader:
        total_count += 1
        # resolve the names
        jids = row[0].split(",")
        for j in jids:
            if not is_valid_jid(j):
                if len(j.split('-') ) > 1:
                    print "Invalid ID " + j
                    continue
                else:
                    j = "ENG-" + j
                    #print "Creating ID " + j + "..."
            if j in consolidated:
                references[j].append(f)
                fuzziness = fuzz.ratio(cleanstring(consolidated[j]), cleanstring(row[1]))
                #if not (consolidated[j] == row[1]):
                if fuzziness < 95:
                    if not args.quiet:
                        print "Mismatched definitions for " + j + " (" + \
                            str(fuzziness) +  ")"
                        t = ["1: " + f,"2: " + references[j][0]]
                        if args.verbose:
                            t[0] += "\n" + row[1] + "\n"
                            t[1] += "\n" + consolidated[j]
                        print t[0]
                        print t[1] + "\n"
                    add_mismatch(j)
                else:
                    add_match(j)
            else:
                consolidated[j] = row[1]
                references[j] = [f]
                unique_count += 1
                

end_time = time.time()
print "Processing took " + str(end_time - start_time) + " Seconds."
if args.verbose:
    print str(len(mismatches)) + " mismatches found out of " + str(total_count) + " hits" \
        + " from " + str(len(args.file)) + " files."

print "Mismatches:"
for k in mismatches.keys():
    print " " + k + "   " + str(mismatches[k])
