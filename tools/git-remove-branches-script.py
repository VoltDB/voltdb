#!/usr/bin/python

# Print out scripts for removing either
#  - branches already merged to trunk
#  - branches not merged, but with no current checkins
# This script DOES NOT do the removals - you need to run the
# console output.

#TODO: Make it handle incorrect password the 1st time, then abort


from datetime import date, datetime, timedelta
import getpass
import os
import re
from subprocess import Popen
import subprocess
import sys

import jiratools

exclusions = []

user = getpass.getuser()
password = getpass.getpass('Enter your Jira password: ')
jira_url = 'https://issues.voltdb.com/'
#server_url = 'http://localhost:8080'

def run_cmd(cmd):
    proc = Popen(cmd.split(' '),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out, err) = proc.communicate(input=None)
    return (proc.returncode, out, err)

def get_current_branch():
    current_branch=''
    (returncode, stdout, stderr) = run_cmd ('git branch --no-color')
    if returncode:
        sys.exit('Can\'t get current branch: ' + stderr)
    # Current branch is marked by '* ' at the start
    branch = [b for b in stdout.splitlines() if b.find('* ') == 0]
    if branch:
        branch = branch[0][2:]
    return branch

def get_branch_list(merged):
    branches = []

    print ('git branch -r %s' % '--merged' if merged else '--no-merged' )
    (returncode, stdout, stderr) = run_cmd ('git branch -r %s' % ('--merged' if merged else '--no-merged' ))

    branches = [b.strip() for b in stdout.splitlines() if b.strip().find('origin/') == 0 and b.find('/master') < 0]
    #print branches
    #Filter others from list
    branches = list(set(branches) - set(exclusions))
    branches.sort()

    return branches

def make_delete_branches_script(branches, do_it):
    other_args = ''
    if not do_it:
        other_args = ' --dry-run'
    for b in branches:
        cmd = 'git push origin --delete %s%s' % \
            (b.split('origin/')[1], other_args)
        comment = get_jira_info(b)
        print "%30s %s" %(cmd, comment)

def get_jira_info(b):

    comment = ''
    rg = re.compile('(eng)-?(\d+)', re.IGNORECASE)
    m = rg.search(b)
    if m:
        issue = m.group(1) + '-' + m.group(2)
        #print "##Getting %s" % issue
        ticket = jiratools.get_jira_issue(jira_url, user, password, issue, 'summary,assignee')
        if ticket:
            assignee = 'Unassigned'
            if ticket['fields']['assignee']:
                assignee = ticket['fields']['assignee']['name']
            summary = ticket['fields']['summary']
            #issue_url = jira_url +  'browse/' + issue_key
            comment = "%s: %s" % (assignee, summary)

    return "   # " + comment

def make_archive_branches_script(branches):
    for b in branches:
        comment = get_jira_info(b)
        shortname = b.split('origin/')[1]
        print
        print 'git tag -m "archiving branch %s" archive/%s %s' % (shortname, shortname, b)
        print 'git push origin --delete %s %s' % (shortname, comment)


def weed_out_newer_branches(branches,maxage):
    old_branches = []
    for b in branches:
        cmd = 'git show -s --pretty=format:"%%ci" %s' % b
        (ret,stdout,stderr) = run_cmd(cmd)
        if not ret:
            #print stdout + b
            d = datetime.strptime(stdout.split(' ')[0],'"%Y-%m-%d').date()
            if (date.today() - d) > timedelta(days = maxage):
                old_branches.append(b)
            else:
                print "#  %s is too new: %s" % (b, stdout.strip())

    return old_branches

if __name__ == "__main__":

    delete = False
    #Only run from master
    current_branch = get_current_branch()
    if current_branch != 'master':
        sys.exit('You must be on master. Your current branch is %s.' % current_branch)


    merged=True
    if len(sys.argv) >= 2 and sys.argv[1] == 'unmerged':
        merged=False
        cutoffday=21
        #if len(sys.argv) == 3:
        #    cutoffday=sys.argv[2]

    branch_list = get_branch_list(merged)

    if merged:
        #print ('\n#----------------\n#dry-run script:\n#----------------')
        #make_delete_branches_script(branch_list, False)
        print ('\n#----------------\n#real script:\n#----------------')
        make_delete_branches_script(branch_list, True)
    else:
        print ('\n#----------------\n#tag- script:\n#----------------')
        old_branches = weed_out_newer_branches(branch_list,cutoffday)
        make_archive_branches_script(old_branches)
        print ('\n#----------------\n#Don\'t forget to git push --tags\n#----------------')

