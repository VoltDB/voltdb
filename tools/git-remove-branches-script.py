#!/usr/bin/python

# Print out scripts for removing either
#  - branches already merged to trunk
#  - branches not merged, but with no current checkins
# This script DOES NOT do the removals - you need to run the
# console output.

#TODO: Make it handle incorrect password the 1st time, then abort

from optparse import OptionParser
from datetime import date, datetime, timedelta
import getpass
import re
from subprocess import Popen
import subprocess

import jiratools

# set exclusions if there are any branches that should not be listed
exclusions = ['master',]
jira_url = 'https://issues.voltdb.com/'

def run_cmd(cmd):
    proc = Popen(cmd.split(' '),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out, err) = proc.communicate(input=None)
    return (proc.returncode, out, err)

def get_branch_list(merged):
    branches = []

    git_cmd = 'git branch -r %s' % ('--merged' if merged else '--no-merged')
    print ('#\n# git command: %s\n#' % git_cmd)
    (returncode, stdout, stderr) = run_cmd (git_cmd)

    #only want branches at origin and don't want HEAD listed
    branches = [b.strip() for b in stdout.splitlines()if b.strip().find('origin/') == 0 and b.find('HEAD') < 0]

    #Filter others from list
    origin_exclusions = ['origin/' + b for b in exclusions]
    branches = list(set(branches) - set(origin_exclusions))
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
        print "%-40s %s" %(cmd, comment)

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
            comment = " # %s: %s" % (assignee, summary)

    return comment

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
                print ("#  %-25s last checkin %-2d days ago - %s" %
                       (b, (date.today()-d).days, d))
    return old_branches

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option('-u', '--username', dest = 'username', action = 'store',
                      help = 'username to use for Jira lookups',
                      default = getpass.getuser())
    parser.add_option('-p', '--password', dest = 'password', action = 'store',
                      help = 'password to use for Jira lookups')
    parser.add_option('--no-merged', dest = 'merged', action = 'store_false',
                      help = "find branches that are not merged to master",
                      default = True)
    parser.add_option('--older', dest = 'olderthan', action = 'store',
                      help = "age of unmerged branches to list",
                      default = 21);


    (options,args) = parser.parse_args()

    user = options.username
    password = options.password or getpass.getpass('Enter your Jira password: ')

    if not options.merged:
        print ('# Branches with checkins within %d days will not be listed'
               % options.olderthan)

    branch_list = get_branch_list(options.merged)

    if options.merged:
        make_delete_branches_script(branch_list, True)
    else:
        old_branches = weed_out_newer_branches(branch_list, options.olderthan)
        make_archive_branches_script(old_branches)
        print ('\n#----------------\n#Don\'t forget to git push --tags\n#----------------')

