#!/usr/bin/python

# Print out scripts for removing either
#  - branches already merged to trunk
#  - branches not merged, but with no current checkins
# This script DOES NOT do the removals - you need to run the
# console output.

#TODO: Make it handle incorrect password the 1st time, then abort

#from datetime import date, datetime, timedelta
import getpass
from optparse import OptionParser
import re
from subprocess import Popen
import subprocess
import sys
import time

import jiratools

# set exclusions if there are any branches that should not be listed
exclusions = ['^master','^release-[1-9]?[0-9.]+\.x$']
combined_regex= '(' + ')|('.join(exclusions) + ')'
jira_url = 'https://issues.voltdb.com/'
gitshowmap = \
    {
    "unixtime":"%ct",
    "datetime":"%ci",
    "humantime":"%cr",
    "email":"%ce",
    }
DELIMITER='^'

def run_cmd(cmd):
    proc = Popen(cmd.split(' '),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out, err) = proc.communicate(input=None)
    return (proc.returncode, out, err)

def get_release_branches():
    git_cmd = 'git branch -r'
    print ('#\n# git command: %s\n#' % git_cmd)
    (returncode, stdout, stderr) = run_cmd (git_cmd)
    return [b.strip() for b in stdout.splitlines() if b.strip().find('origin/release-') == 0 and b.strip()[-2:] == '.x']

def get_branch_list(merged, base=None):
    branches=[]

    if base:
        basebranches=[base]
    else:
        basebranches = get_release_branches()

    #If merged, check release branches and master

    for b in basebranches:
        git_cmd = 'git branch -r %s %s' % ('--merged' if merged else '--no-merged', b)
        print ('#\n# git command: %s\n#' % git_cmd)
        (returncode, stdout, stderr) = run_cmd (git_cmd)

        #only want branches at origin and don't want HEAD listed
        found_branches = [b.strip() for b in stdout.splitlines()if b.strip().find('origin/') == 0 and b.find('HEAD') < 0]
        branches = list(set().union(branches, found_branches))

    return [b for b in branches if not re.match(combined_regex,b.split('origin/')[-1])]

def make_delete_branches_script(branch_infos, olderthan, max_num_branches=10,
                                dry_run=False):
    other_args = ''
    if dry_run:
        other_args = ' --dry-run'

    num_branches=0
    for bi in branch_infos:
        num_branches+=1
        if num_branches > max_num_branches:
            print '\n#\n# Stopping, reached maximum number of (merged) branches', \
                  'to list (& delete): %d, out of %d older than %d days.\n#' % \
                  (max_num_branches, len(branch_infos), olderthan)
            return
        b = bi['name']
        cmd = 'git push origin --delete %s%s' % \
            (b, other_args)
        comment = make_comment(bi)
        print
        print comment.encode('utf-8')
        print cmd
    print '\n#\n# Completed list of %d (merged) branches older than %d days, to be deleted.\n#' \
          % (num_branches, olderthan)

def make_comment(bi):
    comment = '#%-20s last checkin %s %s by %s' % \
        (bi['name'],bi['datetime'],bi['humantime'],bi['email'])
    if options.use_jira:
        ticket_summary = get_jira_info(bi['name'])
        if ticket_summary:
            comment +=  ('\n' + ticket_summary)
    return comment

def get_jira_info(b):

    comment = None
    rg = re.compile('(eng)-?(\d+)', re.IGNORECASE)
    m = rg.search(b)
    if m:
        issue = m.group(1) + '-' + m.group(2)
        #print "##Getting %s" % issue
        ticket = jiratools.get_jira_issue(jira_url, user, password, issue, 'summary,assignee,status,resolution')
        if ticket:
            assignee = 'Unassigned'
            if ticket['fields']['assignee']:
                assignee = ticket['fields']['assignee']['name']
            summary = ticket['fields']['summary']
            #issue_url = jira_url +  'browse/' + issue_key
            status_resolution = ticket['fields']['status']['name']
            if status_resolution in ('Closed','Resolved'):
                status_resolution += '/' + ticket['fields']['resolution']['name']
            comment = "#%s %s %s: %s" % (issue, status_resolution.upper(), assignee, summary)

    return comment

def make_archive_branches_script(branch_infos, olderthan, max_num_branches=10,
                                 dry_run=False):
    other_args = ''
    if dry_run:
        other_args = ' --dry-run'
    num_branches=0
    for bi in branch_infos:
        num_branches+=1
        if num_branches > max_num_branches:
            print '\n#\n# Stopping, reached maximum number of (unmerged) branches', \
                  'to list (& "archive"): %d, out of %d older than %d days.\n#' % \
                  (max_num_branches, len(branch_infos), olderthan)
            return
        comment = make_comment(bi)
        tagname = "archive/" + bi['name']
        print
        print comment
        print 'git tag -m "archiving branch %s" %s origin/%s' % \
            (bi['name'], tagname, bi['name'])
        print 'git push origin %s' % (tagname)
        print 'git push origin --delete %s %s' % (other_args, bi['name'])
    print '\n#\n# Completed list of %d (unmerged) branches older than %d days, to be archived.\n#' \
          % (num_branches, olderthan)

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option('--no-jira', dest='use_jira', action = 'store_false',
                      help = 'Don\'t look up jira ticket',
                      default = 'True')
    parser.add_option('-u', '--username', dest = 'username', action = 'store',
                      help = 'username to use for Jira lookups',
                      default = getpass.getuser())
    parser.add_option('-p', '--password', dest = 'password', action = 'store',
                      help = 'password to use for Jira lookups')
    parser.add_option('--no-merged', dest = 'merged', action = 'store_false',
                      help = "find branches that are not merged to master",
                      default = True)
    parser.add_option('--older', dest = 'olderthan', action = 'store',
                      help = "the age, in days, of unmerged branches to list",
                      type="int", default = 60);
    parser.add_option('--max-num-branches', dest = 'max_num_branches', action = 'store',
                      help = "the maximum number of branches to list (and to remove, potentially)",
                      type="int", default = 100);
    parser.add_option('--release', dest = 'release', action = 'store',
                      help = "a release branch for checking merges, e.g. 'origin/release-8.4.x'");

    (options,args) = parser.parse_args()
    if not options.merged and (options.release != 'master' and options.release != None):
        parser.error('Unmerged branches can only be checked against master.')

    if options.use_jira:
        user = options.username
        password = options.password or getpass.getpass('Enter your Jira password: ')

    #Get the branch list
    branch_names = get_branch_list(options.merged, options.release)
    format_string = DELIMITER.join([gitshowmap[key] for key in sorted(gitshowmap)])
    #Iterate over it and get a bunch of commit information using git log
    branch_infos = []
    for b in branch_names:
        branch_info = {}
        branch_info['name'] = b.split('/')[1]

        #Get the git log info and pack it into a branch_info dictionary
        cmd = 'git log -1 --format=%s %s' % (format_string, b)
        (ret,stdout,stderr) = run_cmd(cmd)
        if not ret:
            values = stdout.rstrip().split(DELIMITER)
            for k,v in zip(sorted(gitshowmap),values):
                try:
                    branch_info[k] = float(v)
                except ValueError:
                    branch_info[k] = v
            branch_infos.append(branch_info)
        else:
            sys.stderr.write( "ERROR: Can't get git information for %s\n" % b)
            sys.stderr.write( "\tcmd = %s\n" % cmd)
            sys.stderr.write( "\tstderr=%s\n" % stderr)

    now = time.time()

    sorted_branch_infos = sorted(branch_infos, key=lambda bi:bi['unixtime'])
    old_branch_infos = [bi for bi in sorted_branch_infos if (now - bi['unixtime']) > options.olderthan * 60* 60* 24]

    if options.merged:
        make_delete_branches_script(old_branch_infos, options.olderthan,
                                    options.max_num_branches, dry_run=False)
    else:
        make_archive_branches_script(old_branch_infos, options.olderthan,
                                     options.max_num_branches, dry_run=False)

