#!/usr/bin/python

# Print out scripts for removing either
#  - branches already merged to trunk
#  - branches not merged, but with no current checkins
# This script DOES NOT do the removals - you need to run the
# console output.

#TODO: Make it handle incorrect password the 1st time, then abort

import getpass
import jiratools
from optparse import OptionParser
import re
from subprocess import Popen
import subprocess
import sys
import time
import traceback
import os

dryrun=sys.argv[1]
# set exclusions if there are any branches that should not be listed
exclusions = ['^master','^release-[1-9]?[0-9.]+\.x$','^release-[1-9]?[0-9.]?[0-9]?']
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
    return [b.strip() for b in stdout.splitlines() if b.strip().find('origin/release-') == 0 ]

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


def get_final_comment(num_listed_branches, num_total_branches, older_than_days,
                      num_skipped_branches=0, merged=False, num_exceptions=0):
    """Returns a final comment, including whether all branches were completed
       or we stopped due to reaching the maximum number, whether they were
       merged or unmerged branches, how old (minimum) the branches to be
       deleted are, and how many branches were skipped. If the number of
       exceptions is greater than 0, it prints the entire comment with an
       additional part about the exceptions, and exits with an error code.
    """
    completed = (num_listed_branches == num_total_branches)
    comment  = ('\n#\n# Completed list of %d ' if completed else
                '\n#\n# Stopping, listed maximum number of %d ') \
                % num_listed_branches

    comment += ('(' if merged else '(un') + 'merged) branch'
    comment += 'es' if (num_listed_branches != 1) else ''
    if not completed:
        comment += ', out of %d,' % num_total_branches

    comment += ' older than %d days, to be deleted' % older_than_days
    if num_skipped_branches > 0:
        comment += ',\n# with %d branch' % num_skipped_branches    \
                    + ('es' if (num_skipped_branches > 1) else '') \
                    + ' skipped'

    if num_exceptions:
        print comment+'.\n#\n'
        plural = 's' if num_exceptions > 1 else ''
        print '# Failed due to %d exception%s (see above); exit with error code: %d\n' \
                % (num_exceptions, plural, num_exceptions)
        sys.exit(num_exceptions)

    return comment+'.\n#\n'


def make_delete_branches_script(branch_infos, olderthan, max_num_branches=10,
                                skip_branch_names=[], dry_run=False):
    """Prints a script that can be used to delete merged branches that are older
       than the specified number of days, up to a specified maximum number of
       branches, and skipping those whose names contain any of several substrings.
    """
    other_args = ''
    if dry_run:
        other_args = ' --dry-run'

    num_branches=0
    num_skipped_branches=0
    for bi in branch_infos:
        num_branches+=1
        if num_branches > max_num_branches:
            print get_final_comment(max_num_branches, len(branch_infos)-num_skipped_branches,
                                    olderthan, num_skipped_branches, merged=True)
            return
        b = bi['name']
        sbn = [bn for bn in skip_branch_names if bn in b]
        if sbn:
            num_branches-=1
            num_skipped_branches+=1
            print "\n# Skipping branch '%s', because its name contains '%s'" \
                    % (b, "', '".join(sbn))
            continue
        cmd = 'git push origin --delete %s%s' % (b, other_args)
        comment = make_comment(bi)
        print '\n' + comment.encode('utf-8') + '\n' + cmd

    print get_final_comment(num_branches, num_branches, olderthan, num_skipped_branches, merged=True)


def make_comment(bi):
    comment = '# %-20s last checkin %s %s by %s' % \
        (bi['name'], bi['datetime'], bi['humantime'], bi['email'])
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
            comment = "# %s %s %s: %s" % (issue, status_resolution.upper(), assignee, summary)

    return comment


def get_tagname(branch_name):
    """Returns a tag name, to be used to "archive" the branch; normally, this
       will be 'archive/<branch_name>', but if that tag already exists in the
       git repository, then it will try 'archive/<branch_name>-2', then
       'archive/<branch_name>-3', and so on.
    """
    max_num_tags_per_branch = 10
    init_tagname = 'archive/' + branch_name
    for i in xrange(1, max_num_tags_per_branch+1):
        tagname = init_tagname + ("-"+str(i) if (i > 1) else '' )
        found_tagname = subprocess.check_output(['git', 'tag', '-l', tagname])
        if found_tagname:
            message  = '\n' if (i <= 1) else ''
            message += '# WARNING: Tagname already exists: ' + tagname + '\n'
            message += '#                        will try: ' + init_tagname+"-"+str(i+1) \
                        if (i < max_num_tags_per_branch) else ''
            print message
        else:
            return tagname

    raise RuntimeError("Unable to create tags 'archive/%s-X': maximum of %d such tags already exist!"
                       % (branch_name, max_num_tags_per_branch) )


def make_archive_branches_script(branch_infos, olderthan, max_num_branches=10,
                                 skip_branch_names=[], dry_run=False):
    """Prints a script that can be used to "archive" (or tag, technically) and
       delete unmerged branches that are older than the specified number of days,
       up to a specified maximum number of branches, and skipping those whose
       names contain any of several substrings.
    """
    other_args = ''
    if dry_run:
        other_args = ' --dry-run'

    num_branches=0
    num_skipped_branches=0
    num_tagname_exceptions=0
    for bi in branch_infos:
        num_branches+=1
        if num_branches > max_num_branches:
            print get_final_comment(max_num_branches, len(branch_infos)-num_skipped_branches,
                                    olderthan, num_skipped_branches, merged=False)
            return
        b = bi['name']
        sbn = [bn for bn in skip_branch_names if bn in b]
        found_tagname_exception = False
        if not sbn:
            try:
                tagname = get_tagname(b)
            except:
                found_tagname_exception = True
                num_tagname_exceptions+=1
                message  = "\n# Skipping branch '%s', because of the following " \
                            % b + "exception while getting a tagname:\n#   "
                message += "\n#   ".join( traceback.format_exc().split('\n') )
        if sbn or found_tagname_exception:
            num_branches-=1
            num_skipped_branches+=1
            if not found_tagname_exception:
                message = "\n# Skipping branch '%s', because its name contains '%s'" \
                            % (b, "', '".join(sbn))
            print message
            continue
        comment = make_comment(bi)
        print '\n' + comment.encode('utf-8')
        print 'git tag %s origin/%s -m "archiving branch %s"' % (tagname, b, b)
        print 'git push origin %s' % (tagname)
        print 'git push origin --delete %s %s' % (other_args, b)

    print get_final_comment(num_branches, num_branches, olderthan, num_skipped_branches,
                            merged=False, num_exceptions=num_tagname_exceptions)


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
    parser.add_option('--keep-branches', dest = 'keep_branches', action = 'store',
                      help = "branch names (or substrings thereof) to be kept, i.e., "
                      + "not to be removed or archived; aside from the standard branch "
                      + "name substrings, which are always kept (keep, feature, integ)",
                      default = '');
    parser.add_option('--release', dest = 'release', action = 'store',
                      help = "a release branch for checking merges, e.g. 'origin/release-8.4.x'");

    (options,args) = parser.parse_args()
    if not options.merged and (options.release != 'master' and options.release != None):
        parser.error('Unmerged branches can only be checked against master.')

    if options.use_jira:
        user = options.username
        password = options.password or getpass.getpass('Enter your Jira password: ')

    # Initialize the list of branch names (or substrings) to be skipped, i.e.,
    # branches whose names contain any of these strings will not be deleted
    # (or listed for possible deletion)
    print "##dryrun is %s" % dryrun
    skip_branch_names = ['keep', 'feature', 'integ']
    if options.keep_branches:
        skip_branch_names.extend(options.keep_branches.split(','))

    #Get the branch list
    branch_names = get_branch_list(options.merged, options.release)
    format_string = DELIMITER.join([gitshowmap[key] for key in sorted(gitshowmap)])
    #Iterate over it and get a bunch of commit information using git log

    print "##branch_names is %s" % branch_names

    branch_infos = []
    for b in branch_names:
        if b.find("/refs/tags/") != -1:
            print "##Skip tag!"
            break
        elif b.find("origin/feature") != -1:
           branch_info = {}
           branch_info['name'] = b.split('/')[2] 
        else:
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
                                    options.max_num_branches, skip_branch_names,
                                    dry_run=False)
    else:
        make_archive_branches_script(old_branch_infos, options.olderthan,
                                     options.max_num_branches, skip_branch_names,
                                     dry_run=False)
