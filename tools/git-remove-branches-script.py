#!/usr/bin/python

# Find all the merged branches and print a script
# (to console) for a dry-run and real run.

import getopt
import os
from subprocess import Popen
import subprocess
import sys

exclusions = []

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

def get_merged_branch_list():
    branches = []
    (returncode, stdout, stderr) = run_cmd ('git branch -r --merged')
    #Find branches that have been merged (but not master)
    branches = [b.strip() for b in stdout.splitlines() if b.find('/master') < 0]
    #print branches
    #Filter others from list
    branches = list(set(branches) - set(exclusions))
    branches.sort()
    #for b in branches:
    #    print '  ' + b
    return branches

def make_delete_branches_script(branches, do_it):
    other_args = ''
    if not do_it:
        other_args = ' --dry-run'
    for b in branches:
        cmd = 'git push origin --delete %s%s' % \
            (b.split('origin/')[1], other_args)
        print cmd

if __name__ == "__main__":

    delete = False
    #Only run from master
    current_branch = get_current_branch()
    if current_branch != 'master':
        sys.exit('You must be on master. Your current branch is %s.' % current_branch)
    delete = False
    branch_list = get_merged_branch_list()

    print ('\n----------------\ndry-run script:\n----------------')
    make_delete_branches_script(branch_list, False)

    print ('\n----------------\nreal script:\n----------------')

    make_delete_branches_script(branch_list, True)
