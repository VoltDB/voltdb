#!/usr/bin/python


import os
import sys
import time

jenkins_vars=[
    'BUILD_NUMBER',
    'BUILD_ID',
    'BUILD_URL',
    'NODE_NAME',
    'JOB_NAME',
    'BUILD_TAG',
    'GIT_COMMIT',
    'GIT_BRANCH',
    'GIT_URL',
]

repos = dict(voltdb='unknown', pro='unknown')

def map_repo(url, branch):
    for r in repos.iterkeys():
        if r in url:
            repos[r] = branch.split('origin/')[-1]

sv_prefix = 'kit_'
outfile = 'workspace_params.properties'

if len(sys.argv) == 2:
    outfile = sys.argv[1]

#Set up the stored_vars_map (some might not be set)
stored_vars_map = {}
for jv in jenkins_vars:
    sv = sv_prefix + jv.lower()
    if jv in os.environ:
        stored_vars_map[sv] = os.environ[jv]
    else:
        stored_vars_map[sv] = 'unknown'

map_repo(stored_vars_map['kit_git_url'], stored_vars_map['kit_git_branch'])

#Map all additional git repos
for i in range(1, 10):
    jv = 'GIT_URL_%s' % i
    if jv not in os.environ:
        break
    map_repo(os.environ[jv], os.environ[jv.replace('URL', 'BRANCH')])

#BRANCH is used by downstream jobs
branch = repos['voltdb']
if repos['voltdb'] != repos['pro']:
    branch += ("/" + repos['pro'])

stored_vars_map['BRANCH'] = branch

with open (outfile,'w') as f:
    for sv in stored_vars_map:
        f.write( sv + '=' +  stored_vars_map[sv] + '\n')


