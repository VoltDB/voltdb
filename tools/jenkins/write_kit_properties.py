#!/usr/bin/python


import os
import sys
import time

repos = {}

# Extract commit/branch info from GIT_URL* environment variables
for i in range(0,10):
    postfix = ""
    if i != 0:
        postfix = '_%s' %i
    else:
        postfix  = ''
    if 'GIT_URL' + postfix not in os.environ:
        break

    # Extract the common name for the given repo from the url.
    # If it's an "internal" repo, change it's common name to voltdb.
    familiar_name = os.environ['GIT_URL' + postfix].split('/')[-1]
    familiar_name = familiar_name[:-4]
    if familiar_name == "internal":
        familiar_name = "voltdb"

    repos[familiar_name] = {
        'url': os.environ['GIT_URL' + postfix],
        'commit': os.environ['GIT_COMMIT' + postfix],
        'branch': os.environ['GIT_BRANCH' + postfix].split('origin/')[-1],
        # skip unusued GIT_CHECKOUT_DIR, GIT_COMMITTER_EMAIL, nor GIT_COMMITER_NAME
    }

# add jenkins variables to properties
jenkins_vars=[
    'BUILD_NUMBER',
    'BUILD_ID',
    'BUILD_URL',
    'NODE_NAME',
    'JOB_NAME',
    'BUILD_TAG',
]

sv_prefix = 'kit_'
outfile = 'workspace_params.properties'

if len(sys.argv) == 2:
    outfile = sys.argv[1]

properties = {}
for jv in jenkins_vars:
    sv = sv_prefix + jv.lower()
    if jv in os.environ:
        properties[sv] = os.environ[jv]
    else:
        print("missing env var '%s', setting to 'unknown'" % jv)
        properties[sv] = 'unknown'

# add repo info to properties
for r in repos:
    values = repos[r]
    properties['kit_git_url__' + r] = values['url']
    properties['kit_git_commit__' + r] = values['commit']
    properties['kit_git_branch__' + r] = values['branch']

# Apprunner copies kit_git_url and kit_git_branch to triage db records.
# Until it uses the kit_git_*__ variables above, we include these.
properties['kit_git_branch'] = 'origin/' + repos['voltdb']['branch']
properties['kit_git_commit'] = repos['voltdb']['commit']
properties['kit_git_url'] = repos['voltdb']['url']

#BRANCH is used by downstream jobs
branch = repos['voltdb']['branch']
if branch != repos['pro']['branch']:
    branch += ("/" + repos['pro']['branch'])

properties['BRANCH'] = branch

with open (outfile,'w') as f:
    for sv in properties:
        f.write( sv + '=' +  properties[sv] + '\n')
