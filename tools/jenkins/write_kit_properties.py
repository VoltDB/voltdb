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
]

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


#BRANCH is used by downstream jobs
stored_vars_map['BRANCH'] = os.getenv('BRANCH') or stored_vars_map[sv_prefix + 'git_branch'].split('/')[-1]

with open (outfile,'w') as f:
    for sv in stored_vars_map:
        f.write( sv + '=' +  stored_vars_map[sv] + '\n')


