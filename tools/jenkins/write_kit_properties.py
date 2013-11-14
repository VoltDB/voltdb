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
stored_vars_map['BRANCH'] = stored_vars_map[sv_prefix + 'git_branch'].split('/')[-1]


with open (outfile,'w') as f:
    for sv in stored_vars_map:
        f.write( sv + '=' +  stored_vars_map[sv] + '\n')

exit()
if os.environ['USER'] in ['test','jenkins']:
    try:
        import mysql.connector as mdb
        con = mdb.connect(host='volt2', user='test', database='qa')
        cur = con.cursor()
        columns = ','.join([k.replace(sv_prefix,'',1) for k in sorted(stored_vars_map)])
        values = ','.join(['%('+ k + ')s' for k in sorted(stored_vars_map)])
        insert_sql = ("INSERT INTO `apprunner-kits` (%s) values (%s);" 
                      % (columns,values))
        print insert_sql      
        cur.execute(insert_record,stored_vars_map)
        con.commit()
        cur.close()
        con.close()

    except Exception as e:
        print >>sys.stderr, "Exception raised recording kit record '%s'" % e
        raise e

