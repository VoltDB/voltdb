#/usr/bin/python

import sys, os
import urllib2


def getdata(url, build):
    url = 'http://ci.voltdb.lan:8080/' + url + str(build) + "/api/python"
    #print url
    return eval(urllib2.urlopen(url).read())

if __name__ == '__main__':

    #DATA = {"_class":"hudson.model.FreeStyleBuild","actions":[{"_class":"hudson.model.ParametersAction","parameters":[{"_class":"hudson.model.StringParameterValue","name":"GIT_BRANCH","value":"origin/ENG-12931-PoisonPillInEE"},{"_class":"hudson.model.StringParameterValue","name":"GIT_REPO","value":"voltdb"},{"_class":"hudson.model.StringParameterValue","name":"REPOSITORY_VOLTDB","value":"git@github.com:VoltDB/voltdb.git"},{"_class":"hudson.model.StringParameterValue","name":"REPOSITORY_PRO","value":"git@github.com:VoltDB/pro.git"},{"_class":"hudson.model.StringParameterValue","name":"GIT_REPOSITORY_PRO","value":"git@github.com:VoltDB/pro.git"},{"_class":"hudson.model.StringParameterValue","name":"GIT_REPOSITORY_VOLTDB","value":"git@github.com:VoltDB/voltdb.git"}]},{"_class":"hudson.model.CauseAction","causes":[{"_class":"hudson.model.Cause$UpstreamCause","shortDescription":"Started by upstream project \"SyncProjectBranchesWithJenkins\" build number 33,941","upstreamBuild":33941,"upstreamProject":"SyncProjectBranchesWithJenkins","upstreamUrl":"job/SyncProjectBranchesWithJenkins/"}]},{},{},{},{}],"artifacts":[],"building":False,"description":None,"displayName":"#31944.voltdb origin/ENG-12931-PoisonPillInEE","duration":23487,"estimatedDuration":24740,"executor":None,"fullDisplayName":"SyncStartBranchJobs #31944.voltdb origin/ENG-12931-PoisonPillInEE","id":"31944","keepLog":False,"number":31944,"queueId":13006,"result":"SUCCESS","timestamp":1501871991747,"url":"http://ci:8080/job/SyncStartBranchJobs/31944/","builtOn":"","changeSet":{"_class":"hudson.scm.EmptyChangeLogSet","items":[],"kind":None}}
    #print DATA
    DATA = getdata(os.environ['JOB_BASE_NAME'], '/'+ os.environ['BUILD_NUMBER'])

    affectedPaths = []
    while True:
        #for f in DATA.iteritems():
        #    print f
        # look at job's changeset
        cs = dict(DATA['changeSet'])

        if len(cs['items']) > 0:
            # process changes
            for i in cs['items']:
                affectedPaths.extend(i['affectedPaths'])
            print affectedPaths
            sys.exit(len(affectedPaths))

        else:
            actions = DATA['actions']
            for a in actions:
                if 'causes' in a:
                    upstream = a['causes']
                    if len(upstream) > 0:
                        upstream = upstream[0]
                        DATA = getdata(upstream['upstreamUrl'], str(upstream['upstreamBuild']))
                        break
