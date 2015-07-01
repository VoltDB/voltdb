#!/bin/bash -e
#
# this is the script for the jenkins job of the same name
#
shopt -s nocasematch
env


JPW="test:adhocquery"

startVoltdbJobs() {

    JOBS="
        branch-1-licensecheck-master
        branch-2-sqlcoverage-master
        branch-2-memcheck-lite-master
        branch-2-community-eecheck-master
        branch-2-community-distcheck-master
        branch-2-community-junit-U10.04-master
        "

    for JOB in $JOBS
    do
            URL="http://ci/job/${JOB/master/${BRANCH}}/build"
            ALTURL="http://ci/job/${JOB/master/${BRANCH}}/buildWithParameters?BRANCH=${BRANCH}"
            echo "Triggering: $URL"
            curl -f -X POST -u test:adhocquery $URL || wget --auth-no-challenge --http-user=test --http-password=adhocquery --post-data 'foo' $URL ||\
                curl -f -X POST -u test:adhocquery $ALTURL || wget --auth-no-challenge --http-user=test --http-password=adhocquery --post-data 'foo' $ALTURL
    done
}

startProJobs() {

    JOBS="
        branch-1-build-pro-master
    "

    for JOB in $JOBS
    do

        URL="http://${JPW}@ci/job/${JOB/master/${BRANCH}}/buildWithParameters?BRANCH=${BRANCH}&PRO_REV=${PROBRANCH}&VOLTDB_REV=${BRANCH}"
        echo "Triggering: $URL"
        #[ "$BRANCH" == "phil-jenkins-branch-testing" ] || continue
        curl -X POST $URL || wget --auth-no-challenge --post-data 'foo' $URL
    done
}


echo "repo: $GIT_REPO branch: $GIT_BRANCH"
if [ -x "$GIT_REPO" ]; then
   echo "ERROR no repo specified"
   exit 1
fi

GIT_BRANCH=${GIT_BRANCH/origin\/HEAD/origin\/master}
BRANCH=${GIT_BRANCH#origin/}

[[ "$BRANCH" =~ "notest" ]] && exit 0

# pro branch checkin requires a similarily named voltdb branch (replaces branchpro-0-pro-trigger-pro-<BRANCH>)
# voltdb branch checkin runs pro branch with voltdb master if there is no similarly named pro branch (replaces branch-0-voltdb-trigger-pro-<BRANCH>)

PROBRANCH=$BRANCH

case $GIT_REPO in

    pro)
        git ls-remote --heads git@github.com:VoltDB/voltdb.git origin "$BRANCH" | grep -q refs || exit 0
        ;;

    voltdb)
        startVoltdbJobs
        git ls-remote --heads git@github.com:VoltDB/pro.git origin "$BRANCH" | grep -q refs || PROBRANCH=master
        ;;

    *)
        echo "ERROR unknown REPO code: $GIT_REPO"
        exit 1
        ;;
esac

startProJobs
