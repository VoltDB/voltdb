#!/bin/bash

CURRENT=$1
SINCE=$2


if [ "$CURRENT" = "master" ]
then
   CURRENT="HEAD"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../..

echo "====================================================="
echo "VOLTDB REPOSITORY"
pwd
echo

echo; echo "---------- Checking lib and third party directories-------------"
git diff --name-status  $SINCE..$CURRENT -- lib/ third_party/ | \
    egrep -v 'tests/mocktests|voltcli|vdm/tests' || \
    echo "No differences in checked in lib and third_party files"

echo; echo "---------- Checking db monitor source-------------"
git diff --name-status $SINCE..$CURRENT -- src/frontend/org/voltdb/dbmonitor/js/ \
    src/frontend/org/voltdb/dbmonitor/css/ | \
    egrep -v 'js/voltdb|template.js|js/queryui|resources/images' || \
    echo "No differences in dbmonitor javascript files"


echo; echo "---------- Diffing README.thirdparty* -------------"
git diff -w $SINCE..$CURRENT -- README.thirdparty* || echo "No differences in README.thirdparty"

echo; echo "---------- Diffing importer bundle included jar files  -------------"
git diff -w $SINCE..$CURRENT --  build-importers.xml   | egrep 'zip.*file' || \
    echo "No differences in jar files used in osgi bundles"


cd ../pro

echo "====================================================="
echo "PRO REPOSITORY"
pwd
echo

echo; echo "---------- Checking lib and third party directories-------------"
git diff --name-status  $SINCE..$CURRENT -- lib/ third_party/ | \
    egrep -v 'tests/mocktests|voltcli|vdm/tests' || \
    echo "No differences in checked in lib and third_party files"

echo; echo "---------- Diffing README.thirdparty* -------------"
git diff -w $SINCE..$CURRENT -- README.thirdparty* || echo "No differences in README.thirdparty"
