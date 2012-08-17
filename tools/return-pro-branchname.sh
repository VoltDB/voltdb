#!/bin/bash

#Return the pro branch for a voltdb branch (passed as $1)
# If pro branch exists of same name as passed branch, return that name
# else, return 'master'

branch=master
repo='git@github.com:VoltDB/pro.git'
tmpdir=/tmp/$USER/$RANDOM


if [ $# -gt 0 ]
then
  branch=$1
fi

#No need to ask git if master exists
if [ $branch = master ]
then
  echo $branch
  exit
fi


mkdir -p $tmpdir

# Some minimal git to find out whether a branch exists
( git init $tmpdir >> /dev/null) || exit -1
( cd $tmpdir; git remote add -t $branch origin $repo ) || exit -1
found=$(cd $tmpdir; git remote show origin | grep -c 'next fetch will store')

rm -Rf $tmpdir

if [ $found = 1 ]
then
    echo $branch
else
    echo 'master'
fi


