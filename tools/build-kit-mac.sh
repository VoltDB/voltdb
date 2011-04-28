#!/bin/sh

if [ $# != 1 ]; then
    echo "usage: $0 <tag>"
    exit 1
fi

TAG=$1
if [ $1 != "trunk" ]; then
    TAG=tags/${TAG}
fi

# check that the directory is empty
ls * > /dev/null && exit 1

OS=`uname -s | tr [a-z] [A-Z] | sed 's/DARWIN/MAC/'`

rm -rf eng

svn co https://svn.voltdb.com/eng/$TAG eng

mkdir -p ~/releases/`cat eng/version.txt`
cd eng
svn status
ant clean default dist
cp obj/release/voltdb-`cat version.txt`.tar.gz ~/releases/`cat version.txt`/${OS}-voltdb-`cat version.txt`.tar.gz

cd ..
