#!/bin/sh

if [ $# != 1 ]; then
    echo "usage: $0 <tag>"
    exit 1
fi

BUILDDIR=/tmp/buildtemp

if [ -d "$BUILDDIR" ]; then
    echo "Purging $BUILDDIR..."
    rm -rf "$BUILDDIR"
fi

echo "Creating $BUILDDIR..."
mkdir "$BUILDDIR"

TAG=$1
if [ $1 != "trunk" ]; then
    TAG=tags/${TAG}
fi

# check that the directory is empty
cd "$BUILDDIR"
ls * > /dev/null && exit 1

OS=`uname -s | tr [a-z] [A-Z] | sed 's/DARWIN/MAC/'`

rm -rf eng

svn co https://svn.voltdb.com/eng/$TAG eng

RELEASEDIR=~/releases/`cat eng/version.txt`
echo "Release directory is: $RELEASEDIR..."
RELEASEDIRBAK="$RELEASEDIR".prev

if [ -d "$RELEASEDIRBAK" ]; then
    echo "Purging backup release dir $RELEASEDIRBAK..."
    rm -rf "$RELEASEDIRBAK"
fi

if [ -d "$RELEASEDIR" ]; then
    echo "Backing up release dir $RELEASEDIR to $RELEASEDIRBAK..."
    mv "$RELEASEDIR" "$RELEASEDIRBAK"
fi

echo "Creating release directory $RELEASEDIR..."
mkdir -p "$RELEASEDIR"
cd eng
svn status
ant clean default dist
cp obj/release/voltdb-`cat version.txt`.tar.gz ~/releases/`cat version.txt`/${OS}-voltdb-`cat version.txt`.tar.gz

#create candidate symlinks for Hudson jobs
CANDDIR=~/releases/candidate
if [ -d "$CANDDIR" ]; then
    if [ -L "$CANDDIR" ]; then
        rm "$CANDDIR"
    else
        #someone created a dir with the name we want, bad
        rm -rf "$CANDDIR"
    fi
fi
cd ~/releases
ln -s "$RELEASEDIR" candidate
