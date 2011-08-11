#!/bin/sh

if [ $# != 1 ]; then
    echo "usage: $0 <tag>"
    exit 1
fi

# create a place to build in /tmp
BUILDDIR=/tmp/buildtemp

if [ -d "$BUILDDIR" ]; then
    echo "Purging $BUILDDIR..."
    rm -rf "$BUILDDIR"
fi

echo "Creating $BUILDDIR..."
mkdir "$BUILDDIR"

PARAM1=$1
if [ $1 != "trunk" ]; then
    ENGTAG=tags/${PARAM1}
    PROTAG=tags/${PARAM1}
else
    ENGTAG=trunk
    PROTAG=branches/rest
fi

# check that the directory is empty
cd "$BUILDDIR"
ls * > /dev/null && exit 1

OS=`uname -s | tr [a-z] [A-Z] | sed 's/DARWIN/MAC/'`

rm -rf eng pro

svn co https://svn.voltdb.com/eng/$ENGTAG eng
svn co https://svn.voltdb.com/pro/$PROTAG pro

# Figure out the release directory.  If it exists, make a backup copy before proceeding
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
echo "Copying Mac kit (if any)..."
scp test@voltmini:~/releases/`cat version.txt`/MAC-voltdb-`cat version.txt`.tar.gz "$RELEASEDIR"
if [ ! -f "$RELEASEDIR"/MAC-voltdb-`cat version.txt`.tar.gz ]; then
    echo "Unable to copy Mac VoltDB kit.  Aborting..."
    exit 1
fi

svn status
# Build the community version
ant clean default dist voltbin
cp obj/release/voltdb-`cat version.txt`.tar.gz ~/releases/`cat version.txt`/${OS}-voltdb-`cat version.txt`.tar.gz
cp obj/release/voltdb-client-java-`cat version.txt`.tar.gz ~/releases/`cat version.txt`/voltdb-client-java-`cat version.txt`.tar.gz
# Now build the enterprise version
cd ../pro
svn status
VOLTCORE=../eng ant -f mmt.xml clean dist.pro
cp obj/pro/voltdb-ent-*.tar.gz ~/releases/`cat ../eng/version.txt`/
cd ../eng
echo "CRC checksums:" > ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
cksum ~/releases/`cat version.txt`/*.gz >> ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
echo "MD5 checksums:" >> ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
md5sum ~/releases/`cat version.txt`/*.gz >> ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
echo "SHA1 checksums:" >> ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
sha1sum ~/releases/`cat version.txt`/*.gz >> ~/releases/`cat version.txt`/checksums.txt

#scp -r ~/releases/`cat version.txt` root@community.voltdb.com:/var/www/drupal/sites/default/files/archive

mkdir -p ~/releases/`cat version.txt`/other
cp obj/release/voltdb-`cat version.txt`.sym ~/releases/`cat version.txt`/other/
cd ..

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
