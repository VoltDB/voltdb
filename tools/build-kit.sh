#!/bin/sh

if [ $# != 1 ]; then
    echo "usage: $0 <tag>"
    exit 1
fi

PARAM1=$1
if [ $1 != "trunk" ]; then
    ENGTAG=tags/${PARAM1}
    PROTAG=tags/${PARAM1}
else
    ENGTAG=trunk
    PROTAG=branches/rest
fi

# check that the directory is empty
ls * > /dev/null && exit 1

OS=`uname -s | tr [a-z] [A-Z] | sed 's/DARWIN/MAC/'`

rm -rf eng pro

svn co https://svn.voltdb.com/eng/$ENGTAG eng
svn co https://svn.voltdb.com/pro/$PROTAG pro

mkdir -p ~/releases/`cat eng/version.txt`
cd eng
svn status
ant clean default dist voltbin
cp obj/release/voltdb-`cat version.txt`.tar.gz ~/releases/`cat version.txt`/${OS}-voltdb-`cat version.txt`.tar.gz
cd ../pro
svn status
ant -f mmt.xml clean
VOLTCORE=../eng ant -f mmt.xml dist.pro
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
