#!/bin/bash -e

# Cleanup previous log
rm -f ~/.svncos.build.log


./build.pro.sh $1; &> ~/.svncos.build.log


# Validate no-single-failure during the build process (or we will not replace the kit!)
if [ "`cat ~/.svncos.build.log | grep 'BUILD FAILED' | awk 'END {print NR}'`" != "0" ]; then
 echo "Build failed - see ~/.svncos.build.log"
 exit -1
fi

# Find active version to prepare tar kit file

VERSION=`cat $1/eng/version.txt`
SSH_PACK_COMMAND="rm -rf /tmp/voltdb-ent*; cp $1/pro/obj/pro/voltdb-ent-"$VERSION".tar.gz /tmp"
eval $SSH_PACK_COMMAND


# Prepare mail message for anyone interested
echo "To: volt-dev@voltdb.com
Subject: QA PRO Kit Update
Body:
----- This is an automated message - Do not reply -----
@ "$(date)"
-------------------------------------------------------
The latest PRO Kit (CentOS build) is available at:

 ${HOSTNAME}:/tmp/voltdb-ent-$VERSION.tar.gz 

SVN Revisions for this build:
 - "`cat ~/.svncos.build.log | grep 'Revision: ' | awk 'NR == 1 {print "voltdb/eng/trunk :: "$2};'`"
 - "`cat ~/.svncos.build.log | grep 'Revision: ' | awk 'NR == 2 {print "voltdb/pro/rest  :: "$2};'`"

A valid  license is  pre-installed, so you  can run VEM
out-of-the-box,  and all  sample  application  catalogs
have been built.

The following  script will  retrieve and setup this kit
on your machine at: ~/Downloads/voltdb-ent-$VERSION
Note: previously installed version will be removed.

-------------------------------------------------------

rm -rf ~/Downloads/voltdb-ent-$VERSION*
cd ~/Downloads
scp ${HOSTNAME}:/tmp/voltdb-ent-$VERSION.tar.gz .
tar -xzf voltdb-ent-$VERSION.tar.gz
rm voltdb-ent-$VERSION.tar.gz

-------------------------------------------------------
" > ~/.svncos.mail

# Send mail
# msmtp -t < ~/.svncos.mail
cat ~/.svncos.mail | mail -s "QA PRO Kit Update" volt-dev@voltdb.com 

# Cleanup
# rm ~/.svncos.mail

