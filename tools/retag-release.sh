#!/bin/sh

# move the release tag along trunk for the eng and pro repos.

if [ $# != 1 ]; then
    echo "usage: $0 <tag>"
    exit 1
fi

VERSIONTAG=$1

svn info https://svn.voltdb.com/eng/tags/${VERSIONTAG}
ENGHAS=$?
svn info https://svn.voltdb.com/pro/tags/${VERSIONTAG}
PROHAS=$?

if [ $ENGHAS -eq 0 ]; then
    echo "Removing old eng repo tag for ${VERSIONTAG}..."
    svn rm https://svn.voltdb.com/eng/tags/${VERSIONTAG} -m "Purging old release tag ${VERSIONTAG}"
    if [ $? -ne 0 ]; then
        echo "Error deleting tag from eng repo...aborting."
        exit 1
    fi
fi
echo "Creating new eng repo tag for ${VERSIONTAG}..."
svn cp https://svn.voltdb.com/eng/trunk https://svn.voltdb.com/eng/tags/${VERSIONTAG} -m "Create release tag ${VERSIONTAG}"
if [ $? -ne 0 ]; then
    echo "Error creating tag ${VERSIONTAG} on eng repo...aborting."
    exit 1
fi

if [ $PROHAS -eq 0 ]; then
    echo "Removing old pro repo tag for ${VERSIONTAG}..."
    svn rm https://svn.voltdb.com/pro/tags/${VERSIONTAG} -m "Purging old release tag ${VERSIONTAG}"
    if [ $? -ne 0 ]; then
        echo "Error deleting tag from pro repo...aborting."
        exit 1
    fi
fi
echo "Creating new pro repo tag for ${VERSIONTAG}..."
svn cp https://svn.voltdb.com/pro/branches/rest https://svn.voltdb.com/pro/tags/${VERSIONTAG} -m "Create release tag ${VERSIONTAG}"
if [ $? -ne 0 ]; then
    echo "Error creating tag ${VERSIONTAG} on pro repo...aborting."
    echo "WARNING: eng and pro repos are likely tagged inconsistently."
    exit 1
fi
