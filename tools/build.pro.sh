#!/usr/bin/env bash
BUILD_BRANCH=$1
BUILD_TARGET=$BUILD_BRANCH/pro/obj/pro
DIST_NAME=voltdb-ent-`cat $BUILD_BRANCH/eng/version.txt`

rm -rf $BUILD_TARGET
cd $BUILD_BRANCH/eng
svn info
ant clean
cd $BUILD_BRANCH/pro
svn info
ant -f mmt.xml clean
VOLTCORE=$BUILD_BRANCH/eng ant -f mmt.xml dist.pro
cd $BUILD_TARGET/$DIST_NAME/examples/auction
ant build
cd $BUILD_TARGET/$DIST_NAME/examples/game_of_life
ant build
cd $BUILD_TARGET/$DIST_NAME/examples/helloworld
ant build
cd $BUILD_TARGET/$DIST_NAME/examples/key_value
ant build
cd $BUILD_TARGET/$DIST_NAME/examples/satellite
ant build
cd $BUILD_TARGET/$DIST_NAME/examples/twitter
ant build
cd $BUILD_TARGET/$DIST_NAME/examples/voter
ant build
cp $BUILD_BRANCH/pro/tests/mocktests/license.xml $BUILD_TARGET/$DIST_NAME/management
cd $BUILD_TARGET
rm -rf $DIST_NAME.tar.gz
tar -czf $DIST_NAME.tar.gz $DIST_NAME 
