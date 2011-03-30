#!/bin/sh

cd ~/trunk
svn info
LD_PRELOAD=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre/lib/amd64/libjsig.so ant clean sqlcoverage -DtimeoutLength=3600000
EXIT=$?
cp -r $HOME/trunk/obj/release/sqlcoverage/* $HOME/.hudson/userContent/sidebar_sqlcoverage/ || true
exit $EXIT
