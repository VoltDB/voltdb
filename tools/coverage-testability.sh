#!/bin/sh

cd ~/trunk
svn info
LD_PRELOAD=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre/lib/amd64/libjsig.so ant clean testability-report
EXIT=$?
cp    $HOME/trunk/obj/release/testability.result.html $HOME/.hudson/userContent/sidebar_testability/ || true
exit $EXIT
