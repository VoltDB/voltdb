#!/bin/sh

cd ~/trunk
svn info
LD_PRELOAD=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre/lib/amd64/libjsig.so ant compile ee emma-report
EXIT=$?
cp    $HOME/trunk/obj/release/emma/coverage.html $HOME/.hudson/userContent/sidebar_emma/ || true
cp -r $HOME/trunk/obj/release/emma/_files $HOME/.hudson/userContent/sidebar_emma/ || true
exit $EXIT
