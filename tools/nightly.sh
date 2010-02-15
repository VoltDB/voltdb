#!/bin/bash

# clean up properties so that if this build fails the plot tool won't pick them up
# not cleaned up at end because plot tool runs after this script, not concurrently
rm *.properties

# build and distribute jars
pushd newtrunk
ant clean dist_internal voltbin
scp -r obj/release/voltbin volt1:

# run benchmarks
~/tools/check-cluster.sh && exit 1
# cd newtrunk && ant benchmarkcluster
# cd ~/voltbin && ./matrixbenchmark.py
cd obj/release/voltbin
~/voltbin/matrixbenchmark.py | tee ../../../../benchmark-log # in workspace root
~/tools/cleanup-cluster.sh

# analysis
popd # back to workspace root
newtrunk/tools/parsenightlylog.py < benchmark-log > performance.properties

# try to conserve disk space
cd newtrunk
ant clean
