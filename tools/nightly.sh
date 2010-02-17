#!/bin/bash

# clean up properties so that if this build fails the plot tool won't pick them up
# not cleaned up at end because plot tool runs after this script, not concurrently
rm *.properties

# distribute jars
pushd trunk
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
trunk/tools/parsenightlylog.py < benchmark-log > performance.properties
