#!/bin/bash
# This is a script used to run all necessary workloads listed in the microbench.xml file.
cd ..
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectInts
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectStrings
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectLongs
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectBytes
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectDoubles
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectDecimals
ant benchmarklocal -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=SelectTimestamps
