#!/bin/bash

echo "# This file contains a record of execution times for computing exact" > bench_perf.dat
echo "# and estimated cardinality on tables of varying sizes.  Time units" >> bench_perf.dat
echo "# are in millisenconds." >> bench_perf.dat
echo "#rows        count(distinct ...)    approx_count_distinct(...)" >> bench_perf.dat

echo "# This file contains a record of exact and estimated cardinality for" > bench_accuracy.dat
echo "# tables of varying sizes.  The fourth column is the percentage that" >> bench_accuracy.dat
echo "# the estimate was off by."  >> bench_accuracy.dat
echo "# (0.05 means that the estimate was off by 0.05%.)"  >> bench_accuracy.dat
echo "#rows        exact cardinality    approx cardinality    percent change" >> bench_accuracy.dat

./run.sh 32000 16000
./run.sh 64000 32000
./run.sh 128000 64000
./run.sh 256000 128000
./run.sh 512000 256000
./run.sh 1024000 512000
#./run.sh 2048000 1024000

gnuplot=`which gnuplot`
if [ "$gnuplot" != "" ]; then
    gnuplot bench_perf.gpl
    gnuplot bench_accuracy.gpl
fi

echo
echo "### bench_perf.dat ###"
cat bench_perf.dat
echo

echo "### bench_accuracy.dat ###"
cat bench_accuracy.dat
echo
