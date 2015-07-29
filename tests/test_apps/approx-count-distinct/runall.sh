#!/bin/bash

rm bench_perf.dat
rm bench_accuracy.dat

echo "#rows/uniqe_vals  time exact (ms)    time approx (ms)" > bench_perf.dat
echo "#rows/uniqe_vals  exact cardinality    approx cardinality" > bench_accuracy.dat

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
