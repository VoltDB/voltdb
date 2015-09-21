This test app compares performance and accuracy of the SQL aggregate
function `APPROX_COUNT_DISTINCT(col)` as compared with with
`COUNT(DISTINCT col)`.  Typically the former will be faster and use
less memory (especially in multi-partition queries where an exact
answer requires the coordinator to do more work).  The approximate
answer is usually within 1% of the exact answer, if the number of
unique values is in the thousands or greater.

In cases where computing an exact distinct cardinality would be
prohibitive due to long latencies or hitting the temp table memory
limit, APPROX_COUNT_DISTINCT is therefore a good alternative.

To run this test, execute this script:
```bash
./runall.sh
```

The test will produce `bench_perf.dat` and `bench_accuracy.dat` files
with latency and accuracy statistics for tables with varying numbers
of rows and varying numbers of distinct values.

On machines that have gnuplot installed, .png files containing a
visualization of the results will be produced.
