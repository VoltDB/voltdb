## NP Transaction Performance Benchmark
This is a benchmark for measuring the performance of N-partition transactions. The scenario is based on credit card transactions with 3 kinds of procedures:
- 2P inter-person transaction
- SP transaction to change the balance in a single account
- MP transaction to select a range of accounts

The primary purpose of using 2 other types of procedures is to test how a 2P/NP transaction framework could reduce global locking time in order to increase throughput. The ratio of the types of transactions can be provided before benchmarking. We can add more coverage on NP transactions in the future.

### Instructions
- Make sure the **voltdb/bin** is in your PATH
- In one terminal, run `./run.sh`
- In the 2nd terminal, run `./run.sh client` to start the benchmark
- Run `./run.sh cleanUp` to remove the generated files

### Parameters
- `--sprate` The ratio of SP txns compared to 2P txns. Must be between 0 and 1. (default 0.6)
- `--mprate` The ratio of MP txns compared 2P txns. Must be between 0 and 1. (default 0.02)
- `--cardcount` The total number of credit card accounts. (default 500000)
- `--skew` The data access skew ratio, the larger the skew value, the more constrained the data access range will be. (default 0.0, which means the data access pattern is evenly distributed)
- `--duration` The duration of the benchmark in secs. (default 30)

### Reference
- See <https://issues.voltdb.com/browse/ENG-12741> for the ticket description.
- The original NP txn framework: <https://github.com/VoltDB/voltdb/pull/3822/>