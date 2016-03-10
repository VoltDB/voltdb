README for client package
=========================

This java client source is based on the VoltDB example application "voter", specifically the AsyncBenchmark, but refactored to separate boilerplate from application-specific code.

BaseBenchmark.java: 
  - boilerplate
  - should not need to be modified
  - establishes connection, collects statistics, prints periodic stats, drives the main loops of the benchmark
  
BenchmarkCallback.java
  - a general-purpose callback class for tracking the results of asynchronous stored-procedure calls.  
  - Keeps a thread-safe count of invocations, commits, and rollbacks
  - provides a summary report
  
BenchmarkConfig.java
  - defines the commmand-line arguments for the benchmark
  
PositionsBenchmark.java
  - extends BaseBenchmark.java
  - uses command-line arguments from BenchmarkConfig.java
  - Provides the implementation for application-specific actions:
     initialize() - executed once, pre-populates lookup tables and the working set of positions
     iterate() - executed at a controlled rate throughout the duration of the benchmark, generates and executes semi-random transactions
     printResults() - customized to list the results of the particular stored procedures involved.

If you were to use this applicatoin as a template, you should only need to copy and modify PositionsBenchmark and possibly add options to BenchmarkConfig.
