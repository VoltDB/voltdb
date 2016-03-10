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
  
NbboBenchmark.java
  - extends BaseBenchmark.java
  - uses command-line arguments from BenchmarkConfig.java
  - Provides the implementation for application-specific actions:
     initialize() - executed once
     iterate() - executed at a controlled rate repeatedly

CsvLineParser.java
  - simple CSV line parser, used to load stock symbols

Symbols.java
  - handles loading stock symbols used for synthetic data generation

If you were to use this applicatoin as a template, you should only need to copy and modify NbboBenchmark and possibly add additional command-line parameters to BenchmarkConfig.
