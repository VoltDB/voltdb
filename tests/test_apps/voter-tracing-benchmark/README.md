--------------------------------------------------------------------------------
Brief Introduction
--------------------------------------------------------------------------------
To measure the performance impact of the VoltDB tracing system, a case study example is 
constructed based on the schema of Voter application (voltdb/examples/voter). 
This example is simple. We choose two stored procedures, one is a "INSERT" statement 
of the table VOTES, and the other is a "DistinctCount" stored procedure that 
counts the number of different states in the table AREA_CODE_STATE. For either of the 
stored procedures, we run it repeatedly for a large number of times and the time 
performance of each execution is recorded.


--------------------------------------------------------------------------------
Usage
--------------------------------------------------------------------------------
./run.sh tracing-benchmark-showAll          (run the test and print the statistical results of performance on console)
./run.sh tracing-benchmark-showBenchmark    (run the test and print the statistical results of performance with tracing system turned on and turned off, respectively)
./run.sh tracing-benchmark-figurePlot       (run the test and generate the figures to visualize the performance impact of the tracing system)

--------------------------------------------------------------------------------
Notes
--------------------------------------------------------------------------------
(1) As mentioned before, there are two stored procedures "VOTES.INSERT" and "DistinctCount". 
In the example, the default stored procedure is "VOTES.INSERT". The user can choose 
either of them by setting the parameters in the function tracing-benchmark() in run.sh.

To choose "VOTES.INSERT", set "-–doInsert=true"
To choose "DistinctCount", set "-–doInsert=false"

(2) To choose which tracing subsystems (such as CI, SPI, EE, MPI etc.) to enable, 
the user needs to set the related commands in function tracing-benchmark-figurePlot() 
and/or function tracing-benchmark-showAll () in the bash script run.sh.

For example, to turned on the CI subsystem before test, one need to do like this:

sqlcmd --query="exec @Trace enable CI" > /dev/null
#run the tests
sqlcmd --query="exec @Trace disable CI" > /dev/null

(3) The default number of iterations we run the benchmark tests repeatedly is 50.
The users may reset this value by modifying the variable NUM_ITER in function
tracing-benchmark-showBenchmark() and/or function tracing-benchmark-figurePlot() in
the bash script run.sh.

(4) The python file tracingBenchmarkPlot.py is used for plotting the benchmark data 
in figures. It is automatically called in the function tracing-benchmark-figurePlot 
in run.sh. It requires python numpy and matplotlib be installed in advance.