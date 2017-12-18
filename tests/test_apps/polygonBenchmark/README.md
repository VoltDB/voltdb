# Polygon Insertion Benchmark Application

This application inserts many polygons, and times the polygon
creation rate.  It started as Voter, so it has some similarity with that.

In particular, we do checks of polygonfromtext and validpolygonfromtext for
correctly and incorrectly oriented polygons.  For each of polygonfromtext
and validpolygonfromtext we insert polygons for 30
seconds and record the throughput
in transactions per second. Some of the polygons are valid, and some have
been damaged so that their outer ring is clockwise.  These polygons have
no holes.  The benchmark varies the fraction of polygons needing repair
between 0.0 and 1.0 in steps of 0.1.

 We also test inserting polygons in each of four ways.

1. We insert using a stored procedure which expects a WKT string.
1. We insert using a stored procedure which expects a GeographyValue object.
1. We insert using the built in CRUD procedure, handing it a WKT string.
1. We insert using the build in CRUD procedure, handing it a GeographyValue.

Note that the WKT string cases will use polygonfromtext, so they
will check for validity.  The object cases do not do any checking.

To run this sensibly, you need to have a way to build two different
versions of VoltDB, so that you can compare them.  We assume here that
we can just run voltdb and sqlcmd, and that the PATH variable is set up
so that everything just works.

The output will be a log file named "log.out" and a CSV file named
polygonBenchmark.csv.  The contents of these files should be self
explanatory.


# How to run this
Just run:

    voltdb init --force
    voltdb start

Wait until you see "Server completed initialization."

Open a new shell in the directory with the benchmark,  and run the script:

    ./runbenchmark.sh

This shell script will run the client application checked and unchecked for all the
values of the repair fraction, and will write its output to log files.  There will
also be a file output.csv with the timings.

# The results of the benchmark.
As you can see from the spreadsheet, checking for polygon validity
takes about twice as long.  Unsurprisingly, it's worse with larger
polygons, though we don't see that here.

We can also see, by comparing StoredProcedureWKT and StoredProcedureObject,
that polygonfromtext runs about 1/6 the speed of passing a polygon 
object.
