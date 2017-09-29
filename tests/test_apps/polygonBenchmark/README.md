# Polygon Insertion Benchmark Application

This application inserts many polygons, and times the polygon
creation rate.  It started as Voter, so it has some similarity with that.

In particular, we insert polygons for 30 seconds and record the throughput
in transactions per second. Some of the polygons are valid, and some have
been damaged so that their outer ring is clockwise.  These polygons have
no holes.  The benchmark varies the fraction of polygons needing repair
between 0.0 and 1.0 in steps of 0.1.  We assume we have a REGIME, which
is either checked or unchecked, though it could be any name you want.
This is just logged.  We also test inserting polygons in each of four
ways.

1. We insert using a stored procedure which expects a WKT string.
1. We insert using a stored procedure which expects a GeographyValue object.
1. We insert using the built in CRUD procedure, handing it a WKT string.
1. We insert using the build in CRUD procedure, handing it a GeographyValue.

Note that the WKT string cases will use polygonfrom text, so they
will check for validity.  The object cases do not do any checking.

To run this sensibly, you need to have a way to build two different
versions of VoltDB, so that you can compare them.  We assume here that
we can just run voltdb and sqlcmd, and that the PATH variable is set up
so that everything just works.

The output will be a set of files with names REGIME_NNN.txt, where REGIME
is the name set in runbenchmark.sh, and NNN is the repair fraction times 100.
So, if the repair fraction is 0.4, NNN will be 040.  For repair fraction
equal to 1.0 and 0.0 NNN will be 100 and 000 respectively.

# How to run this
Choose the REGIME you will be using, and manipulate your path so that you
are running the version of VoltDB you want.  Then run:

    voltdb init --force
    voltdb start

Wait until you see "Server completed initialization."

Open a new shell in the directory with the benchmark, where this file is
found, and edit the file ./runbenchmark.sh to set the values of REGIME
and NUM_VERTICES.  Then run the script:

    ./runbenchmark.sh

This shell script will run the client application for all the
values of the repair fraction, and will write its output to log files.
You will have to do some fiddling to extract the data from the
log files.  Your friends are grep, sed and sort here, and maybe
emacs keyboard macros.

The contents of the data directory contains the output of runs of the
benchmark taken 2017-09-29.  The file averages.ods is an OpenOffice
spreadsheet with some analysis of these results.

# The results of the benchmark.
As you can see from the spreadsheet, checking for polygon validity
takes about twice as long.  Unsurprisingly, it's worse with larger
polygons, though we don't see that here.

We can also see, by comparing StoredProcedureWKT and StoredProcedureObject,
that polygonfromtext runs about 1/6 the speed of passing a polygon 
object.
