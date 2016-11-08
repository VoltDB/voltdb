Automated Tests of the VMC (VoltDB Management Center), Using GEB
================================================================

This file describes the GEB automated tests of the VMC (VoltDB Management
Center), which is the new web UI, replacing the old Web Studio. These automated
tests are believed to provide more than the level of coverage of the old
GEB tests of Web Studio, in the, now deleted, ../studioWeb/ directory.

To run these tests of the VMC:

1. Download and install OR build from source VoltDB, either the community
or pro version (the 'genqa' test app requires pro, if you want to use that).

2. Launch a (backgrounded) VoltDB server. The easiest way to do that, for VMC
testing purposes, is from the voltdb/tests/geb/vmc/server/ directory, via this
command:
<pre>
    ./run_voltdb_server.sh -p
</pre>
The '-p' argument is used to start the pro version of VoltDB; the default is
the community version, so if you are running that, leave out this arg.
You may also specify '-g' to run against the 'genqa' test app (which used to
be the preferred server to use), or '-v' to run against the 'voter' example
app.
Or, you may start up and run against some other VoltDB server, if you prefer.

3. From the voltdb/tests/geb/vmc/ directory, launch the all-in-one install/run
script for the automated tests:
<pre>
    ./run_vmc_tests.sh debug
</pre>
The 'debug' argument is optional; if specified, you will get more information
in the results summary (HTML) files. This script simply runs the following
Gradle command (which you may run directly, if you prefer), after putting the
arguments in the right order, and providing defaults (and expanding the 'debug'
arg); since no browser was specified, the default value of 'firefox' is used:
<pre>
    ./gradlew -PdebugPrint=true firefox --rerun-tasks
</pre>
(Using 'firefox' or 'firefoxTest' here is equivalent.)

4. Scan the console output for highlighted FAILED messages or failed test
counts and/or browse the test result summary rooted in:  <br>
    voltdb/tests/geb/vmc/build/reports/firefoxTest/tests/index.html  <br>
or, if you used PhantomJS / Ghost Driver:  <br>
    voltdb/tests/geb/vmc/build/reports/phantomjsTest/tests/index.html  <br>
or, if you used Chrome:  <br>
    voltdb/tests/geb/vmc/build/reports/chromeTest/tests/index.html  <br>
or, if you used Internet Explorer (IE):  <br>
    voltdb/tests/geb/vmc/build/reports/ieTest/tests/index.html  <br>
or, if you used Safari:  <br>
    voltdb/tests/geb/vmc/build/reports/safariTest/tests/index.html  <br>

5. Stop the backgrounded server ("voltadmin shutdown" or "kill %1" - or 
"kill <whatever your actual background job number(s) may be>").

To add to or modify the existing tests:

1. To add additional SQL queries to be run in the VMC, add additional lines to
the file voltdb/tests/geb/vmc/src/resources/sqlQueries.txt; the format is
JSON-based, and the specific format should be fairly self-explanatory, by
looking at existing tests. Make sure the new tests clean up after themselves,
and can be run twice in a row; for example, any new tables that are Created
should also be Dropped.
2. More substantial changes to the tests can be made in the
voltdb/tests/geb/vmc/src/pages/ and voltdb/tests/geb/vmc/src/tests/
directories, which contain most of the actual Groovy / GEB code.
3. For more info on GEB, see:  <br>
    http://www.gebish.org  <br>
    http://www.gebish.org/manual/current/  <br>
(especially the latter).
For more info on Spock (which is also used), see:  <br>
    http://spockframework.github.io/spock/docs/  <br>
    https://code.google.com/p/spock/wiki/SpockBasics  <br>
(especially the latter, which explains the use of 'setup', 'cleanup', 'when:',
'then:', 'and:', 'expect:' and 'where:', etc.).

Periodically, it is useful to update the (default) version of Selenium being
used by these tests, in order to support the latest version of Firefox, or
other browsers.
(This used to be necessary frequently, when running on the Firefox browser,
because each version of Selenium supported only a narrow group of Firefox
versions; however, since support for native events was removed in Selenium
2.45.0, and in Firefox versions around the same time, this is less of an
issue.)
To do this:

1. Check the most recent version of Selenium (with Java) in:  <br>
    http://www.seleniumhq.org/download/  <br>
Also, you may still wish to confirm which version(s) of Firefox it supports, in: <br>
    https://raw.githubusercontent.com/SeleniumHQ/selenium/master/java/CHANGELOG  <br>
2. In the file voltdb/tests/geb/vmc/build.gradle, change the line:
<pre>
    def seleniumVersion = getProjectPropertyOrDefaultValue("seleniumVersion", "2.45.0")
</pre>
to use the latest version (e.g. "2.47.1").
Also, see Note 7 below, about how to change which version of Selenium (and
other things) is used for a particular run.

Notes:

1. If you want to run these tests "headless", without launching a browser,
so no GUI is needed (which is particularly useful on a Linux system without
X11), using PhantomJS / Ghost Driver:
<pre>
    ./run_vmc_tests.sh debug phantomjs
</pre>
(you may use 'phantomjs' or 'phantomjsTest', and 'debug' is optional), then
you will first need to download PhantomJS, as described here:
    http://phantomjs.org/download.html
(and make sure its bin directory is included in the system PATH).

2. If you want to run these tests on Chrome, using:
<pre>
    ./run_vmc_tests.sh debug chrome
</pre>
(you may use 'chrome' or 'chromeTest', and 'debug' is optional), then you will
first need to download the Chrome Driver, as described in:  <br>
    https://github.com/SeleniumHQ/selenium/wiki/ChromeDriver  <br>
(mainly, make sure it's in a directory included in the system PATH).

3. Similarly, if you want to run these tests on Safari, on a Mac, using:
<pre>
    ./run_vmc_tests.sh debug safari
</pre>
(you may use 'safari' or 'safariTest', and 'debug' is optional), then you will
first need to follow the instructions here:  <br>
    https://github.com/SeleniumHQ/selenium/wiki/SafariDriver  <br>
about opening the latest version of SafariDriver.safariextz, and clicking the
"install" button.
(This will likely not work as well as running on Firefox, Chrome, or "headless"
with PhantomJS / Ghost Driver.)

4. Similarly, if you want to run these tests on Internet Explorer (IE), on a
Windows system, using (note that the run_vmc_tests.sh script will not work on
Windows; 'gradlew' here refers to gradlew.bat):
<pre>
    gradlew -PdebugPrint=true ie --rerun-tasks
</pre>
(you may use 'ie' or 'ieTest', and '-PdebugPrint=true' is optional), then you
will first need to download the IE driver, as described here (under 'The
Internet Explorer Driver Server'):  <br>
    http://docs.seleniumhq.org/download/  <br>
but also be aware of this recent issue:  <br>
    https://groups.google.com/forum/m/#!topic/selenium-users/TdY_rRNF-gw  <br>
and you may want to turn off IE's auto-correct (spell checking).
(This will likely not work as well as running on Firefox, Chrome, or "headless"
with PhantomJS / Ghost Driver.)

5. If you want to run just one test class or method, you may do so using
the --tests argument. For example, to run all of the tests in the
NavigatePagesTest class (on Firefox, with the optional 'debug' argument), run:
<pre>
    ./run_vmc_tests.sh debug --tests=*NavigatePages*
</pre>
Or, to run just the checkTables method (in the SqlQueriesTest class), run:
<pre>
    ./run_vmc_tests.sh debug --tests=*checkTables
</pre>

6. Two test classes, SqlQueriesTest and FullDdlSqlTest, mostly run tests that
are defined, including their names, in other (JSON, SQL, or Java) files, so the
above technique will not work well for these.
However, you may narrow down these further using the '-PsqlTests=...' argument.
For instance, you can run only the first two tests defined in 
voltdb/tests/geb/vmc/src/resources/sqlQueries.txt, as follows:
<pre>
    ./run_vmc_tests.sh debug --tests=*sqlQueries* -PsqlTests=InsertBigInt,InsertTinyInt
</pre>
Or, you can run only the (related) first tests in both
voltdb/tests/frontend/org/voltdb/fullddlfeatures/fullDDL.sql and
voltdb/tests/frontend/org/voltdb/fullddlfeatures/TestDDLFeatures.java, as follows:
<pre>
    ./run_vmc_tests.sh debug --tests=*FullDdlSql* -PsqlTests=CREATE_TABLE_T1,testCreateUniqueIndex
</pre>

7. There are several other system properties that can be specified on the
command-line using '-P' arguments, as follows:
<pre>
    ./run_vmc_tests.sh -Purl=http://my.server.com:8080/ -PdebugPrint=true -PtimeoutSeconds=30
</pre>
Here is a description of all system properties currently available:
<pre>
    NAME             DEFAULT  DESCRIPTION
    debugPrint       false    If true, debug output is produced (in the test result HTML pages)
    timeoutSeconds   20       How long to wait for HTML elements to appear, before giving up
    url              http://localhost:8080/  The URL for the VMC to be tested
    windowWidth      1500     The width of the browser window
    windowHeight     1000     The height of the browser window
    gebVersion       0.12.2   The version of GEB to use
    spockVersion     0.7-groovy-2.0  The version of Spock to use
    seleniumVersion  2.47.1   The version of Selenium to use
    phantomjsVersion 1.2.1    The version of PhantomJS Ghost Driver to use (if any)
</pre>
These are used only in the SqlQueriesTest class (and the FullDdlSqlTest class,
for the last two), mainly in the insertQueryCountAndDeleteForTablesAndViews
test method:
<pre>
    numRowsToInsert  3        How many rows to insert, in each Table
    testTables       PARTITIONED_TABLE,REPLICATED_TABLE  Which Tables to test (or ALL)
    testViews        null     Which Views (if any) to test (or ALL)
    insertJson       false    If true, VARCHAR values will be inserted as JSON data
    sleepSeconds     0        Can slow down the tests, to watch what they are doing
    sqlTests         null     Which tests, specified in a file, to run (null means all)
</pre>
If you are running the default server (for the GEB tests of the VMC), or the
'genqa' test app, then PARTITIONED_TABLE and REPLICATED_TABLE should already be
defined; but if not, they will be created by the SqlQueriesTest class, and then
dropped at the end. The system properties that are available are defined in: <br>
    [voltdb/tests/geb/vmc/build.gradle](/tests/geb/vmc/build.gradle) (all except one)  <br>
    [voltdb/tests/geb/vmc/src/resources/GebConfig.groovy](/tests/geb/vmc/src/resources/GebConfig.groovy) (timeoutSeconds only)  <br>
So for more info, see there, or the code (especially SqlQueriesTest.java and
FullDdlSqlTest.java).

8. If you want to run these tests regularly on your machine, you may want
to set your Firefox Preferences (under Advanced, Update) to something other
than "Automatically install updates" ("Check for updates, but let me choose
whether to install them" is a good choice), so that your version of Firefox
does not get ahead of what Selenium can handle.
(As mentioned above, this may be less of an issue now that native events are
no longer supported.)

9. Running the tests "headless" with HtmlUnit does not currently work; however,
you can run "headless" (without a browser), using PhantomJS / Ghost Driver, as
described above.
The complete list of browser drivers is specified in:  <br>
    [voltdb/tests/geb/vmc/src/resources/GebConfig.groovy](/tests/geb/vmc/src/resources/GebConfig.groovy)  <br>
See also:  <br>
    [voltdb/tests/geb/vmc/build.gradle](/tests/geb/vmc/build.gradle)
