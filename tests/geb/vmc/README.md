Automated Tests of the VMC (VoltDB Management Center), Using GEB
----------------------------------------------------------------

This file describes the GEB automated tests of the VMC (VoltDB Management
Center), which is the new UI, replacing the old Web Studio. These automated
tests are believed to provide more than the level of coverage of the old
GEB tests of Web Studio, in the, now deleted, ../studioWeb/ directory.

To run these tests of the VMC:
1. Download and install OR build from source VoltDB, preferably the pro
   version (since 'genqa' requires pro).
2. From the voltdb/tests/test_apps/genqa/ directory, launch a backgrounded
   genqa application server:
      ./run.sh server &
   (You can also run against other examples, such as the 'voter' example,
   though the testing is slightly less complete.)
3. From the voltdb/tests/geb/vmc/ directory, launch the all-in-one install/run
   script for the automated tests:
      ./gradlew firefox --rerun-tasks
   (You may use 'firefox' or 'firefoxTest'.)
4. Scan the console output for highlighted FAILED messages or failed test
   counts and/or browse the test result summary rooted in:
      voltdb/tests/geb/vmc/build/reports/firefoxTest/tests/index.html
   or, if you used Chrome:
      voltdb/tests/geb/vmc/build/reports/chromeTest/tests/index.html
   or, if you used Internet Explorer (IE):
      voltdb/tests/geb/vmc/build/reports/ieTest/tests/index.html
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
3. For more info on GEB, see:
      http://www.gebish.org/
      http://www.gebish.org/manual/current/
   For more info on Spock (which is also used), see:
      http://docs.spockframework.org/en/latest/
      https://code.google.com/p/spock/wiki/SpockBasics

Periodically, it is necessary to update the version of Selenium being used
by these tests, in order to support the latest version of Firefox (or other
browsers). To do this:
1. Check the most recent version of Selenium (with Java) here:
      http://www.seleniumhq.org/download/
   Also, you may wish to confirm which version(s) of Firefox it supports here:
      http://selenium.googlecode.com/git/java/CHANGELOG
2. In the file voltdb/tests/geb/vmc/build.gradle, change the line:
      def seleniumVersion = "2.44.0"
   to use the latest version (e.g. "2.45.2").

Note 1: If you want to run these tests on Chrome, using:
      ./gradlew chrome --rerun-tasks
   (you may use 'chrome' or 'chromeTest'), then you will first need to download
   the Chrome Driver, as described here:
      https://code.google.com/p/selenium/wiki/ChromeDriver
   (mainly, make sure it's in a directory included in the system PATH).

Note 2: Similarly, if you want to run these tests on Internet Explorer (IE), on a
   Windows system, using ('gradlew' here refers to gradlew.bat):
      gradlew ie --rerun-tasks
   (you may use 'ie' or 'ieTest'), then you will first need to download the IE
   driver, as described here (under 'The Internet Explorer Driver Server'):
      http://docs.seleniumhq.org/download/
   but also be aware of this recent issue:
      https://groups.google.com/forum/m/#!topic/selenium-users/TdY_rRNF-gw
   and you may want to turn off IE's auto-correct (spell checking).

Note 3: If you want to run just one test class or method, you may do so using
   the --tests argument. For example, to run all of the tests in the
   NavigatePagesTest class (on Firefox), run:
      ./gradlew firefox --tests=*NavigatePages* --rerun-tasks
   Or, to run just the checkTables method (in the SqlQueriesTest class), run:
      ./gradlew firefox --tests=*checkTables --rerun-tasks
   You can also run all of the tests defined in
   voltdb/tests/geb/vmc/src/resources/sqlQueries.txt, as follows:
      ./gradlew firefox --tests=*sqlQueries* --rerun-tasks

Note 4: There are several system properties that can be specified on the
   command-line using '-P', as follows:
      ./gradlew -Purl=http://my.server.com:8080/ -PdebugPrint=true -PtimeoutSeconds=10 firefox --rerun-tasks
   Here is a description of all system properties currently available:
      NAME            DEFAULT  DESCRIPTION
      url             http://localhost:8080/  The URL for the VMC to be tested
      debugPrint      false    Lots of debug output is produced (in the test result HTML pages)
      timeoutSeconds  5        How long to wait for HTML elements to appear, before giving up
      windowWidth     1500     The width of the browser window
      windowHeight    1000     The height of the browser window
   These are used only in the SqlQueriesTest class, mainly in the
   insertQueryCountAndDeleteForTablesAndViews test method:
      numRowsToInsert 3        How many rows to insert, in each Table
      testTables      PARTITIONED_TABLE,REPLICATED_TABLE  Which Tables to test (or ALL)
      testViews       null     Which Views (if any) to test (or ALL)
      insertJson      false    
      sleepSeconds    0        Can slow down the tests, to watch what they are doing
   If you are running the 'genqa' test app, then PARTITIONED_TABLE and
   REPLICATED_TABLE are already defined; but if not, they will be created by
   the SqlQueriesTest class, and then dropped at the end. The system properties
   that are available are defined in:
      voltdb/tests/geb/vmc/build.gradle (all except one)
      voltdb/tests/geb/vmc/src/resources/GebConfig.groovy (timeoutSeconds only)
   So for more info, see there, or the code (especially SqlQueriesTest).

Note 5: If you want to run these tests regularly on your machine, you may want
   to set your Firefox Preferences (under Advanced, Update) to something other
   than "Automatically install updates" ("Check for updates, but let me choose
   whether to install them" is a good choice), so that your version of Firefox
   does not get ahead of what Selenium can handle.

Note 6: Running the tests against Safari does not currently work; no other
   browsers (besides Firefox, Chrome and Internet Explorer) are currently
   supported. (The browser drivers are specified in
   voltdb/tests/geb/vmc/src/test/resources/GebConfig.groovy.)

Note 7: For now, these tests must be run in a graphical environment, where the
   browser can be launched, though we hope to add a "headless" option eventually.
