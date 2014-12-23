Automated Tests of the VMC (VoltDB Management Center), Using GEB
----------------------------------------------------------------

This page describes the GEB automated tests of the VMC (VoltDB Management
Center), which is the new UI, replacing the old Web Studio. These automated
tests are believed to provide more than the level of coverage of the now
deprecated GEB tests of Web Studio, in the ../studioWeb/ directory.

To run these tests of the VMC:
1. Download and install OR build from source the VoltDB pro version.
2. From the voltdb/tests/test_apps/genqa/ directory, launch a backgrounded
   genqa application server ("./run.sh server &").
3. From the voltdb/tests/geb/vmc/ directory, launch the all-in-one install/run
   script for the automated test ("./gradlew firefox --rerun-tasks").
4. Scan the console output for highlighted FAILED messages or failed test
   counts and/or browse the test result summary rooted in:
   voltdb/tests/geb/vmc/build/reports/firefoxTest/tests/index.html
5. Stop the backgrounded server ("voltadmin shutdown" or "kill %1" – or 
   "kill <whatever your actual background job number(s) may be>").

To add to or modify the existing tests:
1. To add additional SQL queries to be run in the VMC, add additional lines to
   the file voltdb/tests/geb/vmc/src/resources/sqlQueries.txt; the format is
   JSON-based, and the specific format should be fairly self-explanatory, by
   looking at existing tests.
2. More substantial changes to the tests can be made in the
   voltdb/tests/geb/vmc/src/pages/ and voltdb/tests/geb/vmc/src/tests/
   directories, which contain most of the actual Groovy / GEB code.
3. For more info on GEB, see:
      http://www.gebish.org/
      http://www.gebish.org/manual/current/
   For more info on Spock (which is also used), see;
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

Note 1: if you want to run these tests regularly on your machine, you may want
   to set your Firefox Preferences (under Advanced, Update) to something other
   than "Automatically install updates" ("Check for updates, but let me choose
   whether to install them" is a good choice), so that your version of Firefox
   does not get ahead of what Selenium can handle.
Note 2: if you want to run these tests using Chrome
   ("./gradlew chrome --rerun-tasks"), then you will first need to download the
   Chrome Driver, as described here:
      https://code.google.com/p/selenium/wiki/ChromeDriver
   (mainly, make sure it's in a directory included in the system PATH).
Note 3: running the tests against Safari does not currently work; no other
   browsers (besides Firefox & Chrome) are currently supported. (The browser
   drivers are specified in voltdb/tests/geb/vmc/src/test/resources/GebConfig.groovy.)
