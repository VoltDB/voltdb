/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package vmcTest.tests

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.voltdb.fullddlfeatures.TestDDLFeatures
import spock.lang.*
import vmcTest.pages.SqlQueryPage
import vmcTest.pages.VoltDBManagementCenterPage.ColumnHeaderCase

/**
 * This class tests the 'fullDDL.sql' file in the VMC, by running its queries
 * on the 'SQL Query' tab of the VoltDB Management Center (VMC) page, and
 * validates the results by running the JUnit test methods in the TestDDLFeatures
 * class, via reflection.
 */
class FullDdlSqlBasicTest extends SqlQueriesTestBase {
    static final String FULL_DDL_FILE = '../../frontend/org/voltdb/fullddlfeatures/fullDDL.sql';

    @Shared def fullDdlFile = new File(FULL_DDL_FILE)
    @Shared def fullDdlSqlStatements = []
    @Shared def testDdlFeatures = null
    @Shared def testDdlFeaturesTestMethods = []
    @Shared def existingTables = []
    @Shared def existingViews = []
    @Shared def existingStoredProcs = []
    @Shared def newStreams = []
    @Shared def newRoles = []
    @Shared def errors = [:]

    def setupSpec() { // called once, before any tests
        // Make sure we're on the SQL Query page
        when: 'click the SQL Query link (if needed)'
        ensureOnSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        // Check which tables, views & (user) Stored Procs exist, before this
        // test (so we can delete the new ones, at the end)
        existingTables = page.getTableNames()
        existingViews  = page.getViewNames()
        existingStoredProcs = page.getUserStoredProcedures()
        debugPrint '\nExisting Tables:\n' + existingTables
        debugPrint '\nExisting Views:\n' + existingViews
        debugPrint '\nExisting User Stored Procedures:\n' + existingStoredProcs

        // Get the list of tests that we actually want to run
        // (if empty, run all tests)
        String sqlTestsProperty = System.getProperty('sqlTests', '')
        def sqlTests = []
        if (sqlTestsProperty) {
            sqlTests = Arrays.asList(sqlTestsProperty.split(','))
            debugPrint '\nsqlTests:\n' + sqlTests
        }
        debugPrint 'sqlTests.isEmpty(): ' + sqlTests.isEmpty()

        // Get the lines of the fullDDL.sql file (ignoring comment lines
        // starting with '--', and blank lines)
        def lines = getFileLines(fullDdlFile, '--', false)

        // Break the lines into statements, based on ending with ';'
        int startStatementAtLine = 0
        lines.eachWithIndex { line, i ->
            if (line.trim().endsWith(';')) {
                def statement = lines[startStatementAtLine]
                for (int j=startStatementAtLine+1; j <= i; j++) {
                    statement += '\n' + lines[j]
                }
                startStatementAtLine = i + 1
                // DDL statements that do not include certain key phrases
                // ("CREATE TABLE", "CREATE ROLE", "CREATE STREAM")
                // should be combined with the previous statement; otherwise, we
                // end up with a huge number of tests, which slows things down)
                String statementUpper = statement.toUpperCase()
                if (fullDdlSqlStatements && !statementUpper.contains('CREATE TABLE') &&
                        !statementUpper.contains('CREATE ROLE') &&
                        !statementUpper.contains('CREATE STREAM')) {
                    int len = fullDdlSqlStatements.size()
                    fullDdlSqlStatements.set(len-1, fullDdlSqlStatements[len-1] + '\n' + statement)
                } else {
                    fullDdlSqlStatements.add(statement)
                }
            }
        }
        // If specific test names to run were specified, prune out all others
        if (sqlTests) {
            fullDdlSqlStatements.retainAll { sqlTests.contains(getSqlStatementTestName(it)) }
            //debugPrint '\nfullDdlSqlStatements:\n' + fullDdlSqlStatements
        }

        // Get the list of TestDDLFeatures.java test methods
        testDdlFeatures = new TestDDLFeatures();
        // The first method to run should be TestDDLFeatures.startClient()
        // (even though that is not an @Test method)
        testDdlFeaturesTestMethods.add(testDdlFeatures.getClass().getMethod('startClient'))
        def allMethods = testDdlFeatures.getClass().getMethods()
        allMethods.each {
            // If specific test names to run were specified, include only those
            if (!sqlTests || sqlTests.contains(it.getName())) {
                // Include only methods with an @Test annotation
                Annotation[] annotations = it.getAnnotations()
                for (Annotation annotation : annotations) {
                    if (annotation.annotationType().equals(org.junit.Test.class)) {
                        testDdlFeaturesTestMethods.add(it)
                    }
                }
            }
        }
        if (sqlTests) {
            debugPrint '\ntestDdlFeaturesTestMethods:\n[' + (testDdlFeaturesTestMethods.collect { it.getName() }).join(', ') + ']'
        }
    }

    def cleanupSpec() { // called once, after all the tests
        // Make sure we're on the SQL Query page
        when: 'click the SQL Query link (if needed)'
        ensureOnSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        // Next, get the lists of new tables, views & (user) Stored Procs,
        // created by this test
        def newTables = page.getTableNames()
        def newViews  = page.getViewNames()
        def newStoredProcs = page.getUserStoredProcedures()
        newTables.removeAll(existingTables)
        newViews.removeAll(existingViews)
        newStoredProcs.removeAll(existingStoredProcs)
        debugPrint '\nNew Roles (to be dropped):\n' + newRoles
        debugPrint '\nNew Tables (to be dropped):\n' + newTables
        debugPrint '\nNew Streams (to be dropped):\n' + newStreams
        debugPrint '\nNew Views (to be dropped):\n' + newViews
        debugPrint '\nNew User Stored Procedures (to be dropped):\n' + newStoredProcs

        // Drop all the new roles, tables, stream tables, views & (user) Stored
        // Procedures that were created by this test
        newStoredProcs.each { runQuery(page, 'Drop procedure ' + it + ' if exists') }
        newViews.each { runQuery(page, 'Drop view ' + it + ' if exists') }
        newStreams.each { runQuery(page, 'Drop stream ' + it + ' if exists') }
        newTables.each { runQuery(page, 'Drop table ' + it + ' if exists') }
        newRoles.each { runQuery(page, 'Drop role ' + it + ' if exists') }
    }

    /**
     * Given a (DDL) SQL statement, or set of statements, returns a simpler name
     * for the test that will run it in the VMC. The simpler name consists of
     * the first line of the SQL statement(s), with underscores (_) substituted
     * for spaces; also, if the SQL statement is a single line terminated with
     * a semicolon (;), the semicolon is not included.
     * @param statement - the complete (DDL) SQL statement that contains the name.
     * @return a simpler test name, based on the <i>statement</i>.
     */
    private String getSqlStatementTestName(String statement) {
        int nameEnd = statement.indexOf('\n')
        if (nameEnd < 1) {
            nameEnd = statement.indexOf(';')
        }
        if (nameEnd < 1) {
            nameEnd = statement.length()
        }
        return statement.substring(0, nameEnd).replace(' ', '_')
    }

    /**
     * Returns the 'name' within a specified (DDL) query, following a specified
     * part of that query; for example, the name of the role in a CREATE ROLE
     * query, or the name of the table in an EXPORT TABLE query.
     * @param query - the complete (DDL) query that contains the name.
     * @param afterThis - the word, within the <i>query</i>, after which the
     * name is to be found.
     * @return the next identifier that follows <i>afterThis</i>, within the
     * <i>query</i>.
     */
    private String getTableOrRoleName(String query, String afterThis) {
        int start = query.toUpperCase().indexOf(afterThis) + afterThis.length()
        int end = query.length();
        for (int i=start; i < end; i++) {
            if (Character.isJavaIdentifierStart(query.charAt(i))) {
                start = i
                break
            }
        }
        for (int i=start; i < query.length(); i++) {
            if (!Character.isJavaIdentifierPart(query.charAt(i))) {
                end = i
                break
            }
        }
        return query.substring(start, end)
    }

    /**
     * Runs the specifed (DDL) SQL statement(s).
     * @param statement - the (DDL) SQL statement(s) to be run.
     * @return the error returned by running the statement(s), if any; normally
     * <b>null</b>, assuming that no error was returned.
     */
    private String runDdlSqlStatement(String statement) {
        String statementUpperCase = statement.toUpperCase()
        runQuery(page, statement, ColumnHeaderCase.AS_IS)
        String error = page.getQueryError()

        // Keep track of certain (DDL) SQL statements:
        // Keep track of CREATE ROLE statements
        if (statementUpperCase.contains('CREATE') && statementUpperCase.contains('ROLE')) {
            newRoles.add(getTableOrRoleName(statement, 'ROLE'))
        // Keep track of CREATE STREAM statements
        } else if (statementUpperCase.contains('CREATE') && statementUpperCase.contains('STREAM')) {
            newStreams.add(getTableOrRoleName(statement, 'STREAM'))
        }
        return error
    }

    /**
     * Runs each (DDL) SQL statement specified in the fullDDL.sql file.
     */
    @Unroll // performs this method for each statement in the fullDDL.sql file
    def '#fullDdlSqlFileTestName'() {

        setup: 'initializations'
        String error = null
        String duration = ''
        boolean foundError = false

        when: 'run the specified (DDL) SQL statement(s)'
        // For a really long set of (DDL) SQL statements, break them into
        // smaller groups of statements, then run each group individually
        int maxNumStatementsPerGroup = 15
        if ((statement =~ ';').count > maxNumStatementsPerGroup) {
            int start = 0
            int semicolon = 0
            while (semicolon < statement.length()) {
                for (int j = 0; j < maxNumStatementsPerGroup; j++) {
                    semicolon = statement.indexOf(';', semicolon) + 1
                    if (semicolon <= 0) {
                        semicolon = statement.length()
                        break
                    }
                }
                error = runDdlSqlStatement(statement.substring(start, semicolon))
                start = semicolon
                duration = page.getQueryDuration()
                if (error != null || duration == null || duration.isEmpty() || duration.contains('error')) {
                    println '\nFAILURE: error non-null or duration null/empty/has error:'
                    println 'Error   : ' + error
                    println 'Duration: ' + duration
                    println 'All result text:\n' + page.getQueryResultText()
                    foundError = true
                }
            }

        // Usual case: run the entire (DDL) SQL statement(s), all together
        } else {
            error = runDdlSqlStatement(statement)
            duration = page.getQueryDuration()
            // If there is a problem, refresh the page and give it a second try
            if (error != null || duration == null || duration.isEmpty() || duration.contains('error')) {
                println '\nWARNING: error non-null or duration null/empty/has error; ' +
                        'will refresh page and make a second attempt to run the SQL:'
                println 'Error   : ' + error
                println 'Duration: ' + duration
                println 'All result text:\n' + page.getQueryResultText()
                driver.navigate().refresh()
                error = runDdlSqlStatement(statement)
                duration = page.getQueryDuration()
                println '\nResult of second attempt to run the SQL:'
                println 'Error   : ' + error
                println 'Duration: ' + duration
                println 'All result text:\n' + page.getQueryResultText()
            }
        }

        then: 'make sure there was no error'
        error == null
        duration != null && !duration.isEmpty() && !duration.contains('error')
        !foundError

        where: 'list of DDL SQL statements to test'
        statement << fullDdlSqlStatements
        fullDdlSqlFileTestName = getSqlStatementTestName(statement)
    }

    /**
     * Runs each JUnit test in TestDDLFeatures.java. (Note: the corresponding
     * SQL DDL statements in the fullDDL.sql file must be run first.)
     */
    @Unroll // performs this method for each JUnit test in TestDDLFeatures.java
    def '#testDdlFeaturesTestName'() {
        when: 'run the specified JUnit test method'
        debugPrint '\nInvoking JUnit Method: TestDDLFeatures.' + testDdlFeaturesTestName
        testMethod.invoke(testDdlFeatures)
        debugPrint 'JUnit Method passed  : TestDDLFeatures.' + testDdlFeaturesTestName

        then: 'test passes if you reach this point without an AssertionFailedError'
        true

        where: 'list of TestDDLFeatures.java JUnit test methods'
        testMethod << testDdlFeaturesTestMethods
        testDdlFeaturesTestName = testMethod.getName()
    }

}
