/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 * validates the results by running the test methods in the TestDDLFeatures
 * class.
 */
class FullDdlSqlTest extends SqlQueriesTestBase {
    static final String FULL_DDL_FILE = '../../frontend/org/voltdb/fullddlfeatures/fullDDL.sql';

    @Shared def fullDdlFile = new File(FULL_DDL_FILE)
    @Shared def fullDdlSqlStatements = []
    @Shared def testDdlFeatures = null
    @Shared def testDdlFeaturesTestMethods = []
    @Shared def existingTables = []
    @Shared def existingViews = []
    @Shared def existingStoredProcs = []
    @Shared def newExportTables = []
    @Shared def newRoles = []
    @Shared def errors = [:]
    @Shared def ignoreTheseTestMethods = ['testCreateProcedureFromClass']

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
        String sqlTestNamesProperty = System.getProperty('sqlTestNames', '')
        debugPrint '\nsqlTestNamesProperty: ' + sqlTestNamesProperty
        def sqlTestNames = []
        if (sqlTestNamesProperty) {
            sqlTestNames = Arrays.asList(sqlTestNamesProperty.split(','))
        }
        debugPrint 'sqlTestNames:\n' + sqlTestNames
        debugPrint 'sqlTestNames.isEmpty(): ' + sqlTestNames.isEmpty()

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
                // ("CREATE TABLE", "CREATE ROLE", "CREATE PROCEDURE", "EXPORT TABLE")
                // should be combined with the previous statement; otherwise, we
                // end up with a huge number of tests, which slows things down)
                String statementUpper = statement.toUpperCase()
                if (fullDdlSqlStatements && !statementUpper.contains('CREATE TABLE') &&
                        !statementUpper.contains('CREATE ROLE') &&
                        !statementUpper.contains('CREATE PROCEDURE') &&
                        !statementUpper.contains('EXPORT TABLE')) {
                    int len = fullDdlSqlStatements.size()
                    fullDdlSqlStatements.set(len-1, fullDdlSqlStatements[len-1] + '\n' + statement)
                } else {
                    fullDdlSqlStatements.add(statement)
                }
            }
        }
        // If specific test names to run were specified, prune out all others
        if (sqlTestNames) {
            fullDdlSqlStatements.retainAll { sqlTestNames.contains(getSqlStatementTestName(it)) }
            debugPrint '\nfullDdlSqlStatements:\n' + fullDdlSqlStatements
        }

        // Get the list of TestDDLFeatures.java test methods
        testDdlFeatures = new TestDDLFeatures();
        // The first method to run should be TestDDLFeatures.startClient()
        // (even though that is not an @Test method)
        testDdlFeaturesTestMethods.add(testDdlFeatures.getClass().getMethod('startClient'))
        def allMethods = testDdlFeatures.getClass().getMethods()
        allMethods.each {
            // If specific test names to run were specified, include only those
            if (!sqlTestNames || sqlTestNames.contains(it.getName())) {
                // Include only methods with an @Test annotation
                Annotation[] annotations = it.getAnnotations()
                for (Annotation annotation : annotations) {
                    if (annotation.annotationType().equals(org.junit.Test.class)) {
                        testDdlFeaturesTestMethods.add(it)
                    }
                }
            }
        }
        if (sqlTestNames) {
            debugPrint '\ntestDdlFeaturesTestMethods:\n' + testDdlFeaturesTestMethods
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
        debugPrint '\nNew Tables (to be dropped):\n' + newTables
        debugPrint '\nNew Export Tables (to be dropped):\n' + newExportTables
        debugPrint '\nNew Views (to be dropped):\n' + newViews
        debugPrint '\nNew User Stored Procedures (to be dropped):\n' + newStoredProcs
        debugPrint '\nNew Roles (to be dropped):\n' + newRoles

        // Drop all the new roles, tables, export tables, views & (user) Stored
        // Procedures that were created by this test
        newStoredProcs.each { runQuery(page, 'Drop procedure ' + it + ' if exists') }
        newViews.each { runQuery(page, 'Drop view ' + it + ' if exists') }
        newTables.each { runQuery(page, 'Drop table ' + it + ' if exists') }
        newExportTables.each { runQuery(page, 'Drop table ' + it + ' if exists') }
        newRoles.each { runQuery(page, 'Drop role ' + it + ' if exists') }
    }

    /**
     * Give a (DDL) SQL statement, or set of statements, returns a simpler name
     * for the test that will run it in the VMC. The simpler name consists of
     * the first line of the SQL statement(s), with underscores (_) substituted
     * for spaces; also, if the SQL statement is a single line terminated with
     * a semicolon (;), that is not included.
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
     * Runs each (DDL) SQL statement specified in the fullDDL.sql file.
     */
    @Unroll // performs this method for each statement in the fullDDL.sql file
    def '#fullDdlSqlFileTestName'() {

        setup: 'get the specified (DDL) SQL statement(s) in upper case'
        String statementUpperCase = statement.toUpperCase()
        def error = null
        def duration = ''

        when: 'run the specified (DDL) SQL statement(s)'
        // Skip any 'CREATE PROCEDURE ... FROM CLASS ...' commands: there
        // is currently no way to load these, in this client-side test
        if (statementUpperCase.contains('CREATE') && statementUpperCase.contains('PROCEDURE') &&
            statementUpperCase.contains('FROM') && statementUpperCase.contains('CLASS')) {
            println '\nSkipping statement:\n' + statement
        // Execute all other statements
        } else {
            runQuery(page, statement, ColumnHeaderCase.AS_IS)
            error    = page.getQueryError()
            duration = page.getQueryDuration()
            if (error != null || duration == null || duration.isEmpty()) {
                println '\nWARNING: error non-null or duration null/empty'
                println 'Error   :' + error
                println 'Duration:' + duration
                println 'All result text:\n' + page.getQueryResultText()
            }
        }

        and: 'keep track of certain (DDL) SQL statements'
        // Keep track of CREATE ROLE statements
        if (statementUpperCase.contains('CREATE') && statementUpperCase.contains('ROLE')) {
            newRoles.add(getTableOrRoleName(statement, 'ROLE'))
        // Keep track of EXPORT TABLE statements
        } else if (statementUpperCase.contains('EXPORT') && statementUpperCase.contains('TABLE')) {
            newExportTables.add(getTableOrRoleName(statement, 'TABLE'))
        }

        then: 'make sure there was no error'
        error == null
        duration != null && !duration.contains('error')

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
        if (ignoreTheseTestMethods.contains(testDdlFeaturesTestName)) {
            debugPrint '\nSkipping JUnit Method: TestDDLFeatures.' + testDdlFeaturesTestName
        } else {
            debugPrint '\nInvoking JUnit Method: TestDDLFeatures.' + testDdlFeaturesTestName
            testMethod.invoke(testDdlFeatures)
            debugPrint 'JUnit Method passed  : TestDDLFeatures.' + testDdlFeaturesTestName
        }

        then: 'test passes if you reach this point without an AssertionFailedError'
        true

        where: 'list of TestDDLFeatures.java JUnit test methods'
        testMethod << testDdlFeaturesTestMethods
        testDdlFeaturesTestName = testMethod.getName()
    }

}
