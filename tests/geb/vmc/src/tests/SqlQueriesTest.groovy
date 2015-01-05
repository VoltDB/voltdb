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

import groovy.json.*
import java.util.List;
import java.util.Map;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */
class SqlQueriesTest extends TestBase {

    static final boolean INSERT_JSON_VALUES = false
    static final int NUM_ROWS_TO_INSERT = 4

    static final String SQL_QUERY_FILE = 'src/resources/sqlQueries.txt';
    static final String TABLES_FILE = 'src/resources/expectedTables.txt';
    static final String VIEWS_FILE  = 'src/resources/expectedViews.txt';
    static final String SYSTEM_STORED_PROCS_FILE  = 'src/resources/expectedSystemStoredProcs.txt';
    static final String DEFAULT_STORED_PROCS_FILE = 'src/resources/expectedDefaultStoredProcs.txt';
    static final String USER_STORED_PROCS_FILE    = 'src/resources/expectedUserStoredProcs.txt';

    static List<String> savedTables = []
    static List<String> savedViews = []
    static Boolean runningGenqa = null;

    @Shared def sqlQueriesFile = new File(SQL_QUERY_FILE)
    @Shared def tablesFile = new File(TABLES_FILE)
    @Shared def viewsFile  = new File(VIEWS_FILE)
    @Shared def systemStoredProcsFile  = new File(SYSTEM_STORED_PROCS_FILE)
    @Shared def defaultStoredProcsFile = new File(DEFAULT_STORED_PROCS_FILE)
    @Shared def userStoredProcsFile    = new File(USER_STORED_PROCS_FILE)
    
    @Shared def sqlQueryLines = []
    @Shared def tableLines = []
    @Shared def viewLines  = []
    @Shared def systemStoredProcLines = []
    @Shared def defaultStoredProcLines = []
    @Shared def userStoredProcLines = []

    @Shared def fileLinesPairs = [
        [sqlQueriesFile, sqlQueryLines],
        [tablesFile, tableLines],
        [viewsFile, viewLines],
        [systemStoredProcsFile, systemStoredProcLines],
        [defaultStoredProcsFile, defaultStoredProcLines],
        [userStoredProcsFile, userStoredProcLines],
    ]
    @Shared def slurper = new JsonSlurper()

    def setupSpec() { // called once, before any tests
        // Move contents of various test files into memory
        def getFileLines = {file,lines -> 
            if(file.size() > 0) {
                file.eachLine {
                    line -> if (!line.trim().startsWith('#')) { lines.add(line) }
                }
            }
        }
        fileLinesPairs.each { getFileLines(*it) }
    }

    def setup() { // called before each test
        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage

        when: 'click the SQL Query link (if needed)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage
    }

    /**
     * Returns the list of table names, as displayed on the page, but saving
     * the list for later, so you don't need to get it over and over again.
     * @param sqp - the SqlQueryPage from which to get the list of table names.
     * @return the list of table names, as displayed on the page.
     */
    static List<String> getTables(SqlQueryPage sqp) {
        if (savedTables == null || savedTables.isEmpty()) {
            savedTables = sqp.getTableNames()
        }
        if (runningGenqa == null) {
            runningGenqa = savedTables.containsAll(["PARTITIONED_TABLE", "REPLICATED_TABLE"])
        }
        return savedTables
    }

    /**
     * Returns the list of view names, as displayed on the page, but saving
     * the list for later, so you don't need to get it over and over again.
     * @param sqp - the SqlQueryPage from which to get the list of view names.
     * @return the list of view names, as displayed on the page.
     */
    static List<String> getViews(SqlQueryPage sqp) {
        if (savedViews == null || savedViews.isEmpty()) {
            savedViews = sqp.getViewNames()
        }
        return savedViews
    }

    /**
     * Runs, on the specified SqlQueryPage, the specified query, and returns
     * the result. (Also, if DEBUG is true, prints: the query, the result, an
     * error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param query - the query to be run.
     * @return the query result (as a Map of Lists of Strings).
     */
    static Map<String,List<String>> runQuery(SqlQueryPage sqp, String query) {
        sqp.runQuery(query)
        def qResult = sqp.getQueryResult()

        debugPrint "\nQuery:\n  " + query
        debugPrint "Result:\n  " + qResult
        def error = sqp.getQueryError()
        if (error != null) {
            debugPrint "Error: " + error
        }
        debugPrint "Duration: " + sqp.getQueryDuration()

        return qResult
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table or
     * view, a 'select * from ...' query, with an 'order by' and a limit 10'
     * clause. (Also, if DEBUG is true, prints: the table or view name, a list
     * of all column names, a list of all column types; and everything that
     * that runQuery prints, for each query.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables or views to be queried.
     * @param tableOrView - this should be "Table" or "View" - whichever is
     * contained in the list.
     */
    static def queryFrom(SqlQueryPage sqp, List<String> tables, String tableOrView) {
        tables.each {
            def columnNames = sqp.getTableColumnNames(it)
            def columnTypes = sqp.getTableColumnTypes(it)
            if (tableOrView != null && tableOrView.equalsIgnoreCase("View")) {
                columnNames = sqp.getViewColumnNames(it)
                columnTypes = sqp.getViewColumnTypes(it)
            }
            debugPrint "\n" + tableOrView + ": " + it
            debugPrint "Column names: " + columnNames
            debugPrint "Column types: " + columnTypes
            runQuery(sqp, 'select * from ' + it + ' order by ' + columnNames.get(0) + ' limit 10')
        }
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table or
     * view, a 'select count(*) as numrows from ...' query, and returns the
     * result of each query, as a List of Integers. (Also, if DEBUG is true,
     * prints everything that runQuery prints, namely: the query, the result,
     * an error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables or views to be queried.
     */
    static List<Integer> queryCount(SqlQueryPage sqp, List<String> tables) {
        def cqResults = []
        tables.each {
            def result = runQuery(sqp, 'select count(*) as numrows from ' + it)
            cqResults.add(Integer.parseInt(result.get('numrows').get(0)))
        }
        return cqResults
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table (or
     * view), a 'delete from ...' query. (Also, if DEBUG is true, prints
     * everything that runQuery prints, namely: the query, the result, an
     * error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables (or views) to be queried.
     */
    static def deleteFrom(SqlQueryPage sqp, List<String> tables) {
        tables.each {
            runQuery(sqp, 'delete from ' + it)
        }
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table (or
     * view), the specified number of 'insert into ...' queries. (Also, if
     * DEBUG is true, prints everything that runQuery prints, namely: the
     * query, the result, an error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables (or views) to be queried.
     */
    static def insertInto(SqlQueryPage sqp, List<String> tables, int numToInsert) {
        tables.each {
            def columns = sqp.getTableColumns(it)
            def count = 0
            for (int i = 1; i <= numToInsert; i++) {
                String query = "insert into " + it + " values ("
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.get(j).contains('varchar')) {
                        if (INSERT_JSON_VALUES) {
                            query += (j > 0 ? ", " : "") + "'{\"id\":\"a" + i + "\"}'"
                        } else {
                            query += (j > 0 ? ", " : "") + "'a" + i + "'"
                        }
                    } else {
                        query += (j > 0 ? ", " : "") + i
                    }
                }
                query += ")"
                runQuery(sqp, query)
            }
        }
    }

    /**
     * Runs delete from, insert into, query from, count query, and (again)
     * delete from queries, for every Table listed on the SQL Query page of
     * the VMC; and, when appropriate, for every View.<p>
     * Note: unlike the '#testName' tests, this will run against any VoltDB
     * database, since it gets the Table and View names from the UI.
     */
    def 'insert into, query from, count query, and delete from, all Tables and Views'() {
        setup: 'get list of all Tables (plus optional debug print)'
        boolean startedWithEmptyDatabase = false
        def tables = getTables(page)
        debugPrint "\n\nIn 'insert into, query from, count query, and delete from, all Tables and Views'..."
        debugPrint "\nTables:\n  " + tables
        
        when: 'perform initial count queries on all Tables'
        def cqResults = queryCount(page, tables)

        then: 'all Tables should empty, for this test (otherwise, delete at end would be dangerous)'
        cqResults.size() == tables.size()
        cqResults.each { assert it ==  0}

        when: 'insert data into all Tables'
        startedWithEmptyDatabase = true  // true, if we made it this far
        insertInto(page, tables, NUM_ROWS_TO_INSERT)

        and: 'perform queries from all Tables'
        queryFrom(page, tables, "Table")

        and: 'perform queries from all Views'
        def views = getViews(page)
        queryFrom(page, views, "View")

        and: 'perform count queries on all Tables'
        cqResults = queryCount(page, tables)

        then: 'all Tables should have the number of rows that were inserted above'
        cqResults.size() == tables.size()
        cqResults.each { assert it ==  NUM_ROWS_TO_INSERT}

        when: 'perform count queries on all Views'
        cqResults = queryCount(page, views)
        debugPrint "\nViews:\n  " + views

        then: 'all Views should have the number of rows that were inserted above'
        cqResults.size() == views.size()
        // Note: this might not always be true, but it works for 'genqa'
        if (runningGenqa) {
            cqResults.each { assert it ==  NUM_ROWS_TO_INSERT}
        }

        cleanup: 'delete all data added to all Tables (only if database was empty)'
        if (startedWithEmptyDatabase) {
            deleteFrom(page, tables)
        }
    }

    /**
     * Optionally (if DEBUG is true), prints a list of items (found somewhere
     * in the UI); and, also optionally (depending on the <i>compare</i>
     * argument), compares that list to a list of expected items.
     * 
     * @param typesToCompare - the type of items being compared (e.g. 'Tables'
     * or 'System Stored Procedures'), for print output purposes.
     * @param fileName - the name (perhaps including the path) of the file
     * containing the list of expected items, for an error message, if needed.
     * @param expected - the list of values expected to be found.
     * @param actual - the list of actual values found (in the UI).
     * @param compare - whether or not you want to do the comparison part of
     * the test (if false, the comparison is skipped).
     * @return true if the test completed successfully; otherwise, throws an
     * AssertionError.
     */
    static <T> boolean printAndCompare(String typesToCompare, String fileName,
                                       boolean compare, List<T> expected, List<T> actual) {
        // Print out the list of (actual) items - if DEBUG is true
        debugPrint '\n# ' + typesToCompare + ': (compare with ' + fileName + ')'
        actual.each { debugPrint it }

        // Check that the expected and actual stored procedures match
        if (expected == null || expected.isEmpty()) {
            assert false, 'ERROR: No expected ' + typesToCompare + ' found! '
                           + 'Need to specify some in ' + fileName
        } else if (compare) {
            assert expected == actual
        }
        return true
    }

    /**
     * Check that the list of Tables displayed on the page matches the expected
     * list (for the 'genqa' test app).
     */
    def checkTables() {
        expect: 'List of displayed Tables should match expected list'
        printAndCompare('Tables', TABLES_FILE, runningGenqa, tableLines, getTables(page))
    }
    
    /**
     * Check that the list of Views displayed on the page matches the expected
     * list (for the 'genqa' test app).
     */
    def checkViews() {
        expect: 'List of displayed Views should match expected list'
        printAndCompare('Views', VIEWS_FILE, runningGenqa, viewLines, getViews(page))
    }

    /**
     * Check that the list of System Stored Procedures displayed on the page
     * matches the expected list (for any app, not just 'genqa'!).
     */
    def checkSystemStoredProcs() {
        expect: 'List of displayed System Stored Procedures should match expected list'
        printAndCompare('System Stored Procedures', SYSTEM_STORED_PROCS_FILE, true,
                        systemStoredProcLines, page.getSystemStoredProcedures())
    }

    /**
     * Check that the list of Default Stored Procedures displayed on the page
     * matches the expected list (for the 'genqa' test app).
     */
    def checkDefaultStoredProcs() {
        expect: 'List of displayed Default Stored Procedures should match expected list'
        printAndCompare('Default Stored Procedures', DEFAULT_STORED_PROCS_FILE, runningGenqa,
                        defaultStoredProcLines, page.getDefaultStoredProcedures())
    }

    /**
     * Check that the list of User Stored Procedures displayed on the page
     * matches the expected list (for the 'genqa' test app).
     */
    def checkUserStoredProcs() {
        expect: 'List of displayed User Stored Procedures should match expected list'
        printAndCompare('User Stored Procedures', USER_STORED_PROCS_FILE, runningGenqa,
                        userStoredProcLines, page.getUserStoredProcedures())
    }

    /**
     *  Tests all the SQL queries specified in a text file. Note that these
     *  queries only work if you are running against the 'genqa' test app;
     *  otherwise, they will fail immediately.
     */
    @Unroll // performs this method for each 'testName' in the SQL Query text file
    def '#testName'() {

        if (!runningGenqa && expectedResponse.status == "SUCCESS") {
            println ("\nWARNING: Apparently not running against the 'genqa' test "
                    + "app, so this test (" + testName + ") will probably fail.")
        }

        setup: 'execute the next query (or queries)'
        page.runQuery(query)

        when: 'get the Query Result'
        def qResult = page.getQueryResult()

        debugPrint "\ntestName      : " + testName
        debugPrint "query         : " + query
        debugPrint "expect status : " + expectedResponse.status
        debugPrint "expect result : " + expectedResponse.result
        debugPrint "\nactual results: " + page.getQueryResults()
        debugPrint "last result   : " + qResult

        and: 'get the Error status, and Query Duration'
        def status   = 'SUCCESS'
        def error    = page.getQueryError()
        def duration = page.getQueryDuration()
        if (error != null) {
            status  = 'FAILURE'
            qResult = 'ERROR'
            debugPrint "actual error  : " + error
        }
        debugPrint "actual status : " + status
        debugPrint "query duration: " + duration
        if (duration == null || duration.isEmpty()) {
            println "WARNING: query duration '" + duration + "', for test '" + testName + "' is null or empty!"
        }

        then: 'check the error status, and query result'
        expectedResponse.result == qResult
        expectedResponse.status == status
        
        cleanup: 'delete all rows from the tables'
        page.runQuery('delete from partitioned_table;\ndelete from replicated_table')

        where: 'list of queries to test and expected responses'
        line << sqlQueryLines
        iter = slurper.parseText(line)
        testName = iter.testName
        query = iter.sqlCmd
        expectedResponse = iter.response
    }
}
