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

import geb.*
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

    static final int DEFAULT_NUM_ROWS_TO_INSERT = 2
    static final boolean DEFAULT_INSERT_JSON = false

    static final String SQL_QUERY_FILE = 'src/resources/sqlQueries.txt';
    static final String TABLES_FILE = 'src/resources/expectedTables.txt';
    static final String VIEWS_FILE  = 'src/resources/expectedViews.txt';
    static final String SYSTEM_STORED_PROCS_FILE  = 'src/resources/expectedSystemStoredProcs.txt';
    static final String DEFAULT_STORED_PROCS_FILE = 'src/resources/expectedDefaultStoredProcs.txt';
    static final String USER_STORED_PROCS_FILE    = 'src/resources/expectedUserStoredProcs.txt';

    // Names of tables from the 'genqa' test app that are needed for testing
    static final List<String> GENQA_TEST_TABLES = ['PARTITIONED_TABLE', 'REPLICATED_TABLE']
    // Indicates the partitioning column (if any) for the corresponding table
    static final List<String> GENQA_TEST_TABLE_PARTITION_COLUMN = ['rowid', null]
    // Indicates whether the corresponding table has been created
    static List<Boolean> createdGenqaTestTable = [false, false]
    // Names of *all* tables from the 'genqa' test app
    static List<String> GENQA_ALL_TABLES = ['EXPORT_MIRROR_PARTITIONED_TABLE']

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
        // List of all 'genqa' tables should include the 'genqa' test tables
        GENQA_ALL_TABLES.addAll(GENQA_TEST_TABLES)
        // Move contents of the various files into memory
        fileLinesPairs.each { file, lines -> lines.addAll(getFileLines(file, '#', false)) }
    }

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        int count = 0

        while(count<numberOfTrials) {
            count ++
            try{
                when: 'click the SQL Query link (if needed)'
                ensureOnSqlQueryPage()
                then: 'should be on SQL Query page'
                at SqlQueryPage
                break
            } catch(org.openqa.selenium.ElementNotVisibleException e) {


            }
        }
        // Create tables from the 'genqa' app, needed for testing (e.g.
        // PARTITIONED_TABLE, REPLICATED_TABLE), if they don't already exist
        boolean createdNewTable = false;
        for (int i=0; i < GENQA_TEST_TABLES.size(); i++) {
            if (!createdGenqaTestTable.get(i)) {
                createdGenqaTestTable.set(i, createTableIfDoesNotExist(page, GENQA_TEST_TABLES.get(i),
                        GENQA_TEST_TABLE_PARTITION_COLUMN.get(i)))
                createdNewTable = createdNewTable || createdGenqaTestTable.get(i)
            }
        }
        // If new table(s) created, refresh the page, and therby the list of tables
        if (createdNewTable) {
            driver.navigate().refresh()
        }
    }

    def cleanupSpec() { // called once, after all the tests
        // Drop any tables that were created in setup()
        for (int i=0; i < GENQA_TEST_TABLES.size(); i++) {
            if (createdGenqaTestTable.get(i)) {
                ensureOnSqlQueryPage()
                runQuery(page, 'Drop table ' + GENQA_TEST_TABLES.get(i) + ';')
            }
        }
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
     * Creates a table with the specified name, if that table does not already
     * exist in the database, and with the column names and types found in the
     * PARTITIONED_TABLE and REPLICATED_TABLE, in the 'genqa' test app.
     * @param sqp - the SqlQueryPage from which to get the list of view names.
     * @param sqp - the name of the table to be created (if necessary).
     * @return the list of view names, as displayed on the page.
     */
    def boolean createTableIfDoesNotExist(SqlQueryPage sqp, String tableName, String partitionColumn='') {
        if (getTables(sqp).contains(tableName)) {
            return false
        } else {
            String ddl = 'Create table ' + tableName + ' (\n' +
                    '  rowid                     BIGINT        NOT NULL,\n' +
                    '  rowid_group               TINYINT       NOT NULL,\n' +
                    '  type_null_tinyint         TINYINT,\n' +
                    '  type_not_null_tinyint     TINYINT       NOT NULL,\n' +
                    '  type_null_smallint        SMALLINT,\n' +
                    '  type_not_null_smallint    SMALLINT      NOT NULL,\n' +
                    '  type_null_integer         INTEGER,\n' +
                    '  type_not_null_integer     INTEGER       NOT NULL,\n' +
                    '  type_null_bigint          BIGINT,\n' +
                    '  type_not_null_bigint      BIGINT        NOT NULL,\n' +
                    '  type_null_timestamp       TIMESTAMP,\n' +
                    '  type_not_null_timestamp   TIMESTAMP     NOT NULL,\n' +
                    '  type_null_float           FLOAT,\n' +
                    '  type_not_null_float       FLOAT         NOT NULL,\n' +
                    '  type_null_decimal         DECIMAL,\n' +
                    '  type_not_null_decimal     DECIMAL       NOT NULL,\n' +
                    '  type_null_varchar25       VARCHAR(32),\n' +
                    '  type_not_null_varchar25   VARCHAR(32)   NOT NULL,\n' +
                    '  type_null_varchar128      VARCHAR(128),\n' +
                    '  type_not_null_varchar128  VARCHAR(128)  NOT NULL,\n' +
                    '  type_null_varchar1024     VARCHAR(1024),\n' +
                    '  type_not_null_varchar1024 VARCHAR(1024) NOT NULL,\n' +
                    '  PRIMARY KEY (rowid)\n' +
                    ');'
            if (partitionColumn) {
                ddl += '\nPartition table ' + tableName + ' on column ' + partitionColumn + ';'
            }
            runQuery(sqp, ddl)
            // Ensure the list of tables will get updated, to include this new one
            savedTables = null
            return true
        }
    }

    /**
     * Returns whether or not we are currently running the 'genqa' test app,
     * based on whether the expected tables are listed on the page.
     * @param sqp - the SqlQueryPage from which to get the list of table names.
     * @return true if we are currently running the 'genqa' test app.
     */
    static boolean isRunningGenqa(SqlQueryPage sqp) {
        if (runningGenqa == null) {
            runningGenqa = getTables(sqp).containsAll(GENQA_ALL_TABLES)
        }
        return runningGenqa
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
    def queryFrom(SqlQueryPage sqp, List<String> tables, String tableOrView) {
        tables.each {
            def columnNames = null
            def columnTypes = null
            if (tableOrView != null && tableOrView.equalsIgnoreCase("View")) {
                columnNames = sqp.getViewColumnNames(it)
                columnTypes = sqp.getViewColumnTypes(it)
            } else {
                columnNames = sqp.getTableColumnNames(it)
                columnTypes = sqp.getTableColumnTypes(it)
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
    def List<Integer> queryCount(SqlQueryPage sqp, List<String> tables) {
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
    def deleteFrom(SqlQueryPage sqp, List<String> tables) {
        tables.each {
            runQuery(sqp, 'delete from ' + it)
        }
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table (or
     * view), the specified number of 'insert into ...' or 'upsert into ...'
     * queries. (Also, if debugPrint is true, prints everything that runQuery
     * prints, namely: the query, the result, an error message, if any, and
     * the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables (or views) to be queried.
     * @param numToInsert - the number of rows to be inserted (or upserted).
     * @param minIntValue - the minimum int value to be inserted (or upserted).
     * @param insertOrUpsert - must be either 'insert' or 'upsert'.
     */
    def insertOrUpsertInto(SqlQueryPage sqp, List<String> tables, int numToInsert,
                           int minIntValue, String insertOrUpsert) {
        def insertJson = getBooleanSystemProperty('insertJson', DEFAULT_INSERT_JSON)
        tables.each {
            def columns = sqp.getTableColumns(it)
            def count = 0
            for (int i = 1; i <= numToInsert; i++) {
                String query = insertOrUpsert + " into " + it + " values ("
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.get(j).contains('varchar')) {
                        if (insertJson) {
                            query += (j > 0 ? ", " : "") + "'{\"id\":\"z" + i + "\"}'"
                        } else {
                            query += (j > 0 ? ", " : "") + "'z" + i + "'"
                        }
                    } else {
                        query += (j > 0 ? ", " : "") + (minIntValue + i)
                    }
                }
                query += ")"
                runQuery(sqp, query)
            }
        }
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table (or
     * view), the specified number of 'insert into ...' queries. (Also, if
     * debugPrint is true, prints everything that runQuery prints, namely: the
     * query, the result, an error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables (or views) to be queried.
     * @param numToInsert - the number of rows to be inserted.
     * @param minIntValue - the minimum int value to be inserted.
     */
    def insertInto(SqlQueryPage sqp, List<String> tables, int numToInsert, int minIntValue) {
        insertOrUpsertInto(sqp, tables, numToInsert, minIntValue, 'insert')
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table (or
     * view), the specified number of 'upsert into ...' queries. (Also, if
     * debugPrint is true, prints everything that runQuery prints, namely: the
     * query, the result, an error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param tables - the list of tables (or views) to be queried.
     * @param numToInsert - the number of rows to be upserted.
     * @param minIntValue - the minimum int value to be upserted.
     */
    def upsertInto(SqlQueryPage sqp, List<String> tables, int numToInsert, int minIntValue) {
        insertOrUpsertInto(sqp, tables, numToInsert, minIntValue, 'upsert')
    }

    /**
     * Tests the query result format options (which normally are "HTML", "CSV"
     * and "Monospace").
     */
    def queryResultFormat() {
        when: 'get the query result format options (text)'
        def options = page.getQueryResultFormatOptions()
        debugPrint "\nQuery result format options (text): " + options

        and: 'get the query result format options values'
        def values = page.getQueryResultFormatOptionValues()
        debugPrint "Query result format option values: " + values

        and: 'get the initially selected query result format'
        def format = page.getSelectedQueryResultFormat()
        debugPrint "Initially selected query result format: " + format

        then: 'the query result format options (text) should match the values'
        options == values

        and: 'the query result format options should match the expected list'
        options == ['HTML', 'CSV', 'Monospace']

        and: 'the HTML format option should be selected initially'
        format == 'HTML'

        options.each {
            when: 'select one of the query result format to the options'
            page.selectQueryResultFormat(it)
            format = page.getSelectedQueryResultFormat()
            debugPrint "Currently selected query result format: " + format

            then: 'the query result format should be set to the selected option'
            format == it
        }

        cleanup: 'reset the query result format to the default option'
        page.selectQueryResultFormat('HTML')
        format = page.getSelectedQueryResultFormat()
        debugPrint "Final     selected query result format: " + format
        assert format == 'HTML'
    }

    /**
     * Runs insert into, upsert into, query from, count query, and delete from
     * queries, for every Table listed on the SQL Query page of the VMC; and,
     * when appropriate, for every View.
     * <p>Note: this can run against any VoltDB database, since it gets the
     * Table and View names from system properties, or from the UI.
     */
    def insertQueryCountAndDeleteForTablesAndViews() {
        setup: 'get the list of Tables to test'
        boolean startedWithEmptyTables = false
        String testTables = System.getProperty('testTables', '')
        def tables = []
        if ('ALL'.equalsIgnoreCase(testTables)) {
            tables = getTables(page)
        } else if (testTables) {
            tables = Arrays.asList(testTables.split(','))
        }
        debugPrint "\nTables to test:  " + tables

        and: 'get the list of Views to test'
        String testViews = System.getProperty('testViews', '')
        def views = []
        if ('ALL'.equalsIgnoreCase(testViews)) {
            views = getViews(page)
        } else if (testViews) {
            views = Arrays.asList(testViews.split(','))
        }
        debugPrint "\nViews to test:  " + views

        when: 'perform initial count queries on all Tables'
        def cqResults = queryCount(page, tables)

        then: 'number of count query results should match number of Tables'
        cqResults.size() == tables.size()

        when: 'check whether all Tables are empty (otherwise, delete at end would be dangerous)'
        boolean allTablesEmpty = true
        def minValuesForEachTable = [:]
        cqResults.eachWithIndex { res, i ->
            if (res > 0) {
                allTablesEmpty = false
                // TODO: improve this (to get max values, and insert/delete above them)
                //def 
                //minValuesForEachTable.put(tables[i], getFirstColumnAndMaxValue(tables[i]))
            }
        }
        startedWithEmptyTables = allTablesEmpty

        and: 'insert data into the test Tables'
        def numRows = getIntSystemProperty('numRowsToInsert', DEFAULT_NUM_ROWS_TO_INSERT)
        insertInto(page, tables, numRows, 100)

        and: 'perform queries from the test Tables (after insert)'
        queryFrom(page, tables, "Table")

        and: 'perform queries from the test Views (after insert)'
        queryFrom(page, views, "View")

        and: 'perform count queries on the test Tables (after insert)'
        cqResults = queryCount(page, tables)

        then: 'the test Tables should have the number of rows that were inserted above'
        cqResults.size() == tables.size()
        cqResults.each { assert it ==  numRows}

        when: 'perform count queries on the test Views (after insert)'
        cqResults = queryCount(page, views)
        debugPrint "\nViews:  " + views

        then: 'the test Views should have the number of rows that were inserted above'
        cqResults.size() == views.size()
        // Note: this might not always be true, but it works for 'genqa'
        if (isRunningGenqa(page)) {
            cqResults.each { assert it ==  numRows}
        }

        when: 'upsert data into the test Tables'
        startedWithEmptyTables = true  // true, if we made it this far
        upsertInto(page, tables, numRows, 101)

        and: 'perform queries from the test Tables (after upsert)'
        queryFrom(page, tables, "Table")

        and: 'perform queries from the test Views (after upsert)'
        queryFrom(page, views, "View")

        and: 'perform count queries on the test Tables (after upsert)'
        cqResults = queryCount(page, tables)

        then: 'the test Tables should have the number of rows that were inserted/upserted above'
        cqResults.size() == tables.size()
        cqResults.each { assert it == numRows + 1}

        when: 'perform count queries on the test Views (after upsert)'
        cqResults = queryCount(page, views)

        then: 'the test Views should have the number of rows that were inserted/upserted above'
        cqResults.size() == views.size()
        // Note: this might not always be true, but it works for 'genqa'
        if (isRunningGenqa(page)) {
            cqResults.each { assert it == numRows + 1}
        }

        cleanup: 'delete all data added to the test Tables (only if they were all empty)'
        if (startedWithEmptyTables) {
            deleteFrom(page, tables)
        }
    }

    /**
     * Check that the list of Tables displayed on the page matches the expected
     * list (for the 'genqa' test app).
     */
    def checkTables() {
        expect: 'List of displayed Tables should match expected list'
        printAndCompare('Tables', TABLES_FILE, isRunningGenqa(page), tableLines, getTables(page))
    }

    /**
     * Check that the list of Views displayed on the page matches the expected
     * list (for the 'genqa' test app).
     */
    def checkViews() {
        expect: 'List of displayed Views should match expected list'
        printAndCompare('Views', VIEWS_FILE, isRunningGenqa(page), viewLines, getViews(page))
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
        printAndCompare('Default Stored Procedures', DEFAULT_STORED_PROCS_FILE, isRunningGenqa(page),
                defaultStoredProcLines, page.getDefaultStoredProcedures())
    }

    /**
     * Check that the list of User Stored Procedures displayed on the page
     * matches the expected list (for the 'genqa' test app).
     */
    def checkUserStoredProcs() {
        expect: 'List of displayed User Stored Procedures should match expected list'
        printAndCompare('User Stored Procedures', USER_STORED_PROCS_FILE, isRunningGenqa(page),
                userStoredProcLines, page.getUserStoredProcedures())
    }

    /**
     * Tests all the SQL queries specified in the sqlQueries.txt file.
     */
    @Unroll // performs this method for each test in the SQL Query text file
    def '#sqlQueriesTestName'() {

        setup: 'execute the next query (or queries)'
        runQuery(page, query)

        when: 'get the Query Result'
        def qResult = page.getQueryResult()

        debugPrint "\nquery         : " + query
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
            println "WARNING: query duration '" + duration + "', for test '" +
                    sqlQueriesTestName + "' is null or empty!"
        }

        then: 'check the error status, and query result'
        expectedResponse.result == qResult
        expectedResponse.status == status

        cleanup: 'delete all rows from the tables'
        runQuery(page, 'delete from partitioned_table;\ndelete from replicated_table')

        where: 'list of queries to test and expected responses'
        line << sqlQueryLines
        iter = slurper.parseText(line)
        sqlQueriesTestName = iter.testName
        query = iter.sqlCmd
        expectedResponse = iter.response
    }

    //sql queries test for admin-client port


    def "Check sqlquery client to admin port switching for cancel popup"() {

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String checkQuery = page.getQueryToCreateTable()

        when: 'set create query in the box'
        page.setQueryText(checkQuery)
        then: 'run the query'
        page.runQuery()

        try {
            waitFor(10) {
                page.cancelpopupquery.isDisplayed()
                page.cancelpopupquery.click()
                page.queryDurHtml.isDisplayed()
                println("result shown without popup, hence it is in admin port")
                println("cancel button clicked")

            }



        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("pop up won't occurr due to already in running state")
            println("it is already in admin port")

        } catch (geb.waiting.WaitTimeoutException e) {


            println("already in admin port state")

        }

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        try {
            waitFor(10) {
                page.networkInterfaces.clusterClientPortValue.isDisplayed()
                cluster.pausebutton.isDisplayed()
            }
            cluster.pausebutton.click()
            waitFor(10) { cluster.pauseok.isDisplayed() }
            cluster.pauseok.click()
            println("Pause button displayed and clicked!!")

        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("Already in pause state!! in admin page.")

        } catch (geb.waiting.WaitTimeoutException e) {

            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            println("rechecking due to geb waiting exception")

        }

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()
        try {
            waitFor(15) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.isDisplayed()
                page.switchadminport.isDisplayed()
                page.queryexecutionerror.isDisplayed()
                page.queryerrortxt.isDisplayed()
            }

            page.cancelpopupquery.click()
            println("all popup query verified for creating table!!")
        }catch(geb.waiting.WaitTimeoutException e) {println("waiting time exceed here")}

        when: 'set select query in the box'
        page.setQueryText("SELECT * FROM " + tablename)
        then: 'run the query'
        page.runQuery()
        try {
            waitFor(5) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.isDisplayed()
                page.switchadminport.isDisplayed()
                page.queryexecutionerror.isDisplayed()
                page.queryerrortxt.isDisplayed()
            }
            page.cancelpopupquery.click()
            println("all popup query verified for selecting data from table!!")

            when: 'set delete query in the box'
            page.setQueryText(deleteQuery)
            then: 'run the query'
            page.runQuery()
            waitFor(5) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.isDisplayed()
                page.switchadminport.isDisplayed()
                page.queryexecutionerror.isDisplayed()
                page.queryerrortxt.isDisplayed()
            }
            page.cancelpopupquery.click()
            println("all popup for query verified for deleting data from table!!")
        }catch(geb.error.RequiredPageContentNotPresent e) {println("element not found")}

        catch(geb.waiting.WaitTimeoutException e) {println("waiting time exceed here")}
    }



    def "Check sqlquery client to admin port switching for ok poup"() {
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        try {
            waitFor(10) {

                page.networkInterfaces.clusterClientPortValue.isDisplayed()
                cluster.pausebutton.click()
                cluster.pauseok.click()
                println("Pause button displayed and clicked!!")}

        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("Already in resume state!!")

        } catch (geb.waiting.WaitTimeoutException e) {

            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            println("rechecking due to geb waiting exception")

        }

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        try {
            waitFor(10) {


                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.isDisplayed()
                page.switchadminport.isDisplayed()
                page.queryexecutionerror.isDisplayed()
                page.queryerrortxt.isDisplayed()
            }

            page.okpopupquery.click()
            println("all popup query verified for creating table!!")
        } catch(geb.waiting.WaitTimeoutException e) {println("waiting time exceed")}

        try {
            if(waitFor(5){page.htmlresultallcolumns.isDisplayed()}){
                println("all columns displayed for creating table as: " +page.htmlresultallcolumns.text())}
            if(waitFor(5){page.htmltableresult.isDisplayed()}){
                println("table result shown for creating table HTML format i.e, "+page.htmltableresult.text())
            }

        }catch (geb.waiting.WaitTimeoutException e) {println("couldn't check due to server not online error or waiting time error")}


        when: 'set select query in the box'
        page.setQueryText("SELECT * FROM " + tablename)
        then: 'run the query'
        page.runQuery()

        try {
            if(waitFor(5){page.htmlresultselect.isDisplayed()}){
                println("all columns displayed for selecting table as: " +page.htmlresultselect.text())}

        }catch (geb.waiting.WaitTimeoutException e) {println("couldn't check due to server not online error or waiting time error")}


        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()

    }


// for tables

    def "Check created table by refreshing in SQL QUERY tab and Schema tab"() {

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String checkQuery = page.getQueryToCreateTable()

        when: 'set create query in the box'
        page.setQueryText(checkQuery)
        then: 'run the query'
        page.runQuery()


        try {
            waitFor(10) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.click()

            }

        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("pop up won't occurr due to already in running state")


        } catch (geb.waiting.WaitTimeoutException e) {


            println("already in admin port state")

        }

        waitFor(5){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        println("refresh button clicked and created table shown in SQLQuery tab!!")


        // In Schema Page Schema Tab
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(5){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created table shown in schema page of schema tab")


        // In Schema page DDL Source tab
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(5){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created table shown in Schema page of DDL source tab")

        // In Size and Worksheet tab
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(5){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created table shown in schema page of Size and worksheet tab")


        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage


        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename =  page.getTablename()

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()
        waitFor(5){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        println("created table deleted!! in SQL Query tab")


        // In Schema Page Schema Tab for checking deleted table
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(5){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created table deleted in schema tab")

        // In Schema page DDL Source tab for checking deleted table
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(5){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created table deleted in Schema page of DDL source tab")

        // In Size and Worksheet tab for checking deleted table
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(5){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created table deleted in schema page of Size and worksheet tab")

    }

// for views

    def "Check created views by refreshing in SQL QUERY tab and Schema tab"() {

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String checkQuery = page.getQueryToCreateView()

        when: 'set create query in the box'
        page.setQueryText(checkQuery)
        then: 'run the query'
        page.runQuery()


        try {
            waitFor(10) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.click()

            }

        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("pop up won't occurr due to already in running state")


        } catch (geb.waiting.WaitTimeoutException e) {


            println("already in admin port state")

        }



        waitFor(5){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        try {
            waitFor(15){page.checkview.isDisplayed()}
            page.checkview.click()} catch (geb.error.RequiredPageContentNotPresent e) {println("element not found")}catch (geb.waiting.WaitTimeoutException e){println("waiting timeout")}
        println("views that is created has been displayed!!")

        // In Schema Page Schema Tab
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(5){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created views shown in schema page of schema tab")


        // In Schema page DDL Source tab
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(5){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created views shown in Schema page of DDL source tab")

        // In Size and Worksheet tab
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(5){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created views shown in schema page of Size and worksheet tab")


        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage


        String createQuery = page.getQueryToCreateView()
        String deleteQuery = page.getQueryToDeleteView()
        String tablename =  page.getTablename()

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()

        waitFor(5){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        waitFor(10){page.checkview.isDisplayed()}
        page.checkview.click()
        println("created views has been deleted!! in SQL Query tab")


        // In Schema Page Schema Tab for checking deleted table
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(5){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created views deleted in schema tab")

        // In Schema page DDL Source tab for checking deleted table
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(5){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created views deleted in Schema page of DDL source tab")

        // In Size and Worksheet tab for checking deleted table
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(5){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created views deleted in schema page of Size and worksheet tab")

    }


}
