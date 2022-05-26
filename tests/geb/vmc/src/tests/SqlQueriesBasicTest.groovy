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

import geb.*
import groovy.json.*
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */

class SqlQueriesBasicTest extends SqlQueriesTestBase {

    static final int DEFAULT_NUM_ROWS_TO_INSERT = 2
    static final boolean DEFAULT_INSERT_JSON = false

    static final String SQL_QUERY_FILE = 'src/resources/sqlQueries.txt';

    // Files used to determine the expected Tables, Streams, Views, and (Default
    // and User-defined) Stored Procedures, when running the Voter example app
    static final String VOTER_TABLES_FILE  = 'src/resources/expectedVoterTables.txt';
    static final String VOTER_STREAMS_FILE = 'src/resources/expectedVoterStreams.txt';
    static final String VOTER_VIEWS_FILE   = 'src/resources/expectedVoterViews.txt';
    static final String VOTER_DEFAULT_STORED_PROCS_FILE = 'src/resources/expectedVoterDefaultStoredProcs.txt';
    static final String VOTER_USER_STORED_PROCS_FILE    = 'src/resources/expectedVoterUserStoredProcs.txt';

    // Files used to determine the expected Tables, Streams, Views, and (Default
    // and User-defined) Stored Procedures, when running the Genqa test app
    static final String GENQA_TABLES_FILE  = 'src/resources/expectedGenqaTables.txt';
    static final String GENQA_STREAMS_FILE = 'src/resources/expectedGenqaStreams.txt';
    static final String GENQA_VIEWS_FILE   = 'src/resources/expectedGenqaViews.txt';
    static final String GENQA_DEFAULT_STORED_PROCS_FILE = 'src/resources/expectedGenqaDefaultStoredProcs.txt';
    static final String GENQA_USER_STORED_PROCS_FILE    = 'src/resources/expectedGenqaUserStoredProcs.txt';

    // Files used to determine the expected Tables, Streams, Views, and (Default,
    // User-defined and System) Stored Procedures, when running the GEB VMC test server
    // (which is the default; see voltdb/tests/geb/vmc/server/run_voltdb_server.sh)
    static final String TABLES_FILE  = 'src/resources/expectedTables.txt';
    static final String STREAMS_FILE = 'src/resources/expectedStreams.txt';
    static final String VIEWS_FILE   = 'src/resources/expectedViews.txt';
    static final String DEFAULT_STORED_PROCS_FILE = 'src/resources/expectedDefaultStoredProcs.txt';
    static final String USER_STORED_PROCS_FILE    = 'src/resources/expectedUserStoredProcs.txt';
    static final String SYSTEM_STORED_PROCS_FILE  = 'src/resources/expectedSystemStoredProcs.txt';

    // Values used to determine which app we are running
    static final List<String> VOTER_TEST_TABLES   = ['AREA_CODE_STATE', 'CONTESTANTS', 'VOTES']
    static final List<String> GENQA_TEST_TABLES   = ['EXPORT_MIRROR_PARTITIONED_TABLE', 'PARTITIONED_TABLE', 'REPLICATED_TABLE']
    static final List<String> GENQA_TEST_USER_STORED_PROCS = ['JiggleExportDoneTable', 'JiggleExportSinglePartition',
                                                              'JiggleSkinnyExportSinglePartition']

    // Names of standard tables that are needed for testing (these names
    // originally came from the 'Genqa' test app, but are now also used
    // by the default GEB VMC test server)
    static final List<String> STANDARD_TEST_TABLES = ['PARTITIONED_TABLE', 'REPLICATED_TABLE']
    // Indicates the partitioning column (if any) for the corresponding table
    static final List<String> STANDARD_TEST_TABLE_PARTITION_COLUMN = ['rowid', null]
    // Indicates whether the corresponding table has been created
    static List<Boolean> createdStandardTestTable = [false, false]

    static List<String> savedTables  = []
    static List<String> savedStreams = []
    static List<String> savedViews  = []
    static Boolean initialized  = false;
    static Boolean runningVoter = null;
    static Boolean runningGenqa = null;
    static Boolean runningVmcTestSever = null;
    static Map<String,Object> sqlQueryVariables = [:]

    @Shared String tablesFileName  = TABLES_FILE
    @Shared String streamsFileName = STREAMS_FILE
    @Shared String viewsFileName   = VIEWS_FILE
    @Shared String defaultStoredProcsFileName = DEFAULT_STORED_PROCS_FILE
    @Shared String userStoredProcsFileName    = USER_STORED_PROCS_FILE

    @Shared def sqlQueriesFile = new File(SQL_QUERY_FILE)
    @Shared def tablesFile  = new File(TABLES_FILE)
    @Shared def streamsFile = new File(STREAMS_FILE)
    @Shared def viewsFile   = new File(VIEWS_FILE)
    @Shared def systemStoredProcsFile  = new File(SYSTEM_STORED_PROCS_FILE)
    @Shared def defaultStoredProcsFile = new File(DEFAULT_STORED_PROCS_FILE)
    @Shared def userStoredProcsFile    = new File(USER_STORED_PROCS_FILE)

    @Shared def sqlQueryLines = []
    @Shared def tableLines  = []
    @Shared def streamLines = []
    @Shared def viewLines   = []
    @Shared def systemStoredProcLines = []
    @Shared def defaultStoredProcLines = []
    @Shared def userStoredProcLines = []

    @Shared def fileLinesPairs = [
            [sqlQueriesFile, sqlQueryLines],
            [tablesFile, tableLines],
            [streamsFile, streamLines],
            [viewsFile, viewLines],
            [systemStoredProcsFile, systemStoredProcLines],
            [defaultStoredProcsFile, defaultStoredProcLines],
            [userStoredProcsFile, userStoredProcLines],
    ]
    @Shared def slurper = new JsonSlurper()

    def setupSpec() { // called once, before any tests

        // Move the contents of the various files into memory
        readFiles(false)

        // Get the list of tests that we actually want to run
        // (if empty, run all tests)
        String sqlTestsProperty = System.getProperty('sqlTests', '')
        debugPrint '\nsqlTestsProperty: ' + sqlTestsProperty
        def sqlTests = []
        if (sqlTestsProperty) {
            sqlTests = Arrays.asList(sqlTestsProperty.split(','))
        }
        debugPrint 'sqlTests:\n' + sqlTests
        debugPrint 'sqlTests.isEmpty(): ' + sqlTests.isEmpty()

        // If specific test names to run were specified, prune out all others
        if (sqlTests) {
            sqlQueryLines.retainAll { line -> sqlTests.find { name -> line.contains(name) } }
            debugPrint '\nsqlQueryLines:\n' + sqlQueryLines
        }
    }

    def setup() { // called before each test
        // SqlQueriesTestBase.setup gets called first (automatically)

        // Initializations that can only occur after we are on a SqlQueryPage
        // (which we are not, when running setupSpec)
        if (!initialized) {
            initialized = true
            // Determine whether we are running against the Genqa test app or the Voter
            // example app; otherwise, we assume we are running against the default GEB
            // VMC test server (see voltdb/tests/geb/vmc/server/run_voltdb_server.sh)
            if (isRunningVoter(page)) {
                tablesFileName = VOTER_TABLES_FILE
                viewsFileName = VOTER_VIEWS_FILE
                defaultStoredProcsFileName = VOTER_DEFAULT_STORED_PROCS_FILE
                userStoredProcsFileName = VOTER_USER_STORED_PROCS_FILE
                tablesFile = new File(tablesFileName)
                viewsFile  = new File(viewsFileName)
                defaultStoredProcsFile = new File(defaultStoredProcsFileName)
                userStoredProcsFile    = new File(userStoredProcsFileName)
                readFiles()  // reinitialize the file data we just changed
            } else if (isRunningGenqa(page)) {
                tablesFileName = GENQA_TABLES_FILE
                viewsFileName = GENQA_VIEWS_FILE
                defaultStoredProcsFileName = GENQA_DEFAULT_STORED_PROCS_FILE
                userStoredProcsFileName = GENQA_USER_STORED_PROCS_FILE
                tablesFile = new File(tablesFileName)
                viewsFile  = new File(viewsFileName)
                defaultStoredProcsFile = new File(defaultStoredProcsFileName)
                userStoredProcsFile    = new File(userStoredProcsFileName)
                readFiles()  // reinitialize the file data we just changed
            }
        }

        // Create standard tables, needed for testing (e.g. PARTITIONED_TABLE,
        // REPLICATED_TABLE), if they don't already exist
        boolean createdNewTable = false;
        for (int i=0; i < STANDARD_TEST_TABLES.size(); i++) {
            if (!createdStandardTestTable.get(i)) {
                createdStandardTestTable.set(i, createTableIfDoesNotExist(page, STANDARD_TEST_TABLES.get(i),
                        STANDARD_TEST_TABLE_PARTITION_COLUMN.get(i)))
                createdNewTable = createdNewTable || createdStandardTestTable.get(i)
            }
        }
        // If new table(s) created, refresh the page, and therby the list of tables
        if (createdNewTable) {
            driver.navigate().refresh()
        }
    }

    def cleanupSpec() { // called once, after all the tests
        // Drop any tables that were created in setup()
        for (int i=0; i < STANDARD_TEST_TABLES.size(); i++) {
            if (createdStandardTestTable.get(i)) {
                ensureOnSqlQueryPage()
                runQuery(page, 'Drop table ' + STANDARD_TEST_TABLES.get(i) + ';')
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
     * Returns the list of stream names, as displayed on the page, but saving
     * the list for later, so you don't need to get it over and over again.
     * @param sqp - the SqlQueryPage from which to get the list of stream names.
     * @return the list of stream names, as displayed on the page.
     */
    static List<String> getStreams(SqlQueryPage sqp) {
        if (savedStreams == null || savedStreams.isEmpty()) {
            savedStreams = sqp.getStreamNames()
        }
        return savedStreams
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
     * Reads the contents of various input files, and loads their lines into
     * memory, in the form of various list of lines from each file.
     * @param clearValues - whether or not to empty the list of lines read from
     * each file, before loading them (again); default is <b>true</b>.
     */
    def readFiles(clearValues=true) {
        if (clearValues) {
            sqlQueryLines = []
            tableLines = []
            streamLines  = []
            viewLines  = []
            systemStoredProcLines = []
            defaultStoredProcLines = []
            userStoredProcLines = []
            fileLinesPairs = [
                [sqlQueriesFile, sqlQueryLines],
                [tablesFile, tableLines],
                [streamsFile, streamLines],
                [viewsFile, viewLines],
                [systemStoredProcsFile, systemStoredProcLines],
                [defaultStoredProcsFile, defaultStoredProcLines],
                [userStoredProcsFile, userStoredProcLines],
            ]
        }
        // Move contents of the various files into memory
        fileLinesPairs.each { file, lines -> lines.addAll(getFileLines(file, '#', false, (file == sqlQueriesFile ? '}' : ''))) }
    }

    /**
     * Creates a table with the specified name, if that table does not already
     * exist in the database, and with the column names and types found in the
     * PARTITIONED_TABLE and REPLICATED_TABLE, in the usual test app (which is
     * based on an old version of 'genqa').
     * @param sqp - the SqlQueryPage on which to create the table.
     * @param tableName - the name of the table to be created (if necessary).
     * @param partitionColumn - the name of the table to be created (if necessary).
     * @return true if the table needed to be created.
     */
    def boolean createTableIfDoesNotExist(SqlQueryPage sqp, String tableName, String partitionColumn='') {
        if (getTables(sqp).contains(tableName)) {
            return false
        } else {
            String ddl = 'Create table ' + tableName + ' (\n' +
                    '  rowid                     BIGINT          NOT NULL,\n' +
                    '  rowid_group               TINYINT         NOT NULL,\n' +
                    '  type_null_tinyint         TINYINT,\n' +
                    '  type_not_null_tinyint     TINYINT         NOT NULL,\n' +
                    '  type_null_smallint        SMALLINT,\n' +
                    '  type_not_null_smallint    SMALLINT        NOT NULL,\n' +
                    '  type_null_integer         INTEGER,\n' +
                    '  type_not_null_integer     INTEGER         NOT NULL,\n' +
                    '  type_null_bigint          BIGINT,\n' +
                    '  type_not_null_bigint      BIGINT          NOT NULL,\n' +
                    '  type_null_timestamp       TIMESTAMP,\n' +
                    '  type_not_null_timestamp   TIMESTAMP       NOT NULL,\n' +
                    '  type_null_float           FLOAT,\n' +
                    '  type_not_null_float       FLOAT           NOT NULL,\n' +
                    '  type_null_decimal         DECIMAL,\n' +
                    '  type_not_null_decimal     DECIMAL         NOT NULL,\n' +
                    '  type_null_varchar25       VARCHAR(32),\n' +
                    '  type_not_null_varchar25   VARCHAR(32)     NOT NULL,\n' +
                    '  type_null_varchar128      VARCHAR(128),\n' +
                    '  type_not_null_varchar128  VARCHAR(128)    NOT NULL,\n' +
                    '  type_null_varchar1024     VARCHAR(1024),\n' +
                    '  type_not_null_varchar1024 VARCHAR(1024)   NOT NULL,\n' +
                    '  type_null_point           GEOGRAPHY_POINT,\n' +
                    '  type_not_null_point       GEOGRAPHY_POINT NOT NULL,\n' +
                    '  type_null_polygon         GEOGRAPHY,\n' +
                    '  type_not_null_polygon     GEOGRAPHY       NOT NULL,\n' +
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
     * Returns whether or not we are currently running the 'voter' example app,
     * based on whether the expected tables are listed on the page.
     * @param sqp - the SqlQueryPage from which to get the list of table names.
     * @return true if we are currently running the 'voter' example app.
     */
    static boolean isRunningVoter(SqlQueryPage sqp) {
        if (runningVoter == null) {
            runningVoter = getTables(sqp).containsAll(VOTER_TEST_TABLES)
        }
        return runningVoter
    }

    /**
     * Returns whether or not we are currently running the 'genqa' test app,
     * based on whether the expected tables and stored procedures are listed
     * on the page.
     * @param sqp - the SqlQueryPage from which to get the list of table names.
     * @return true if we are currently running the 'genqa' test app.
     */
    static boolean isRunningGenqa(SqlQueryPage sqp) {
        if (runningGenqa == null) {
            runningGenqa =
                    //getTables(sqp).containsAll(GENQA_TEST_TABLES) &&
                    sqp.getUserStoredProcedures().containsAll(GENQA_TEST_USER_STORED_PROCS)
        }
        return runningGenqa
    }

    /**
     * Runs, on the specified SqlQueryPage, and for each specified table or
     * view, a 'select * from ...' query, with an 'order by' and a limit 10'
     * clause. (Also, if DEBUG is true, prints: the table or view name, a list
     * of all column names, a list of all column types; and everything that
     * that runQuery prints, for each query.) (Note: streams are deliberately
     * omitted here, since you cannot query from a stream.)
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
                    } else if (columns.get(j).contains('geography_point')) {
                        query += (j > 0 ? ", " : "") + "PointFromText('POINT(-"+i+" "+i+")')"
                    } else if (columns.get(j).contains('geography')) {
                        query += (j > 0 ? ", " : "") + "PolygonFromText('POLYGON(("+(-i)+" "+(-i)+
                                 ", "+(-i+1)+" "+(-i)+", "+(-i)+" "+(-i+1)+", "+(-i)+" "+(-i)+"))')"
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
     * list (for the default test app).
     */
    def checkTables() {
        expect: 'List of displayed Tables should match expected list'
        printAndCompare('Tables', tablesFileName, true, tableLines, getTables(page))
    }

    /**
     * Check that the list of Streams displayed on the page matches the expected
     * list (for the default test app).
     */
    def checkStreams() {
        expect: 'List of displayed Streams should match expected list'
        printAndCompare('Streams', streamsFileName, true, streamLines, getStreams(page))
    }

    /**
     * Check that the list of Views displayed on the page matches the expected
     * list (for the default test app).
     */
    def checkViews() {
        expect: 'List of displayed Views should match expected list'
        printAndCompare('Views', viewsFileName, true, viewLines, getViews(page))
    }

    /**
     * Check that the list of System Stored Procedures displayed on the page
     * matches the expected list (for any app, not just the default one!).
     */
    def checkSystemStoredProcs() {
        expect: 'List of displayed System Stored Procedures should match expected list'
        printAndCompare('System Stored Procedures', SYSTEM_STORED_PROCS_FILE, true,
                systemStoredProcLines, page.getSystemStoredProcedures())
    }

    /**
     * Check that the list of Default Stored Procedures displayed on the page
     * matches the expected list (for the default test app).
     */
    def checkDefaultStoredProcs() {
        expect: 'List of displayed Default Stored Procedures should match expected list'
        printAndCompare('Default Stored Procedures', defaultStoredProcsFileName, true,
                defaultStoredProcLines, page.getDefaultStoredProcedures())
    }

    /**
     * Check that the list of User Stored Procedures displayed on the page
     * matches the expected list (for the default test app).
     */
    def checkUserStoredProcs() {
        expect: 'List of displayed User-defined Stored Procedures should match expected list'
        printAndCompare('User-defined Stored Procedures', userStoredProcsFileName, true,
                userStoredProcLines, page.getUserStoredProcedures())
    }

    /**
     * Takes a <i>parsedText</i> Map returned by the JSON slurper, and does two
     * things with it. First, if any keys of this Map start with "__", these
     * are interpreted as variable names, whose values are saved for later use
     * (in <i>sqlQueryVariables</i>). Second, the value of parsedText.sqlCmd
     * is returned, with any variable names resolved to their values.
     * @param parsedText - a Map returned by the JSON slurper.
     * @return the value of parsedText.sqlCmd, with any variable names resolved
     * to their values.
     */
    static String getQueryWithVariables(Map<String,Object> parsedText) {
        // Check for any variable definitions (starting with "__") and save
        // their values, to use in the current query or subsequent ones
        List<String> unresolvedVariableNames = []
        for (String key : parsedText.keySet()) {
            if (key.startsWith("__")) {
                Object value = resolveVariableValues(parsedText.get(key), false, true)
                sqlQueryVariables.put(key, value)
                if (value instanceof String && value.contains("__")) {
                    unresolvedVariableNames.add(key)
                }
            }
        }
        // For any variables that were defined using other variables that had
        // not yet been defined, resolve them now
        for (String key : unresolvedVariableNames) {
            sqlQueryVariables.put(key, resolveVariableValues(sqlQueryVariables.get(key)))
        }
        return (String) resolveVariableValues(parsedText.sqlCmd)
    }

    /**
     * Takes a <i>text</i> Object (usually, but not always, a String), and
     * returns it, with any variable names resolved to their values.
     * @param text - normally, the text to be searched, which is then returned
     * after resolving any unresolved variables; however, may also be a
     * non-String Object, in which case it is simply returned intact.
     * @param gettingResult - should be true when resolving variable values in
     * a result (as opposed to in a query), in which case, a single variable
     * may be used to represent a non-String Object (typically, a Map), which
     * will then be returned as said Object; when false, a String will always
     * be returned (assuming that <i>text</i> is a String).
     * @param ignoreUnknownVariables - when true, no WARNING message will be
     * printed, when an unkown variable is encountered (optional, default false).
     * @return the original <i>text</i>, with any variable names resolved to
     * their values.
     */
    static Object resolveVariableValues(Object text, boolean gettingResult=false,
                                        boolean ignoreUnknownVariables=false) {
        if (!(text instanceof String)) {
            // TODO: at some point, we might want to handle Maps differently,
            // to allow variables to be defined within a "result" Map; but
            // for now, that is not supported
            return text
        }
        String result = text
        // Used to prevent infinite loops via recursive variable definitions
        int maxCount = 100, count = 0
        for (Matcher variables = result =~ /(__\w+)/; count++ < maxCount && variables.find(); variables = result =~ /(__\w+)/ ) {
            String variable = variables.group(1)
            // Special case, for a result whose entire text consists of one variable
            if (gettingResult && variable.equals(text)) {
                return sqlQueryVariables.get(variable)
            }
            Object value = sqlQueryVariables.get(variable);
            if (value == null) {
                if (!ignoreUnknownVariables) {
                    println "\nWARNING: Unknown variable '"+variable+"'; so this query or result may fail:\n  " + result
                }
                break
            }

            result = result.replace(variable, value.toString())
        }
        if (count >= maxCount) {
            println "\nWARNING: this query or result contains an excessively nested, probably recursive, variable definition:\n  " +
                    text + " (-> " + result + " )"
        }
        return result
    }

    /**
     * Tests all the SQL queries specified in the sqlQueries.txt file.
     */
    @Unroll // performs this method for each test in the SQL Query text file
    def '#sqlQueriesTestName'() {

        setup: 'execute the next query (or queries)'
        runQuery(page, query)

        when: 'get the Query Result, and Expected Result'
        def qResult = page.getQueryResult()
        def expectedResult = resolveVariableValues(expectedResponse.result, true)

        debugPrint "\nquery         : " + query
        debugPrint "expect status : " + expectedResponse.status
        debugPrint "expect result : " + expectedResult
        if (expectedResponse.error != null) {
            debugPrint "expect error  : " + expectedResponse.error
        }
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
            println "\nWARNING: query duration '" + duration + "', for test '" +
                    sqlQueriesTestName + "' is null or empty!"
        }

        and: 'for a non-matching result, check if it is just a trim issue, and print details'
        if (qResult instanceof Map && expectedResult instanceof Map && qResult != expectedResult) {
            println "\nWARNING: query result does not match expected, for column(s):"
            boolean allDiffsCausedByTrim = true
            def expCols = expectedResult.keySet()
            def actCols = qResult.keySet()
            for (String col: expCols) {
                def expCol = expectedResult.get(col)
                def actCol = qResult.get(col)
                if (!expCol.equals(actCol)) {
                    println "  expected " + col + ": '" + expCol + "'"
                    println "  actual   " + col + ": '" + actCol + "'"
                    for (int i=0; i < expCol.size(); i++) {
                        if (actCol != null && expCol[i].trim().equals(actCol[i])) {
                            expCol[i] = actCol[i]
                        } else {
                            allDiffsCausedByTrim = false
                        }
                    }
                }
            }
            // Check for any columns that occur in the actual, but not expected, results
            for (String col: actCols) {
                def expCol = expectedResult.get(col)
                if (expCol == null) {
                    println "  expected " + col + ": '" + expCol + "'"
                    println "  actual   " + col + ": '" + qResult.get(col) + "'"
                    allDiffsCausedByTrim = false
                }
            }
            if (allDiffsCausedByTrim) {
                println "All these differences appear to be caused by Selenium calling trim() on the " +
                        "column values, so this test (" + sqlQueriesTestName + ") will likely pass."
            } else {
                println "There are real differences here, so this test (" + sqlQueriesTestName + ") will fail."
            }
        }

        then: 'check the query result, error status, and error message (if any)'
        expectedResult == qResult
        expectedResponse.status == status
        expectedResponse.error == null || (error != null && error.contains(expectedResponse.error))

        cleanup: 'delete all rows from the tables'
        runQuery(page, 'delete from partitioned_table;\ndelete from replicated_table')

        where: 'list of queries to test and expected responses'
        line << sqlQueryLines
        iter = slurper.parseText(line)
        sqlQueriesTestName = iter.testName
        query = getQueryWithVariables(iter)
        expectedResponse = iter.response
    }

}
