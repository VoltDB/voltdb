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

//import geb.*
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import org.voltdb.fullddlfeatures.TestDDLFeatures
////import org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */
class FullDdlSqlTest extends TestBase {
    static final String FULL_DDL_FILE = '../../frontend/org/voltdb/fullddlfeatures/fullDDL.sql';

    @Shared def fullDdlFile = new File(FULL_DDL_FILE)
    @Shared def fullDdlLines = []
    @Shared def existingTables = []
    @Shared def existingViews = []
    @Shared def existingStoredProcs = []

    def setupSpec() { // called once, before any tests
        // TestBase.setup gets called first (automatically)

        // Make sure we're on the SQL Query page
        ensureOnSqlQueryPage()

        // Check which tables, views & (user) Stored Procs exist, before this
        // test (so we can delete the new ones, at the end)
        existingTables = page.getTableNames()
        existingViews  = page.getViewNames()
        existingStoredProcs = page.getUserStoredProcedures()
        debugPrint '\nExisting Tables:\n' + existingTables
        debugPrint '\nExisting Views:\n' + existingViews
        debugPrint '\nExisting User Stored Procedures:\n' + existingStoredProcs
    }

    def setup() { // called before each test
        when: 'click the SQL Query link (if needed)'
        ensureOnSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage
    }

    def cleanupSpec() { // called once, after all the tests
        // First, refresh the page, to ensure we have the latest data
        driver.navigate().refresh()
        ensureOnSqlQueryPage()

        // Next, get the lists of new tables, views & (user) Stored Procs,
        // created by this test
        def newTables = page.getTableNames()
        def newViews  = page.getViewNames()
        def newStoredProcs = page.getUserStoredProcedures()
        newTables.removeAll(existingTables)
        newViews.removeAll(existingViews)
        newStoredProcs.removeAll(existingStoredProcs)
        debugPrint '\nNew Tables (to be dropped??):\n' + newTables
        debugPrint '\nNew Views (to be dropped??):\n' + newViews
        debugPrint '\nNew User Stored Procedures (to be dropped??):\n' + newStoredProcs

        // Drop all the new tables, views & (user) Stored Procs that were
        // created by this test
//*
        newStoredProcs.each { runQuery(page, 'Drop procedure ' + it + ' if exists') }
        newViews.each { runQuery(page, 'Drop view ' + it + ' if exists') }
        newTables.each { runQuery(page, 'Drop table ' + it + ' if exists') }
//*/
    }
    
    def ensureOnSqlQueryPage() {
        ensureOnVoltDBManagementCenterPage()
        page.openSqlQueryPage()
    }

    /**
     * Runs, on the specified SqlQueryPage, the specified query, and returns
     * the result. (Also, if DEBUG is true, prints: the query, the result, an
     * error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param query - the query to be run.
     * @return the query result (as a Map of Lists of Strings).
     */
    def Map<String,List<String>> runQuery(SqlQueryPage sqp, String query) {
        sqp.runQuery(query)
        def qResult = sqp.getQueryResult()
        def qResText = sqp.getQueryResultText()

        if (qResText.contains("Connect to a datasource first")) {
            debugPrint "\nQuery : " + query
            debugPrint "Result Text: " + qResText
            debugPrint "Result: " + qResult
            debugPrint "Duration: " + sqp.getQueryDuration()
            debugPrint "Error : " + sqp.getQueryError()
            debugPrint "Reloading and trying again..."
            driver.navigate().refresh()
            if (!sqp.verifyAtSafely()) {
                debugPrint "  Moving to VoltDBManagementCenterPage"
                to VoltDBManagementCenterPage
                page.loginIfNeeded()
                debugPrint "  Opening SqlQueryPage"
                page.openSqlQueryPage()
            }
            sqp.verifyAt()
            sqp.runQuery(query)
            qResult = sqp.getQueryResult()
        }

        // If 'sleepSeconds' property is set and greater than zero, sleep
        int sleepSeconds = getIntSystemProperty("sleepSeconds", 0)
        if (sleepSeconds > 0) {
            try {
                Thread.sleep(1000 * sleepSeconds)
            } catch (InterruptedException e) {
                println "\nIn FullDdlSqlTest.runQuery, caught:\n  " + e.toString() + "\nSee standard error for stack trace."
                e.printStackTrace()
            }
        }

        debugPrint "\nQuery : " + query
        debugPrint "Result: " + qResult
        debugPrint "Duration: " + sqp.getQueryDuration()
        def error = sqp.getQueryError()
        if (error != null) {
            debugPrint "Error : " + error
        }

        return qResult
    }

    /**
     * TODO
     */
    def runFullDdlSqlFile() {
        // Get the lines and commands of the fullDDL.sql file
        def lines = getFileLines(fullDdlFile, '--')
//*
        def commands = []
        int startCommandAtLine = 0
        lines.eachWithIndex { line, i -> if (line.trim().endsWith(';')) {
            def command = lines[startCommandAtLine]
            for (int j=startCommandAtLine+1; j <= i; j++) {
                    command += '\n' + lines[j]
            }
            commands.add(command)
            startCommandAtLine = i + 1
        }}

        // Execute each (DDL) SQL command
        commands.each {
            runQuery(page, it)
            def qResults = page.getQueryResults(/*ColumnHeaderCase.AS_IS*/)
            def error    = page.getQueryError()
            def duration = page.getQueryDuration()
            debugPrint '\nDDL Command:\n' + it
            debugPrint '\nResult(s):\n' + qResults
            if (error != null) {
                debugPrint "Error:\n" + error
            }
            debugPrint 'Duration: ' + duration
        }
//*/
        // Do validation that the (DDL) SQL commands worked
        TestDDLFeatures tdf = new TestDDLFeatures();
        tdf.init()
        //tdf.initClient()

        // TODO
        Method[] methods = tdf.getClass().getMethods()
        for (Method method : methods) {
            Annotation[] annotations = method.getAnnotations()
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(org.junit.Test.class)) {
                    if (!['testCreateProcedureFromClass','testAlterTableDropConstraint'].contains(method.getName())) {
                        println "\nInvoking Method: " + method.getName()
                        method.invoke(tdf)
                    } else {
                        println "\nSkipping Method: " + method.getName()
                    }
                }
            }
        }
/*
        tdf.testCreateUniqueIndex()
        tdf.testCreateAssumeUniqueIndex()
        tdf.testCreateHashIndex()
        
        //tdf.testCreateProcedureAsSQLStmt()
        //tdf.testCreateProcedureFromClass()
        //tdf.testCreateTableDataType()
        
        tdf.testCreateTableConstraint()
        tdf.testCreateView()
        tdf.testExportTable()
        tdf.testPartitionProcedure()
        tdf.testDropView()
        tdf.testDropIndex()
        tdf.testDropProcedure()
        tdf.testDropTable()
        //tdf.testAlterTableDropConstraint()
        tdf.testAlterTableAddConstraint()
        tdf.testAlterTableAddColumn()
        tdf.testAlterTableDropColumn()
        tdf.testAlterTableAlterColumn()
*/
        expect: 'TODO: just a temp placeholder'
        lines.size() == 687
    }

}
