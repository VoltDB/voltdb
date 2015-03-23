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
////import org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc
import spock.lang.*
import vmcTest.pages.SqlQueryPage
import vmcTest.pages.VoltDBManagementCenterPage.ColumnHeaderCase

/**
 * This class tests the 'fullDDL.sql' file in the VMC, by running its queries
 * on the 'SQL Query' tab of the VoltDB Management Center (VMC) page, and
 * validating the results.
 */
class FullDdlSqlTest extends SqlQueriesTestBase {
    static final String FULL_DDL_FILE = '../../frontend/org/voltdb/fullddlfeatures/fullDDL.sql';

    @Shared def fullDdlFile = new File(FULL_DDL_FILE)
    @Shared def fullDdlLines = []
    @Shared def existingTables = []
    @Shared def existingViews = []
    @Shared def existingStoredProcs = []
    @Shared def newExportTables = []
    @Shared def newRoles = []
    @Shared def errors = [:]

    @Shared def ignoreTheseTestMethods = ['testCreateProcedureFromClass']
    
    def setupSpec() { // called once, before any tests
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

    def cleanupSpec() { // called once, after all the tests
        // Make sure we're on the SQL Query page
        ensureOnSqlQueryPage()

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

        // Drop all the new tables, views & (user) Stored Procs that were
        // created by this test
        newStoredProcs.each { runQuery(page, 'Drop procedure ' + it + ' if exists') }
        newViews.each { runQuery(page, 'Drop view ' + it + ' if exists') }
        newTables.each { runQuery(page, 'Drop table ' + it + ' if exists') }
        newExportTables.each { runQuery(page, 'Drop table ' + it + ' if exists') }
        newRoles.each { runQuery(page, 'Drop role ' + it + ' if exists') }
    }

    /**
     * TODO
     */
    private boolean isValidNameChar(char c) {
        if (c >= 'A' && c <= 'Z') {
            return true
        } else if (c >= 'a' && c <= 'z') {
            return true
        } else if (c >= '0' && c <= '9') {
            return true
        } else if (c == '$' || c == '_') {
            return true
        } else {
            return false
        }
    }
    
    /**
     * TODO
     */
    private String getName(String command, String afterThis) {
        println '\nFound a ' + afterThis + ' command...'
        println '  ' + command
        int start = command.toUpperCase().indexOf(afterThis) + afterThis.length()
        int end = command.length();
        for (int i=start; i < end; i++) {
            if (isValidNameChar(command.charAt(i))) {
                start = i
                break
            }
        }
        for (int i=start; i < command.length(); i++) {
            if (!isValidNameChar(command.charAt(i))) {
                end = i
                break
            }
        }
        println '  start, end: ' + start + ', ' + end
        println '  substring : ' + command.substring(start, end)
        return command.substring(start, end)
    }

    /**
     * TODO
     */
    def runFullDdlSqlFile() {
        // Get the lines of the fullDDL.sql file (ignoring comment lines
        // starting with '--', and blank lines)
        def lines = getFileLines(fullDdlFile, '--', false)

        // Break the lines into commands, based on ending with ';'
        def commands = []
        int startCommandAtLine = 0
        lines.eachWithIndex { line, i ->
            if (line.trim().endsWith(';')) {
                def command = lines[startCommandAtLine]
                for (int j=startCommandAtLine+1; j <= i; j++) {
                    command += '\n' + lines[j]
                }
                // Kludge for ENG-7869 (for named LIMIT PARTITION ROWS commands)
                if (command.contains('DROP CONSTRAINT lpr39A')) {
                    println '\nReplacing command:\n' + command
                    command = command.replace('DROP CONSTRAINT lpr39A', 'DROP LIMIT PARTITION ROWS')
                    println 'With command:\n' + command
                }
                commands.add(command)
                startCommandAtLine = i + 1
            }
        }

        // Execute each (DDL) SQL command
        commands.each {
            String commandUpperCase = it.toUpperCase()

            // TODO
            if (commandUpperCase.contains('CREATE') && commandUpperCase.contains('PROCEDURE') &&
                commandUpperCase.contains('FROM') && commandUpperCase.contains('CLASS')) {
                println '\nSkipping command:\n' + it
            } else {
                def qResults = runQuery(page, it, ColumnHeaderCase.AS_IS)
            }

            // Keep track of CREATE ROLE commands
            if (commandUpperCase.contains('CREATE') && commandUpperCase.contains('ROLE')) {
                newRoles.add(getName(it, 'ROLE'))
            // Keep track of EXPORT TABLE commands
            } else if (commandUpperCase.contains('EXPORT') && commandUpperCase.contains('TABLE')) {
                newExportTables.add(getName(it, 'TABLE'))
            }
        }

        // Do validation that the (DDL) SQL commands worked, by running the
        // JUnit tests of the TestDDLFeatures class
        TestDDLFeatures tdf = new TestDDLFeatures();
        tdf.startClient()
        Method[] methods = tdf.getClass().getMethods()
        for (Method method : methods) {
            Annotation[] annotations = method.getAnnotations()
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(org.junit.Test.class)) {
                    if (ignoreTheseTestMethods.contains(method.getName())) {
                        debugPrint '\nSkipping JUnit Method: TestDDLFeatures.' + method.getName()
                    } else {
                        debugPrint '\nInvoking JUnit Method: TestDDLFeatures.' + method.getName()
                        try {
                            method.invoke(tdf)
                        } catch (InvocationTargetException e) {
                            errors.put(method.getName(), e.toString() + ':\n' + e.getCause()?.toString())
                            println 'Caught InvocationTargetException, running TestDDLFeatures.' + method.getName() + ':'
                            e.printStackTrace(System.out);
                        }
                    }
                }
            }
        }

        // List any errors that were found
        for (String key : errors.keySet()) {
            println '\nAction that caused Error:\n' + key
            println '\nError:\n' + errors.get(key)
        }

        expect: 'There should be no errors'
        errors.isEmpty()
    }

}
