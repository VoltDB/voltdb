/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.List;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class tests navigation between pages (or tabs), of the the VoltDB
 * Management Center (VMC), which is the VoltDB (new) web UI.
 */
class SchemaPageTest extends TestBase {

    static final String DDL_SOURCE_FILE = 'src/resources/expectedDdlSource.txt';
    static Boolean runningGenqa = null;
    @Shared def ddlSourceFile = new File(DDL_SOURCE_FILE)
    @Shared def ddlExpectedSourceLines = []
    //@Shared def fileLinesPairs = [ [ddlSourceFile, ddlExpectedSourceLines] ]
    //@Shared def slurper = new JsonSlurper()

    def setupSpec() { // called once, before any tests
        // Move contents of the specified file into memory
        ddlExpectedSourceLines = getFileLines(ddlSourceFile)
        //fileLinesPairs.each { file, lines -> lines.addAll(getFileLines(file)) }
    }

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage
    }

    /**
     * Returns whether or not we are currently running the 'genqa' test app,
     * based on whether the expected DDL Source is listed on the page.
     * @param spdst - the SchemaPageDdlSourceTab from which to get the DDL source.
     * @return true if we are currently running the 'genqa' test app.
     */
    static boolean isRunningGenqa(SchemaPageDdlSourceTab spdst) {
        if (runningGenqa == null) {
            def ddlSource = spdst.getDdlSource()
            runningGenqa = true
            for (table in ['EXPORT_MIRROR_PARTITIONED_TABLE', 'PARTITIONED_TABLE', 'REPLICATED_TABLE']) {
                if (!ddlSource.contains(table)) {
                    runningGenqa = false
                    break
                }
            }
        }
        return runningGenqa
    }

    def 'confirm Overview tab open initially'() {
        expect: 'Overview tab open initially'
        page.isSchemaPageOverviewTabOpen()
    }

    def navigateSchemaPageTabs() {
        when: 'click the Schema (sub-)link (from Overview tab)'
        page.openSchemaPageSchemaTab()
        then: 'should be on Schema tab (of Schema page)'
        at SchemaPageSchemaTab

        when: 'click the Procedures & SQL link (from Schema tab)'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'should be on Procedures & SQL tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click the Size Worksheet link (from Procedures & SQL tab)'
        page.openSchemaPageSizeWorksheetTab()
        then: 'should be on Size Worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click the DDL Source link (from Size Worksheet tab)'
        page.openSchemaPageDdlSourceTab()
        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'click the Overview link (from DDL Source tab)'
        page.openSchemaPageOverviewTab()
        then: 'should be on Overview tab'
        at SchemaPageOverviewTab

        when: 'click the DDL Source link (from Overview tab)'
        page.openSchemaPageDdlSourceTab()
        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'click the Size Worksheet link (from DDL Source tab)'
        page.openSchemaPageSizeWorksheetTab()
        then: 'should be on Size Worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click the Procedures & SQL link (from Size Worksheet tab)'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'should be on Procedures & SQL tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click the Schema (sub-)link (from Procedures & SQL tab)'
        page.openSchemaPageSchemaTab()
        then: 'should be on Schema tab (of Schema page)'
        at SchemaPageSchemaTab

        when: 'click the Overview link (from Schema tab)'
        page.openSchemaPageOverviewTab()
        then: 'should be on Overview tab'
        at SchemaPageOverviewTab
    }

    /**
     * Check that the DDL Source displayed on the DDL Source tab matches the
     * expected list (for the 'genqa' test app).
     */
    def checkSchemaTab() {
        when: 'click the Schema tab link'
        page.openSchemaPageSchemaTab()

        then: 'should be on Schema tab'
        at SchemaPageSchemaTab

        // TODO: should make these into tests, not just debug print
        cleanup: 'for now, just print the table, row-wise'
        debugPrint "\nSchema (tab) table, row-wise:\n" + page.getSchemaTableByRow()

        and: 'for now, just print the table, column-wise'
        debugPrint "\nSchema (tab) table, column-wise:\n" + page.getSchemaTableByColumn()
    }

    /**
     * Check that the DDL Source displayed on the DDL Source tab matches the
     * expected list (for the 'genqa' test app).
     */
    def checkDdlSourceTab() {
        when: 'click the DDL Source tab link'
        page.openSchemaPageDdlSourceTab()

        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'get the DDL Source (lines of text), from the DDL Source tab'
        List<String> ddlActualSourceLines = page.getDdlSourceLines()

        then: 'test & remove the first few lines, which are descriptive text'
        ddlActualSourceLines.remove(0).startsWith('-- This file was generated by VoltDB version 5.')
        ddlActualSourceLines.remove(0).equals('-- This file represents the current database schema.')
        ddlActualSourceLines.remove(0).equals('-- Use this file as input to reproduce the current database structure in another database instance.')
        ddlActualSourceLines.remove(0).isEmpty()

        and: 'DDL Source should match expected text'
        printAndCompare('DDL Source', DDL_SOURCE_FILE, isRunningGenqa(page), ddlExpectedSourceLines, ddlActualSourceLines)
    }
}
