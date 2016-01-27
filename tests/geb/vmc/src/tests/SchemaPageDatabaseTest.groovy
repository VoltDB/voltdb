/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.junit.Test

import java.util.List;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class tests navigation between pages (or tabs), of the the VoltDB
 * Management Center (VMC), which is the VoltDB (new) web UI.
 */
class SchemaPageDatabaseTest extends TestBase {

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
    
    def "Size Worksheet Tab:Add table, search and delete"() {
        boolean result = false
        
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()
        
        // Changes for handling the pause
        when: 'go to Admin page'
        page.openAdminPage()
        then: 'at SQL Query page'
        at AdminPage
        
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }  
            println("Resume button is displayed") 
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }  
          
        if (result == false) {
            println("Resume VMC")
            
            try {
                page.cluster.resumebutton.click()
                waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume confirmation was not found")
                assert false
            }  
            
            try {
                page.cluster.resumeok.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause button was not found")
                assert false
            }
        }
        //
        
        when: 'go to SQL Query page'
        page.openSqlQueryPage()
        then: 'at SQL Query page'
        at SqlQueryPage

        when: 'set search query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'go to Schema page'
        page.openSchemaPage()
        then: 'at Schema page'
        at SchemaPage

        when: 'go to Size Worksheet Tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at Size Worksheet Tab'
        at SchemaPageSizeWorksheetTab

        when: 'tablename is searched'
        page.refreshbutton.click()
        //waitFor(30) { page.searchName.isDisplayed() }
        //page.searchName.value(tablename)
        then: 'at least one table is present'
        waitFor(30) { page.tablenamePresent.isDisplayed() }

        when: 'go to SQL Query page'
        page.openSqlQueryPage()
        then: 'at SQL Query page'
        at SqlQueryPage

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()

        when: 'go to Schema page'
        page.openSchemaPage()
        then: 'at Schema page'
        at SchemaPage

        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'tablename is searched'
        page.refreshbutton.click()
        //waitFor(30) { page.searchName.isDisplayed() }
        //page.searchName.value(tablename)
        then: 'at least one table is present'
        waitFor(30) { !page.tablenamePresent.isDisplayed() }

        if(!page.tablenamePresent.isDisplayed()) {
            println("Size Worksheet Tab:Add table, search and delete-PASS")
            println()
        }
        else {
            println("Size Worksheet Tab:Add table, search and delete-FAIL")
            println()
            assert false
        }
        
        when:
        if (result == false) {
            println("Pause VMC")
            
            page.openAdminPage()
            at AdminPage
            
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause confirmation was not found")
                assert false
            }  
            
            try {
                page.cluster.pauseok.click()
                waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume button was not found")
                assert false
            }
        }
        then:
        println()
    }
	
	def "Schema Tab:Add table, search and delete"() {
		boolean result = false
		
		when: 'go to schema tab'
		page.openSchemaPageSchemaTab()
		then: 'at schema tab'
		at SchemaPageSchemaTab
		
		String createQuery = page.getQueryToCreateTable()
		String deleteQuery = page.getQueryToDeleteTable()
		String tablename = page.getTablename()
				
		// Corrections
		when: 'go to Admin page'
        page.openAdminPage()
        then: 'at SQL Query page'
        at AdminPage
        
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }  
            println("Resume button is displayed") 
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }  
          
        if (result == false) {
            println("Resume VMC")
            
            try {
                page.cluster.resumebutton.click()
                waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume confirmation was not found")
                assert false
            }  
            
            try {
                page.cluster.resumeok.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause button was not found")
                assert false
            }
        }
        //
        
		when: 'go to SQL Query page'
		page.openSqlQueryPage()
		then: 'at SQL Query page'
		at SqlQueryPage
		
		when: 'set search query in the box'
		page.setQueryText(createQuery)
		then: 'run the query'
		page.runQuery()
		
		when: 'go to Schema page'
		page.openSchemaPage()
		then: 'at Schema page'
		at SchemaPage
		
		when: 'go to schema tab'
		page.openSchemaPageSchemaTab()
		then: 'at schema tab'
		at SchemaPageSchemaTab
		
		when: 'tablename is searched'
		page.refreshbutton.click()
		//waitFor(30) { page.searchName.isDisplayed() }
		//page.searchName.value(tablename)
		then: 'at least one table is present'
		waitFor(30) { page.requiredId.isDisplayed() }
		
		when: 'go to SQL Query page'
		page.openSqlQueryPage()
		then: 'at SQL Query page'
		at SqlQueryPage
		
		when: 'set delete query in the box'
		page.setQueryText(deleteQuery)
		then: 'run the query'
		page.runQuery()
		
		when: 'go to Schema page'
		page.openSchemaPage()
		then: 'at Schema page'
		at SchemaPage
		
		when: 'go to schema tab'
		page.openSchemaPageSchemaTab()
		then: 'at sschema tab'
		at SchemaPageSchemaTab
		
		when: 'tablename is searched'
		page.refreshbutton.click()
		//waitFor(30) { page.searchName.isDisplayed() }
		//page.searchName.value(tablename)
		then: 'at least one table is present'
		
		try {
			page.requiredId.isDisplayed()
			println("Schema Tab:Add table, search and delete-FAIL")
			assert false
		}
		catch (geb.error.RequiredPageContentNotPresent e) {
			println("Schema Tab:Add table, search and delete-PASS")
		}
		println()
		
		when:
        if (result == false) {
            println("Pause VMC")
            
            page.openAdminPage()
            at AdminPage
            
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause confirmation was not found")
                assert false
            }  
            
            try {
                page.cluster.pauseok.click()
                waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume button was not found")
                assert false
            }
        }
        then:
        println()
	}
	
    def cleanup() {
		if (!(page instanceof VoltDBManagementCenterPage)) {
            when: 'Open VMC page'
            ensureOnVoltDBManagementCenterPage()
            then: 'to be on VMC page'
            at VoltDBManagementCenterPage
        }

        page.loginIfNeeded()
        
        when: 'click the Schema link (if needed)'
        page.openSqlQueryPage()
        then: 'should be on DB Monitor page'
        at SqlQueryPage
        String deleteQuery = page.getQueryToDeleteTable()
		page.setQueryText(deleteQuery)

		page.runQuery()
	}
}
