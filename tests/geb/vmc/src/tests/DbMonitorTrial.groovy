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

import vmcTest.pages.*
import java.io.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This class contains tests of the 'DB Monitor' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */
class DbMonitorTrial extends TestBase {

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        int count = 0
		
		while(count<numberOfTrials) {
			try {
			    when: 'click the DB Monitor link (if needed)'
                page.openDbMonitorPage()
                then: 'should be on DB Monitor page'
                at DbMonitorPage
            break
			} catch (org.openqa.selenium.ElementNotVisibleException e) {
				println("ElementNotVisibleException: Unable to Start the test")
				println("Retrying")
			}
		}
    }

    def "Verify pagination in Database Replication table"() {
        expect: 'at DbMonitorPage'
        at DbMonitorPage
        
        when:
        println("Test Start: Verify pagination in Database Replication table")
        
        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.drTableBlock.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        
        println("The mode is " + page.drTableModeTypeText.text())
        
        if(page.drTableModeTypeText.text().equals("Master")) {
            println("The Current Mode is MASTER")
            
            String totalPageString = page.drTableTotalPagesMaster.text()
            println("Total page string " + totalPageString)
            int totalPage = Integer.parseInt(totalPageString)
            int expectedCurrentPage = 1
            int actualCurrentPage = 1
            
            if(totalPage>1) {
                println("There are multiple pages")
                
                while(expectedCurrentPage <= totalPage) {
                    actualCurrentPage = Integer.parseInt(page.drTableCurrentPageMaster.text())
                    actualCurrentPage == expectedCurrentPage
                    expectedCurrentPage++
                    println("Expected Current Page " + expectedCurrentPage)
                    if(expectedCurrentPage < totalPage)
                        page.drTableNextMasterEnabled.click()
                }
            }
            else {
                println("There is no multiple pages")
            }
		}
		else if(page.drTableModeTypeText.text().equals("Replica")) {
		    println("The Current Mode is REPLICA")
		    
		    int totalPage = Integer.parseInt(page.drTableTotalPagesReplica.text())
            int expectedCurrentPage = 1
            int actualCurrentPage = 1
            
            if(totalPage>1) {
                println("There are multiple pages")
                
                while(expectedCurrentPage <= totalPage) {
                    actualCurrentPage = Integer.parseInt(page.drTableCurrentPageReplica.text())
                    actualCurrentPage == expectedCurrentPage
                    expectedCurrentPage++
                    println("Expected Current Page " + expectedCurrentPage)
                    if(expectedCurrentPage < totalPage)
                        page.drTableNextReplicaEnabled.click()
                }
            }
            else {
                println("There is no multiple pages")
            }
		}
		else if(page.drTableModeTypeText.text().equals("Both")) {
		    println("The Current Mode is BOTH")
		    
		    int totalPage = Integer.parseInt(page.drTableTotalPages.text())
            int expectedCurrentPage = 1
            int actualCurrentPage = 1
            
            if(totalPage>1) {
                println("There are multiple pages")
                
                while(expectedCurrentPage <= totalPage) {
                    actualCurrentPage = Integer.parseInt(page.drTableCurrentPage.text())
                    actualCurrentPage == expectedCurrentPage
                    expectedCurrentPage++
                }
            }
            else {
                println("There is no multiple pages")
            }
            
            println("Test End: Verify pagination in Database Replication table")
		}
		else {
		    println("The Database Replication table is not visible")
		}
		then:
		println("Test End: Verify pagination in Database Replication table")
    }

/*
    def "Check"() {
        expect: 'at DbMonitorPage'
        at DbMonitorPage
        
        when:
        String str = page.header.help.text()
        then:
        println(str)
    }
    */
} 
