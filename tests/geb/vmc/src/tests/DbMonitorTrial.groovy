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
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage < totalPage) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.drTableNextMasterEnabled.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.drTableCurrentPageMaster.text())
                    }
                    expectedCurrentPage++
                }

                expectedCurrentPage = totalPage

                while(expectedCurrentPage >= 1) {
                    actualCurrentPage = Integer.parseInt(page.drTableCurrentPageMaster.text())
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage > 1) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.drTablePrevMasterEnabled.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.drTableCurrentPageMaster.text())
                    }
                    expectedCurrentPage--
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
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage < totalPage) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.drTableNextReplicaEnabled.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.drTableCurrentPageReplica.text())
                    }
                    expectedCurrentPage++
                }

                expectedCurrentPage = totalPage

                while(expectedCurrentPage >= 1) {
                    actualCurrentPage = Integer.parseInt(page.drTableCurrentPageReplica.text())
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage > 1) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.drTableNextReplicaEnabled.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.drTableCurrentPageReplica.text())
                    }
                    expectedCurrentPage--
                }
            }
            else {
                println("There are no multiple pages")
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
                    if(expectedCurrentPage < totalPage)
                        page.drTableNextReplicaEnabled.click()
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

    def VerifypaginationinCommandLogPerformancetable() {
        boolean CLPDisplayed = false
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify pagination in Command Log Performance table")
        if(page.showHideCLPBlock.isDisplayed()){
            CLPDisplayed = true

            String totalPageString = ""
            count = 0
            while(count<numberOfTrials) {
                count++
                try {
                    totalPageString = page.clpTotalPages.text()
                } catch(org.openqa.selenium.StaleElementReferenceException e) {
                } catch(geb.error.RequiredPageContentNotPresent e) {
                }

            }
            println("Total page string " + totalPageString)
            int totalPage = Integer.parseInt(totalPageString)
            int expectedCurrentPage = 1
            int actualCurrentPage = 1

            if(totalPage>1) {
                println("There are multiple pages")

                while(expectedCurrentPage <= totalPage) {
                    actualCurrentPage = Integer.parseInt(page.clpCurrentPage.text())
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage < totalPage) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.clpNextEnabled.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.clpCurrentPage.text())
                    }
                    expectedCurrentPage++
                }

                expectedCurrentPage = totalPage

                while(expectedCurrentPage >= 1) {
                    actualCurrentPage = Integer.parseInt(page.clpCurrentPage.text())
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage > 1) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.clpPrevEnabled.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.clpCurrentPage.text())
                    }
                    expectedCurrentPage--
                }
            }
            else {
                println("There are no multiple pages")
            }
        }
        else{
            println("CLP table is not visible")
        }

//        int count = 0
//        while(count<numberOfTrials) {
//            count ++
//            try {
//                waitFor(waitTime) { page.showHideCLPBlock.isDisplayed() }
//                println("Success")
//                break
//            } catch(geb.waiting.WaitTimeoutException e) {
//            }
//        }


        then:
        println("Test End: Verify pagination in Command Log Performance table")
    }

    def VerifypaginationinStoredProcedurestable() {
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify pagination in Stored Procedures table")
        if(page.showHideCLPBlock.isDisplayed()) {
            int count = 0
            String totalPageString = ""
            while(count<numberOfTrials) {
                count++
                try {
                    totalPageString = page.storedProcTotalPages.text()
                } catch(org.openqa.selenium.StaleElementReferenceException e) {
                } catch(geb.error.RequiredPageContentNotPresent e) {
                }
            }
            println("Total page " + totalPageString)
            int totalPage = Integer.parseInt(totalPageString)
            int expectedCurrentPage = 1
            int actualCurrentPage = 1

            if(totalPage>1) {
                println("There are multiple pages")

                while(expectedCurrentPage <= totalPage) {
                    actualCurrentPage = Integer.parseInt(page.storedProcCurrentPage.text())
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage < totalPage) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.storedProcNext.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.storedProcCurrentPage.text())
                    }
                    expectedCurrentPage++
                }

                expectedCurrentPage = totalPage

                while(expectedCurrentPage >= 1) {
                    actualCurrentPage = Integer.parseInt(page.storedProcCurrentPage.text())
                    if(actualCurrentPage != expectedCurrentPage) {
                        assert false
                    }
                    if(expectedCurrentPage > 1) {
                        count = 0

                        while(count<numberOfTrials) {
                            count++
                            try {
                                page.storedProcPrev.click()
                            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                            } catch(geb.error.RequiredPageContentNotPresent e) {
                            }
                        }
                        actualCurrentPage = Integer.parseInt(page.storedProcCurrentPage.text())
                    }
                    expectedCurrentPage--
                }
            }
            else {
                println("There are no multiple pages")
            }
        }
        else{
            println("CLP table is not visible")
        }
        then:
        println("Test End: Verify pagination in Stored Procedures table")

//        while(count<numberOfTrials) {
//            count ++
//            try {
//                waitFor(waitTime) { page.showHideData.isDisplayed() }
//                println("Success")
//                break
//            } catch(geb.waiting.WaitTimeoutException e) {
//            }
//        }



    }

    def "Verify pagination in Database table"() {
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify pagination in Database table")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.showHideData.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        String totalPageString = ""
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                totalPageString = page.databaseTableTotalPage.text()
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.error.RequiredPageContentNotPresent e) {
            }
        }
        println("Total page " + totalPageString)
        int totalPage = Integer.parseInt(totalPageString)
        int expectedCurrentPage = 1
        int actualCurrentPage = 1

        if(totalPage>1) {
            println("There are multiple pages")

            while(expectedCurrentPage <= totalPage) {
                actualCurrentPage = Integer.parseInt(page.databaseTableCurrentPage.text())
                if(actualCurrentPage != expectedCurrentPage) {
                    assert false
                }
                if(expectedCurrentPage < totalPage) {
                    count = 0

                    while(count<numberOfTrials) {
                        count++
                        try {
                            page.tablesNext.click()
                        } catch(org.openqa.selenium.StaleElementReferenceException e) {
                        } catch(geb.error.RequiredPageContentNotPresent e) {
                        }
                    }
                    actualCurrentPage = Integer.parseInt(page.databaseTableCurrentPage.text())
                }
                expectedCurrentPage++
            }

            expectedCurrentPage = totalPage

            while(expectedCurrentPage >= 1) {
                actualCurrentPage = Integer.parseInt(page.databaseTableCurrentPage.text())
                if(actualCurrentPage != expectedCurrentPage) {
                    assert false
                }
                if(expectedCurrentPage > 1) {
                    count = 0

                    while(count<numberOfTrials) {
                        count++
                        try {
                            page.tablesPrev.click()
                        } catch(org.openqa.selenium.StaleElementReferenceException e) {
                        } catch(geb.error.RequiredPageContentNotPresent e) {
                        }
                    }
                    actualCurrentPage = Integer.parseInt(page.databaseTableCurrentPage.text())
                }
                expectedCurrentPage--
            }
        }
        else {
            println("There are no multiple pages")
        }
        then:
        println("Test End: Verify pagination in Database table")

    }
}
