/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import geb.Page.*

class DrTest extends TestBase {
    def isDrTabVisible = false

    def setup() { // called before each test
        int count = 0

        while(count<2) {
            count ++
            try {
                setup: 'Open VMC page'
                to VoltDBManagementCenterPage
                expect: 'to be on VMC page'
                at VoltDBManagementCenterPage

                when: 'click the DR link (if needed)'
                page.openDrPage()
                then: 'should be on DR page'
                at DrPage

                isDrTabVisible = true
                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
                try {
                    waitFor(waitTime) { 1 == 0 }
                } catch (geb.waiting.WaitTimeoutException exception) {

                }
                isDrTabVisible = false
            }
        }
    }
    def divId = "1_3"
    def checkClusterId() {
        when:
        println("Process only if DR is present")
        divId = $('#drProducerId').jquery.text()
        then:
        if(isDrTabVisible) {
            when:
            waitFor(10){drCLusterId.isDisplayed()}
            then:
            println(page.drCLusterId.text())
        } else {
            println("DR is not available.")
        }
    }

    def checkLatencyDr() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
           when: "Check Dr Mode"
           page.drMode.isDisplayed()
           then: "Dr Mode must be Master or Both"
           println("DR Mode " + page.drMode.text())
           if (page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
               println("No Master mode")
           } else {
               $('#latencyDR_' + divId).isDisplayed()
           }
        } else {
           println("DR is not available.")
        }
    }

    def verifyShowAndHideDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            when: "Check Dr Mode"
            page.drMode.isDisplayed()
            then: "Dr Mode must be Master or Both"
            println("DR Mode" + page.drMode.text())
            if (page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
                println("No Master mode")
            }
            else {
                println("Master mode")
                when: "ensure the DR section is open"
                if (!page.isDrSectionOpen(divId)) {
                   waitFor(30){ page.openDrArea(divId)}
                }
                then: 'DR area is open (initially)'
                page.isDrAreaOpen(divId)

                when: 'click Show/Hide Graph (to close)'
                page.closeDrArea(divId)
                then:'Dr area is closed'
                !page.isDrAreaOpen(divId)

                when:'click Show/Hide Graph (to open)'
                page.openDrArea(divId)
                then:'Dr area is open (again)'
                page.isDrAreaOpen(divId)

                when: 'click Show/Hide DR (to close again)'
                page.closeDrArea(divId)
                then: 'Graph area is closed (again)'
                !page.isDrAreaOpen(divId)
            }
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInThePartitionIdColumnOfDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"
            if (page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
                println("Master section is not visible")
            }
            else {
                when: 'click Partition ID'
                waitFor(20) { page.isDrMasterSectionOpen(divId) }
                waitFor(10) { page.clickPartitionID(divId) }
                then: 'check if row count is in descending'
                if (waitFor(20) { page.tableInDescendingOrderDT() })
                    after = "descending"
                else
                    after = "ascending"

                when: 'click Partition ID'
                waitFor(10) { page.clickPartitionID(divId) }
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInMbOnDiskColumnOfDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"
            if(page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
                println("Master section is not visible")
            }
            else {
                when: 'click MB On disk'
                waitFor(20){page.isDrMasterSectionOpen(divId)}
                waitFor(10){ page.clickMbOnDisk(divId)}
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click MB On disk'
                waitFor(10){ page.clickMbOnDisk(divId)}
                then: 'check if row count is in descending'
                if (waitFor(20){page.tableInDescendingOrderDT()})
                    after = "descending"
                else
                    after = "ascending"

                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInTheReplicaLatencyColumnOfDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"
            if(page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
                println("Master section is not visible")
            }
            else {
                when: 'click Replica Latency(ms)'
                waitFor(20){page.isDrMasterSectionOpen(divId)}
                waitFor(10) { page.clickReplicaLatencyMs(divId) }
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replica Latency(ms)'
                waitFor(10){ page.clickReplicaLatencyMs(divId)}
                then: 'check if row count is in descending'
                if (waitFor(20) { page.tableInDescendingOrderDT() })
                    after = "descending"
                else
                    after = "ascending"

                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInReplicaLatencyColumnOfDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"
            if(page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
                println("Master section is not visible")
            }
            else {
                when: 'click Replica Latency(Trans)'
                waitFor(20){page.isDrMasterSectionOpen(divId)}
                waitFor(10) { page.clickReplicaLatencyTrans(divId) }
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replica Latency(Trans)'
                waitFor(10) { page.clickReplicaLatencyTrans(divId) }
                then: 'check if row count is in descending'
                if (waitFor(20) { page.tableInDescendingOrderDT() })
                    after = "descending"
                else
                    after = "ascending"
                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheTextInTheTitleInDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            when: "Check Master Title is displayed or not"
            if(page.drMasterTitleDisplayed(divId)) {
                assert true
            }
            then:
            if(page.drMasterTitleDisplayed(divId)) {
                println(waitFor(20) { $("#drMasterTitle_" + divId).text() })
                $("#drMasterTitle_" + divId).text().equals("Master")
            }
            else {
                println("Master Section is not visible")
            }
        } else {
            println("DR is not available.")
        }
    }

    def verifySearchInDatabaseReplicationTableMASTER() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            boolean isValid=false
            String searchText = ""
            boolean isDROpen = false

            when: "Check if DR section is present"
            if(!page.isDrSectionOpen(divId)) {
                isDROpen = false
                println("Dr Replication is not present")
            }
            else {
                if(!page.isDrMasterSectionOpen(divId)) {
                    println("Dr MAster section is not present")
                    isDROpen = false
                }
                else {
                    isDROpen = true
                }
            }
            then:
            println("proceed")

            when: "Set the value of Master filter"
            if(isDROpen==true) {
                if ($("#tblDrMAster_" + divId).find(class:"sorting_1").size() > 1) {
                    searchText = $("#tblDrMAster_" + divId).find(class:"sorting_1")[0].text().substring(0, 1)
                    $("#filterPartitionId_" + divId).value($("#tblDrMAster_" + divId).find(class:"sorting_1")[0].text().substring(0, 1))
                } else {
                    isValid = false
                    assert true
                }
                then: "check the table"
                for (def i = 0; i <= $("#tblDrMAster_" + divId).find(class:"sorting_1").size() - 1; i++) {
                    if (!$("#tblDrMAster_" + divId).find(class:"sorting_1")[i].text().toString().contains(searchText)) {
                        println("test false")
                        isValid = false
                        break
                    } else {
                        isValid = true
                    }
                }
            }
            else {
                isValid=true
            }
            then:
            println("proceed")
            if (isValid == true) {
                assert true;
            } else {
                assert false;
            }
        } else {
            println("DR is not available.")
        }
    }

    def verifyShowAndHideDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            when: "Check Dr Mode"
            page.drMode.isDisplayed()
            then: "Dr Mode must be Master or Both"
            println("DR Mode" + page.drMode.text())
            if(page.drMode.text().equals("") || page.drMode.text().equals("Master")) {
                println("No Replica mode")
            }
            else {
                println("Replica mode")
                when: "ensure the DR section is open"
                if(!page.isDrSectionOpen()) {
                    waitFor(30){ page.openDrArea()}
                }
                then: 'DR area is open (initially)'
                page.isDrAreaOpen()

                when: 'click Show/Hide Graph (to close)'
                page.closeDrArea()
                then:'Dr area is closed'
                !page.isDrAreaOpen()

                when:'click Show/Hide Graph (to open)'
                page.openDrArea()
                then:'Dr area is open (again)'
                page.isDrAreaOpen()

                when: 'click Show/Hide DR (to close again)'
                page.closeDrArea()
                then: 'Graph area is closed (again)'
                !page.isDrAreaOpen()
            }
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInServerColumnOfDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""

            when:"Check if Dr Replica is Displayed"
            if(page.drMode.text().equals("") || page.drMode.text().equals("Master")) {
                println("Replica section is not visible")
            }
            else {
                when: 'click Replica Server'
                waitFor(waitTime) { page.isDrReplicaSectionOpen() }
                waitFor(10){ page.clickReplicaServer() }
                then: 'check if row count is in ascending'
                if (replicaServer.attr("class") == "sorting_asc")
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replica Server'
                waitFor(10){ page.clickReplicaServer()}
                then: 'check if row count is in descending'
                if (replicaServer.attr("class") == "sorting_desc")
                    after = "descending"
                else
                    after = "ascending"

                if (before.equals("ascending") )
                    assert after.equals("descending")

                if (before.equals("descending") )
                    assert after.equals("ascending")
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInStatusColumnOfDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"
            if(page.drMode.text().equals("") || page.drMode.text().equals("Master")) {
                println("Replica section is not visible")
            }
            else {
                when: 'click Replica Status'
                waitFor(waitTime) { page.isDrReplicaSectionOpen() }
                waitFor(10){ page.clickReplicaStatus()}
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replica Status'
                waitFor(10){ page.clickReplicaStatus()}
                then: 'check if row count is in descending'
                if (waitFor(20){page.tableInDescendingOrderDT()})
                    after = "descending"
                else
                    after = "ascending"
                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInReplicationRate1MinColumnOfDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"
            if(page.drMode.text().equals("") || page.drMode.text().equals("Master")) {
                println("Replica section is not visible")
            }
            else {
                when: 'click Replication Rate (1 min)'
                waitFor(waitTime) { page.isDrReplicaSectionOpen() }
                waitFor(10){ page.clickReplicationRate1()}
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replication Rate (1 min)'
                waitFor(10){ page.clickReplicationRate1()}
                then: 'check if row count is in descending'
                if (waitFor(20){page.tableInDescendingOrderDT()})
                    after = "descending"
                else
                    after = "ascending"
                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheAscendingAndDescendingInTheReplicationRate5MinColumnOfDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            String before = ""
            String after = ""
            when: "Check if Dr Master is Displayed"
            if (page.drMode.text().equals("") || page.drMode.text().equals("Master") ) {
                println("Replica section is not visible")
            }
            else {
                when: 'click Replication Rate (5 min)'
                waitFor(40) { page.isDrReplicaSectionOpen() }
                waitFor(10) { page.clickReplicationRate5() }
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replication Rate (5 min)'
                waitFor(10) { page.clickReplicationRate5() }
                then: 'check if row count is in descending'
                if (waitFor(20) { page.tableInDescendingOrderDT() })
                    after = "descending"
                else
                    after = "ascending"
                if (before.equals("ascending") && after.equals("descending"))
                    assert true
                else
                    assert false
            }
            then:
            assert true
        } else {
            println("DR is not available.")
        }
    }

    def verifyTheTextInTheTitleInDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            when: "Check Replica Title is displayed or not"
            if(page.drReplicaTitleDisplayed()) {
                assert true
            }
            then:
            if(page.drReplicaTitleDisplayed()) {
                println(waitFor(20) { page.drReplicaTitle.text() })
                page.drReplicaTitle.text().equals("Replica")
            }
            else {
                println("Replica Section is not visible")
            }
        } else {
            println("DR is not available.")
        }
    }

    def verifySearchInDatabaseReplicationTableREPLICA() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            boolean isValid=false
            String searchText = ""
            boolean isDROpen = false

            when: "Check if DR section is present"
            if(!page.isDrSectionOpen()) {
                isDROpen = false
                println("Dr Replication is not present")
            }
            else {
                if(!page.isDrReplicaSectionOpen()) {
                    println("Dr Replica section is not present")
                    isDROpen = false
                }
                else {
                    isDROpen = true
                }
            }
            then:
            println("proceed")

            when: "Set the value of Master filter"
            if(isDROpen==true) {
                if (page.filterReplicaServerRows.size() > 1) {
                    searchText = waitFor(20){page.filterReplicaServerRows[0].text().substring(0, 1)}
                    page.filterHostID.value(page.filterReplicaServerRows[0].text().substring(0, 1))
                } else {
                    isValid = false
                    assert true
                }
                then: "check the table"
                for (def i = 0; i <= page.filterReplicaServerRows.size() - 1; i++) {
                    if (!page.filterReplicaServerRows[i].text().toString().contains(searchText)) {
                        println("test false")
                        isValid = false
                        break
                    } else {
                        isValid = true
                    }
                }
            }
            else {
                isValid=true
            }
            then:
            println("proceed")
            if (isValid == true) {
                assert true;
            } else {
                assert false;
            }
        } else {
            println("DR is not available.")
        }
    }

    def checkMinValueInDrGraphSeconds() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            if(page.drMode.text().equals("") || page.drMode.text().equals("Master")) {
                println("Replica section is not visible")
            }
            else {
                int count = 0
                when:
                // This loop is used to gain time.
                //        while(count<numberOfTrials) {
                //            count++
                //            page.chooseGraphView("Minutes")
                //            page.chooseGraphView("Minutes")
                //            page.chooseGraphView("Seconds")
                //            if(graphView.text().equals("")) {
                //                break
                //            }
                //        }
                count = 0
                then:
                String stringMax
                String stringMin

                while (count < numberOfTrials) {
                    count++
                    try {
                        waitFor(waitTime) {
                            page.chartDrMax.isDisplayed()
                        }
                        stringMax = page.chartDrMax.text()
                        stringMin = page.chartDrMin.text()
                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }

                String result = page.compareTime(stringMax, stringMin)

                if (result.equals("seconds")) {
                    println("The minimum value is " + stringMin + " and the time is in " + result)
                    assert true
                } else {
                    println("FAIL: It is not in seconds")
                    assert false
                }
            }
        } else {
            println("DR is not available.")
        }
    }

    def checkMaxValueInDrGraphSeconds() {
        when:
        println("Process only if DR is present")
        then:
        if(isDrTabVisible) {
            if(page.drMode.text().equals("") || page.drMode.text().equals("Master")) {
                println("Replica section is not visible")
            }
            else {
                int count = 0
                when:
                // This loop is used to gain time.
                //        while(count<numberOfTrials) {
                //            count++
                //            page.chooseGraphView("Minutes")
                //            page.chooseGraphView("Minutes")
                //            page.chooseGraphView("Seconds")
                //            if(graphView.text().equals("")) {
                //                break
                //            }
                //        }
                count = 0
                then:
                String stringMax
                String stringMin

                while (count < numberOfTrials) {
                    count++
                    try {
                        waitFor(waitTime) {
                            page.chartDrMax.isDisplayed()
                        }
                        stringMax = page.chartDrMax.text()
                        stringMin = page.chartDrMin.text()
                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }

                String result = page.compareTime(stringMax, stringMin)

                if (result.equals("seconds")) {
                    println("The maximum value is " + stringMax + " and the time is in " + result)
                    assert true
                } else {
                    println("FAIL: It is not in seconds")
                    assert false
                }
            }
        } else {
            println("DR is not available.")
        }
    }

}