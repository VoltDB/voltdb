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

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
                try {
                    waitFor(waitTime) { 1 == 0 }
                } catch (geb.waiting.WaitTimeoutException exception) {

                }
            }
        }
    }

    def checkClusterId() {
        when:
        page.clusterId.isDisplayed()
        then:
        println(page.clusterId.text())
    }

    def checkLatencyDr() {
        when: "Check Dr Mode"
        page.drMode.isDisplayed()
        then: "Dr Mode must be Master or Both"
        println("DR Mode" + page.drMode.text())
        if (page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
            println("No Master mode")
        }
        else {
            page.latencyTime.isDisplayed()
        }
    }

    def verifyShowAndHideDatabaseReplicationTableMASTER() {
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
            if (!page.isDrSectionOpen()) {
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
    }

    def verifyTheAscendingAndDescendingInThePartitionIdColumnOfDatabaseReplicationTableMASTER() {
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"
        if (page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
            println("Master section is not visible")
        }
        else {
            when: 'click Partition ID'
            waitFor(20) { page.isDrMasterSectionOpen() }
            waitFor(10) { page.clickPartitionID() }
            then: 'check if row count is in descending'
            if (waitFor(20) { page.tableInDescendingOrderDT() })
                after = "descending"
            else
                after = "ascending"

            when: 'click Partition ID'
            waitFor(10) { page.clickPartitionID() }
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
    }

    def verifyTheAscendingAndDescendingInMbOnDiskColumnOfDatabaseReplicationTableMASTER() {
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"
        if(page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
            println("Master section is not visible")
        }
        else {
            when: 'click MB On disk'
            waitFor(20){page.isDrMasterSectionOpen()}
            waitFor(10){ page.clickMbOnDisk()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click MB On disk'
            waitFor(10){ page.clickMbOnDisk()}
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
    }

    def verifyTheAscendingAndDescendingInTheReplicaLatencyColumnOfDatabaseReplicationTableMASTER() {
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"
        if(page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
            println("Master section is not visible")
        }
        else {
            when: 'click Replica Latency(ms)'
            waitFor(20){page.isDrMasterSectionOpen()}
            waitFor(10) { page.clickReplicaLatencyMs() }
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click Replica Latency(ms)'
            waitFor(10){ page.clickReplicaLatencyMs()}
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
    }

    def verifyTheAscendingAndDescendingInReplicaLatencyColumnOfDatabaseReplicationTableMASTER() {
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"
        if(page.drMode.text().equals("") || page.drMode.text().equals("Replica")) {
            println("Master section is not visible")
        }
        else {
            when: 'click Replica Latency(Trans)'
            waitFor(20){page.isDrMasterSectionOpen()}
            waitFor(10) { page.clickReplicaLatencyTrans() }
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click Replica Latency(Trans)'
            waitFor(10) { page.clickReplicaLatencyTrans() }
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
    }

    def verifyTheTextInTheTitleInDatabaseReplicationTableMASTER() {
        when: "Check Master Title is displayed or not"
        if(page.drMasterTitleDisplayed()) {
            assert true
        }
        then:
        if(page.drMasterTitleDisplayed()) {
            println(waitFor(20) { page.drMasterTitle.text() })
            page.drMasterTitle.text().equals("Master")
        }
        else {
            println("Master Section is not visible")
        }
    }

    def verifySearchInDatabaseReplicationTableMASTER() {
        boolean isValid=false
        String searchText = ""
        boolean isDROpen = false

        when: "Check if DR section is present"
        if(!page.isDrSectionOpen()) {
            isDROpen = false
            println("Dr Replication is not present")
        }
        else {
            if(!page.isDrMasterSectionOpen()) {
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
            if (page.partitionIdRows.size() > 1) {
                searchText = page.partitionIdRows[0].text().substring(0, 1)
                page.filterPartitionId.value(page.partitionIdRows[0].text().substring(0, 1))
            } else {
                isValid = false
                assert true
            }
            then: "check the table"
            for (def i = 0; i <= page.partitionIdRows.size() - 1; i++) {
                if (!page.partitionIdRows[i].text().toString().contains(searchText)) {
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
    }

    def verifyShowAndHideDatabaseReplicationTableREPLICA() {
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
    }

    def verifyTheAscendingAndDescendingInServerColumnOfDatabaseReplicationTableREPLICA() {
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
            report '111'
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
    }

    def verifyTheAscendingAndDescendingInStatusColumnOfDatabaseReplicationTableREPLICA() {
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
    }

    def verifyTheAscendingAndDescendingInReplicationRate1MinColumnOfDatabaseReplicationTableREPLICA() {
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
    }

    def verifyTheAscendingAndDescendingInTheReplicationRate5MinColumnOfDatabaseReplicationTableREPLICA() {
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
    }

    def verifyTheTextInTheTitleInDatabaseReplicationTableREPLICA() {
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
    }

    def verifySearchInDatabaseReplicationTableREPLICA() {
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
    }

}