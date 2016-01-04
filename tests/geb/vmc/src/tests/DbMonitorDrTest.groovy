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
class DbMonitorDrTest extends TestBase {

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        int count = 0

        while (count < numberOfTrials) {
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

    def "Verify Show and Hide Database Replication (DR) table (MASTER)" (){

//        when:"Check showHideDiv is displayed"
//        if(waitFor(30){!page.isDrSectionOpen()})
//        {
//            println("Dr Replication didnot open in 30 seconds")
//        }
//        then:
//        println("proceed")
        when: "Check Dr Mode"
         page.dbDrMode.isDisplayed()
        then: "Dr Mode must be Master or Both"
        println("DR Mode" + page.dbDrMode.text())
        if(page.dbDrMode.text().equals("") || page.dbDrMode.text().equals("Replica"))
        {
            println("No Master mode")
        }
        else
        {
            println("Master mode")
            when: "ensure the DR section is open"
            if(!page.isDrSectionOpen())
            {
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

    def "Verify the Ascending and Descending in the Partition ID column of Database Replication (DR) table (MASTER)"(){
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"

            if(page.dbDrMode.text().equals("") || !waitFor(40){page.isDrMasterSectionOpen()})
            {
                println("Master section is not visible")

            }
            else {
                when: 'click Partition ID'

                waitFor(10){ page.clickPartitionID()}
                then: 'check if row count is in descending'
                if (waitFor(20){page.tableInDescendingOrderDT()})
                    after = "descending"
                else
                    after = "ascending"

                when: 'click Partition ID'
                waitFor(10){ page.clickPartitionID()}
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

        def "Verify the Ascending and Descending in the Status column of Database Replication (DR) table (MASTER)"(){
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"

            if(page.dbDrMode.text().equals("") || !waitFor(40){page.isDrMasterSectionOpen()})
            {
                println("Master section is not visible")

            }
            else {



                when: 'click Status'
                waitFor(10){ page.clickStatus()}
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Status'
                waitFor(10){ page.clickStatus()}
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

        def "Verify the Ascending and Descending in the MB On disk column of Database Replication (DR) table (MASTER)"(){
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"

            if(page.dbDrMode.text().equals("") ||!waitFor(40){page.isDrMasterSectionOpen()})
            {
                println("Master section is not visible")

            }
            else {



                when: 'click MB On disk'
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

        def "Verify the Ascending and Descending in the Replica Latency(ms) column of Database Replication (DR) table (MASTER)"(){
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"

            if(page.dbDrMode.text().equals("") ||!waitFor(40){page.isDrMasterSectionOpen()})
            {
                println("Master section is not visible")

            }
            else {



                when: 'click Replica Latency(ms)'
                waitFor(10){ page.clickReplicaLatencyMs()}
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replica Latency(ms)'
                waitFor(10){ page.clickReplicaLatencyMs()}
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

        def "Verify the Ascending and Descending in the replicaLatency (Transaction) column of Database Replication (DR) table (MASTER)"(){
            String before = ""
            String after  = ""
            when:"Check if Dr Master is Displayed"

            if(page.dbDrMode.text().equals("") ||!waitFor(40){page.isDrMasterSectionOpen()})
            {
                println("Master section is not visible")

            }
            else {



                when: 'click Replica Latency(Trans)'
                waitFor(10){ page.clickReplicaLatencyTrans()}
                then: 'check if row count is in ascending'
                if (page.tableInAscendingOrderDT())
                    before = "ascending"
                else
                    before = "descending"

                when: 'click Replica Latency(Trans)'
                waitFor(10){ page.clickReplicaLatencyTrans()}
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

    def "Verify Show and Hide Database Replication (DR) table (REPLICA)" (){

//        when:"Check showHideDiv is displayed"
//        if(waitFor(30){!page.isDrSectionOpen()})
//        {
//            println("Dr Replication didnot open in 30 seconds")
//        }
//        then:
//        println("proceed")
        when: "Check Dr Mode"
        page.dbDrMode.isDisplayed()
        then: "Dr Mode must be Master or Both"
        println("DR Mode" + page.dbDrMode.text())
        if(page.dbDrMode.text().equals("") || page.dbDrMode.text().equals("Master"))
        {
            println("No Replica mode")
        }
        else
        {
            println("Replica mode")
            when: "ensure the DR section is open"
            if(!page.isDrSectionOpen())
            {
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

    def "Verify the Ascending and Descending in the Server column of Database Replication (DR) table (REPLICA)"(){
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"

        if(page.dbDrMode.text().equals("") ||!waitFor(40){page.isDrReplicaSectionOpen()})
        {
            println("Replica section is not visible")

        }
        else {
            when: 'click Replica Server'
            waitFor(10){ page.clickReplicaServer()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click Replica Server'
            waitFor(10){ page.clickReplicaServer()}
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

    def "Verify the Ascending and Descending in the Status column of Database Replication (DR) table (REPLICA)"(){
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"

        if(page.dbDrMode.text().equals("") ||!waitFor(40){page.isDrReplicaSectionOpen()})
        {
            println("Replica section is not visible")

        }
        else {
            when: 'click Replica Status'
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

    def "Verify the Ascending and Descending in the Replication Rate(1 min) column of Database Replication (DR) table (REPLICA)"(){
        String before = ""
        String after  = ""
        when:"Check if Dr Master is Displayed"

        if(page.dbDrMode.text().equals("") ||!waitFor(40){page.isDrReplicaSectionOpen()})
        {
            println("Replica section is not visible")

        }
        else {
            when: 'click Replication Rate (1 min)'
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

    def "Verify the Ascending and Descending in the Replication Rate(5 min) column of Database Replication (DR) table (REPLICA)"() {
        String before = ""
        String after = ""
        when: "Check if Dr Master is Displayed"

        if (page.dbDrMode.text().equals("") || !waitFor(40) { page.isDrReplicaSectionOpen() }) {
            println("Replica section is not visible")

        } else {
            when: 'click Replication Rate (5 min)'
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

    def "Verify the text in the Title in Database Replication Table (MASTER) "(){
        when: "Check Master Title is displayed or not"
        if(page.drMasterTitleDisplayed())
        {
            assert true
        }
        then:
        if(page.drMasterTitleDisplayed()) {
            println(waitFor(20) { page.drMasterTitle.text() })
            page.drMasterTitle.text().equals("Master")
        }
        else
        {
            println("Master Section is not visible")
        }
    }

    def "Verify the text in the Title in Database Replication Table (REPLICA) "(){
        when: "Check Replica Title is displayed or not"
        if(page.drReplicaTitleDisplayed())
        {
            assert true
        }
        then:
        if(page.drReplicaTitleDisplayed()) {
            println(waitFor(20) { page.drReplicaTitle.text() })
            page.drReplicaTitle.text().equals("Replica")
        }
        else
        {
            println("Replica Section is not visible")
        }
    }


    def "Verify search in Database Replication table (MASTER)"(){
        boolean isValid=false
        String searchText = ""
        boolean isDROpen = false

       when: "Check if DR section is present"
        if(!page.isDrSectionOpen())
        {
            isDROpen = false
            println("Dr Replication is not present")
        }
        else
        {
            if(!page.isDrMasterSectionOpen())
            {
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
        else
        {
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

    def "Verify search in Database Replication table (REPLICA)"(){
        boolean isValid=false
        String searchText = ""
        boolean isDROpen = false

        when: "Check if DR section is present"
        if(!page.isDrSectionOpen())
        {
            isDROpen = false
            println("Dr Replication is not present")
        }
        else
        {
            if(!page.isDrReplicaSectionOpen())
            {
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
        else
        {
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