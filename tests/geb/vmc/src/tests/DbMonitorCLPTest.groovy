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
class DbMonitorCLPTest extends TestBase {

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


    def verifyShowAndHideInCommandLogPerformanceTable() {
        when: "ensure CLP section is present or not"
        println("test" +page.isCmdLogSectionOpen())
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        then:
        println("proceed")
        if( page.isCmdLogSectionOpen()) {
            println("CLP section is present")
            when: "ensure the CLP section is open"
            waitFor(30) { page.openCLPArea() }
            then: 'DR area is open (initially)'
            page.isCLPAreaOpen()

            when: 'click Show/Hide Graph (to close)'
            page.closeCLPArea()

            then: 'CLP area is closed'
            !page.isCLPAreaOpen()

            when: 'click Show/Hide Graph (to open)'
            page.openCLPArea()

            then: 'CLP area is open (again)'
            page.isCLPAreaOpen()

            when: 'click Show/Hide CLP (to close again)'
            page.closeCLPArea()
            then: 'CLP area is closed (again)'
            !page.isCLPAreaOpen()
        }
    }

    def verifyTheAscendingAndDescendingInTheCmdServerColumnOfCommandLogPerformanceTable() {
        String before = ""
        String after  = ""
        when:"Check if Command log is Displayed"
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        else {
            when: 'click cmd Server'
            waitFor(10){ page.clickCmdServer()}
            then: 'check if row count is in descending'
            if (waitFor(20){page.tableInDescendingOrderDT()})
                after = "descending"
            else
                after = "ascending"

            when: 'click cmd Server'
            waitFor(10){ page.clickCmdServer()}
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

    def verifyAscendingAndDescendingInThePendingBytesColumnOfCommandLogPerformanceTable() {
        String before = ""
        String after  = ""
        when:"Check if Command Log is Displayed"
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        else {
            when: 'click cmd PendingBytes'
            waitFor(10){ page.clickCmdPendingBytes()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click cmd PendingBytes'
            waitFor(10){ page.clickCmdPendingBytes()}
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

    def verifyTheAscendingAndDescendingInThePendingTransColumnOfCommandLogPerformanceTable() {
        String before = ""
        String after  = ""
        when:"Check if Command Log is Displayed"
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        else {
            when: 'click cmd PendingTrans'
            waitFor(10){ page.clickCmdPendingTrans()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click cmd PendingTrans'
            waitFor(10){ page.clickCmdPendingTrans()}
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

    def verifyTheAscendingAndDescendingInTheTotalSegmentsColumnOfCommandLogPerformanceTable() {
        String before = ""
        String after  = ""
        when:"Check if Command Log is Displayed"
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        else {
            when: 'click cmd TotalSegments'
            waitFor(10){ page.clickCmdTotalSegments()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click cmd TotalSegments'
            waitFor(10){ page.clickCmdTotalSegments()}
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

    def verifyTheAscendingAndDescendingInTheSegmentsInUseColumnOfCommandLogPerformanceTable() {
        String before = ""
        String after  = ""
        when:"Check if Command Log is Displayed"
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        else {
            when: 'click cmd SegmentsInUse'
            waitFor(10){ page.clickCmdSegmentsInUse()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click cmd SegmentsInUse'
            waitFor(10){ page.clickCmdSegmentsInUse()}
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

    def verifyTheAscendingAndDescendingInTheFsyncIntervalColumnOfCommandLogPerformanceTable() {
        String before = ""
        String after  = ""
        when:"Check if Command Log is Displayed"
        if( page.isCmdLogSectionOpen()==false) {
            println("CLP section is not present")
            assert true
        }
        else {
            when: 'click cmd FsyncInterval'
            waitFor(10){ page.clickCmdFsyncInterval()}
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrderDT())
                before = "ascending"
            else
                before = "descending"

            when: 'click cmd FsyncInterval'
            waitFor(10){ page.clickCmdFsyncInterval()}
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


    def verifyTheTextInTheTitleInCommandLogPerformance() {
        when: "Check Command Log Performance Title is displayed or not"
        if(page.drCLPTitleDisplayed()) {
            assert true
        }
        then:
        if(page.drCLPTitleDisplayed()) {
            println(waitFor(20) { page.drCLPTitle.text() })
            page.drCLPTitle.text().equals("Command Log")
        }
        else {
            println("Command Log Section is not visible")
        }
    }

    def verifySearchInCommandLogPerformanceTable() {
        boolean isValid=false
        String searchText = ""
        boolean isCLPOpen = false

        when: "Check if CLP section is present"
        try {
            waitFor(waitTime) { page.isCmdLogSectionOpen() }
        } catch(geb.waiting.WaitTimeoutException e) {
        }

        if(!page.isCmdLogSectionOpen()) {
            println("CLP section is not present")
            isCLPOpen = false
        }
        else {
            isCLPOpen = true
        }
        then:
        println("proceed")

        when: "Set the value of Master filter"
        println("isCLPOpen" + isCLPOpen)
        if(isCLPOpen==true) {
            if (waitFor(waitTime){page.clpServerRows.size()} > 1) {
                searchText = page.clpServerRows[0].text().substring(0, 1)
                page.filterServer.value(page.clpServerRows[0].text().substring(0, 1))
            } else {
                isValid = false
                assert true
            }
            then: "check the table"
            for (def i = 0; i <= page.clpServerRows.size() - 1; i++) {
                if (!page.clpServerRows[i].text().toString().contains(searchText)) {
                    println("test false")
                    isValid = false
                    break
                } else {
                    isValid = true
                }

            }
        }
        else {
            isValid = true
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
