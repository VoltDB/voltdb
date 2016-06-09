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

class DbMonitorUatTest extends TestBase {

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

    // Command Log Statistics
    def "Verify Max and Min values in Command Log Statistics days"(){
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify Max and Min values in Command Log Statistics days")
        int count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.commandLogStatistics.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.commandLogStatistics.isDisplayed()) {
            // This loop is used to gain time.
            count = 0
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Days")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            String stringMax = ""
            String stringMin = ""

            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.commandLogStatisticsMax.isDisplayed()
                        page.commandLogStatisticsMin.isDisplayed()
                    }
                    stringMax = page.commandLogStatisticsMax.text()
                    stringMin = page.commandLogStatisticsMin.text()

                    println(stringMax)
                    println(stringMin)

                    if(stringMax.length()<10 || stringMax.length()<10) {
                        println("Not fixed")
                        continue
                    }

                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String monthMax = page.changeToMonth(stringMax)
            String monthMin = page.changeToMonth(stringMin)

            String dateMax = page.changeToDate(stringMax)
            String dateMin = page.changeToDate(stringMin)

            int intDateMax = Integer.parseInt(dateMax)
            int intDateMin = Integer.parseInt(dateMin)

            if(monthMax.equals(monthMin)) {
                if(intDateMax > intDateMin) {
                    println("The maximum and minimum values are " + stringMax + " and " + stringMin + " and the time is in Days")
                }
                else {
                    println("FAIL: Date of Max is less than that of date of Min for same month")
                    println("Test End: Verify Max and Min values in Command Log Statistics days")
                    assert false
                }
            }
            else {
                if (intDateMax < intDateMin) {
                    println("Success")
                }
                else {
                    println("FAIL: Date of Max is more than that of date of Min for new month")
                    println("Test End: Verify Max and Min values in Command Log Statistics days")
                    assert false
                }
            }
        }
        else {
            println("The Command Log Statistics graph is not visible")
            println("Test End: Verify Max and Min values in Command Log Statistics days")
        }
        then:
        println("")
    }

    def "Verify Max and Min values in Command Log Statistics minutes"(){
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify Max and Min values in Command Log Statistics minutes")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.commandLogStatistics.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.commandLogStatistics.isDisplayed()) {
            // This loop is used to gain time.
            count = 0
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Minutes")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            String stringMax
            String stringMin

            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.commandLogStatisticsMax.isDisplayed()
                        page.commandLogStatisticsMin.isDisplayed()
                    }
                    stringMax = page.commandLogStatisticsMax.text()
                    stringMin = page.commandLogStatisticsMin.text()
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)

            if(result.equals("minutes")) {
                println("The maximum and minimum values are " + stringMax + " and " + stringMin + " and the time is in " + result )
                println("Test End: Verify Max and Min values in Command Log Statistics minutes")
                assert true
            }
            else {
                println("FAIL: It is not in minutes")
                println("Test End: Verify Max and Min values in Command Log Statistics minutes")
                assert false
            }
        }
        else {
            println("The Command Log Statistics graph is not visible")
            println("Test End: Verify Max and Min values in Command Log Statistics minutes")
        }
        then:
        println("")
    }

    def "Verify Max and Min values in Command Log Statistics seconds"(){
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify Max and Min values in Command Log Statistics seconds")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.commandLogStatistics.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.commandLogStatistics.isDisplayed()) {
            // This loop is used to gain time.
            count = 0
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Seconds")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            String stringMax
            String stringMin

            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.commandLogStatisticsMax.isDisplayed()
                        page.commandLogStatisticsMin.isDisplayed()
                    }
                    stringMax = page.commandLogStatisticsMax.text()
                    stringMin = page.commandLogStatisticsMin.text()
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)

            if(result.equals("seconds")) {
                println("The maximum and minimum values are " + stringMax + " and " + stringMin + " and the time is in " + result )
                println("Test End: Verify Max and Min values in Command Log Statistics seconds")
                assert true
            }
            else {
                println("FAIL: It is not in seconds")
                println("Test End: Verify Max and Min values in Command Log Statistics seconds")
                assert false
            }
        }
        else {
            println("The Command Log Statistics graph is not visible")
            println("Test End: Verify Max and Min values in Command Log Statistics seconds")
        }
        then:
        println("")
    }

    // end of Command Log Statistics

    // Database Replication
    def "Verify Max and Min values in Database Replication days"(){
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify Max and Min values in Database Replication days")
        int count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.databaseReplication.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.databaseReplication.isDisplayed()) {
            // This loop is used to gain time.
            count = 0
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Days")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            String stringMax = ""
            String stringMin = ""

            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.databaseReplicationMax.isDisplayed()
                        page.databaseReplicationMin.isDisplayed()
                    }
                    stringMax = page.databaseReplicationMax.text()
                    stringMin = page.databaseReplicationMin.text()

                    println(stringMax)
                    println(stringMin)

                    if(stringMax.length()<10 || stringMax.length()<10) {
                        println("Not fixed")
                        continue
                    }

                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String monthMax = page.changeToMonth(stringMax)
            String monthMin = page.changeToMonth(stringMin)

            String dateMax = page.changeToDate(stringMax)
            String dateMin = page.changeToDate(stringMin)

            int intDateMax = Integer.parseInt(dateMax)
            int intDateMin = Integer.parseInt(dateMin)

            if(monthMax.equals(monthMin)) {
                if(intDateMax > intDateMin) {
                    println("The maximum and minimum values are " + stringMax + " and " + stringMin + " and the time is in Days")
                }
                else {
                    println("FAIL: Date of Max is less than that of date of Min for same month")
                    println("Test End: Verify Max and Min values in Database Replication days")
                    assert false
                }
            }
            else {
                if (intDateMax < intDateMin) {
                    println("Success")
                }
                else {
                    println("FAIL: Date of Max is more than that of date of Min for new month")
                    println("Test End: Verify Max and Min values in Database Replication days")
                    assert false
                }
            }
        }
        else {
            println("The Database Replication graph is not visible")
            println("Test End: Verify Max and Min values in Database Replication days")
        }
        then:
        println("")
    }

    def "Verify Max and Min values in Database Replication minutes"(){
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify Max and Min values in Database Replication minutes")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.databaseReplication.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.databaseReplication.isDisplayed()) {
            // This loop is used to gain time.
            count = 0
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Minutes")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            String stringMax
            String stringMin

            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.databaseReplicationMax.isDisplayed()
                        page.databaseReplicationMin.isDisplayed()
                    }
                    stringMax = page.databaseReplicationMax.text()
                    stringMin = page.databaseReplicationMin.text()
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)

            if(result.equals("minutes")) {
                println("The maximum and minimum values are " + stringMax + " and " + stringMin + " and the time is in " + result )
                println("Test End: Verify Max and Min values in Database Replication minutes")
                assert true
            }
            else {
                println("FAIL: It is not in minutes")
                println("Test End: Verify Max and Min values in Database Replication minutes")
                assert false
            }
        }
        else {
            println("The Database Replication graph is not visible")
            println("Test End: Verify Max and Min values in Database Replication minutes")
        }
        then:
        println("")
    }

    def "Verify Max and Min values in Database Replication seconds"(){
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify Max and Min values in Database Replication seconds")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.databaseReplication.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.databaseReplication.isDisplayed()) {
            // This loop is used to gain time.
            count = 0
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Seconds")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            String stringMax
            String stringMin

            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.databaseReplicationMax.isDisplayed()
                        page.databaseReplicationMin.isDisplayed()
                    }
                    stringMax = page.databaseReplicationMax.text()
                    stringMin = page.databaseReplicationMin.text()
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)

            if(result.equals("seconds")) {
                println("The maximum and minimum values are " + stringMax + " and " + stringMin + " and the time is in " + result )
                println("Test End: Verify Max and Min values in Database Replication seconds")
                assert true
            }
            else {
                println("FAIL: It is not in seconds")
                println("Test End: Verify Max and Min values in Database Replication seconds")
                assert false
            }
        }
        else {
            println("The Database Replication graph is not visible")
            println("Test End: Verify Max and Min values in Database Replication seconds")
        }
        then:
        println("")
    }
    //end of Database Replication

    //
    def "Verify the invisibility of the Command Log Statistics Graph using Display Preferences"() {
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify the invisibility of the Command Log Statistics Graph using Display Preferences")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.commandLogStatistics.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.commandLogStatistics.isDisplayed()) {
            page.openDisplayPreference()
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            page.commandLogStatisticsCheckbox.isDisplayed()
            page.commandLogStatisticsCheckbox.click()

            page.savePreferences()
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            !page.commandLogStatistics.isDisplayed()

            println("Command Log Statistics Graph isn't displayed")

            page.openDisplayPreference()
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            page.commandLogStatisticsCheckbox.isDisplayed()
            page.commandLogStatisticsCheckbox.click()

            page.savePreferences()
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            page.commandLogStatistics.isDisplayed()

            println("Command Log Statistics Graph is displayed")
            println("Test End: Verify the invisibility of the Command Log Statistics Graph using Display Preferences")
        }
        else {
            println("The Command Log Statistics graph is not visible")
            println("Test End: Verify the invisibility of the Command Log Statistics Graph using Display Preferences")
        }
        then:
        println("")
    }


    def "Verify the invisibility of the Database Replication Graph using Display Preferences"() {
        expect: 'at DbMonitorPage'
        at DbMonitorPage

        when:
        println("Test Start: Verify the invisibility of the Database Replication Graph using Display Preferences")

        int count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                waitFor(waitTime) { page.databaseReplication.isDisplayed() }
                println("Success")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        if(page.databaseReplication.isDisplayed()) {
            page.openDisplayPreference()
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            page.databaseReplicationCheckbox.isDisplayed()
            page.databaseReplicationCheckbox.click()

            page.savePreferences()
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            !page.databaseReplication.isDisplayed()

            println("The Database Replication graph isn't displayed")

            page.openDisplayPreference()
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            page.databaseReplicationCheckbox.isDisplayed()
            page.databaseReplicationCheckbox.click()

            page.savePreferences()
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            page.databaseReplication.isDisplayed()

            println("The Database Replication graph is displayed")
            println("Test End: Verify the invisibility of the Database Replication Graph using Display Preferences")
        }
        else {
            println("The Database Replication graph is not visible")
            println("Test End: Verify the invisibility of the Database Replication Graph using Display Preferences")
        }
        then:
        println("")
    }
}
