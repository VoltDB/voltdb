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
import geb.Page.*

class ImporterTest extends TestBase {
    def isImporterTabVisible = false

    def setup() { // called before each test
        int count = 0

        while(count<2) {
            count ++
            try {
                setup: 'Open VMC page'
                to VoltDBManagementCenterPage
                expect: 'to be on VMC page'
                at VoltDBManagementCenterPage
                browser.driver.executeScript("localStorage.clear()")
                when: 'click the Importer link (if needed)'
                page.openImporterPage()
                then: 'should be on Importer page'
                at ImporterPage

                isImporterTabVisible = true


                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
                try {
                    waitFor(waitTime) { 1 == 0 }
                } catch (geb.waiting.WaitTimeoutException exception) {

                }
                isImporterTabVisible = false
            }
        }
    }

    def waitForTime(time){
        try{
            waitFor(time){ assert 1==0}
        }catch(geb.waiting.WaitTimeoutException e){

        }
    }

    def checkImporterTabOpened(){
        when:
        println("Process only if Importer is present.")
        then:
        if(isImporterTabVisible){
            when:
            waitFor(10){ page.importer.isDisplayed() }
            then:
            println("Importer page is opened.")
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkChartSectionAreDisplayed(){
        when:
        println("Process only if Importer tab is present.")
        then:
        if(isImporterTabVisible){
            waitFor(10){ page.chartOutTransaction.isDisplayed() }
            waitFor(10){ page.chartSuccessRate.isDisplayed() }
            waitFor(10){ page.chartFailureRate.isDisplayed() }

        } else {
            println("Importer tab is not available.")
        }
    }

    def checkDownloadButtons(){
        when:
        println("Process only if Importer tab is present")
        then:
        if(isImporterTabVisible){
            waitFor(5){ chartOutsTransDownloadBtn.isDisplayed() }
            waitFor(5){ chartSuccessDownloadBtn.isDisplayed() }
            waitFor(5){ chartFailureDownloadBtn.isDisplayed() }
        } else {
            println("Importer tab is not available.")
        }
    }

    def expandCollapseImporterHeader(){
        when:
        if(isImporterTabVisible) {
            when: "Check Importer Chart Header"
            assert isImporterChartSectionDisplayed() == true
            showHideImporterGraphBlock.click()
            waitForTime(2)
            assert isImporterChartSectionDisplayed() == false
            showHideImporterGraphBlock.click()
            waitForTime(2)
            assert isImporterChartSectionDisplayed() == true
        } else {
            println("Importer tab is not available.")
        }
        then:
        println("test completed.")

    }

    def checkMinAndMaxValueInOutsTransGraphSeconds() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if Importer is present.")
        then:
        if(isImporterTabVisible) {
            when:
            count = 0
            page.chooseGraphView("Seconds")
            then:
            while (count < 3) {
                count++
                try {
                    waitFor(10) {
                        $('#chartOutTransaction').isDisplayed()
                    }
                    waitFor(waitTime) {
                        chartOutsTransMax.isDisplayed()
                        chartOutsTransMin.isDisplayed()
                    }
                    stringMax = chartOutsTransMax.text()
                    stringMin = chartOutsTransMin.text()

                    break
                } catch (geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)

            if (result.equals("seconds")) {
                println("The maximum value is " + stringMax)
                println("The minimum value is " + stringMin)
                println("The time is in " + result)
                assert true
            } else {
                println("FAIL: It is not in seconds")
                assert false
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInOutsTransGraphMinutes() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if DR is present")
        then:
        if(isImporterTabVisible) {
            when:
            $('#importerGraphView').value("Minutes")
            waitForTime(10);
            then:
            count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        $('#chartOutTransaction').isDisplayed()
                    }
                    waitFor(waitTime) {
                        chartOutsTransMax.isDisplayed()
                        chartOutsTransMin.isDisplayed()
                    }
                    report 'hello'
                    stringMax = chartOutsTransMax.text()
                    stringMin = chartOutsTransMin.text()

                    break
                } catch (geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)
            if (result.toLowerCase().equals("minutes")) {
                println("The maximum value is " + stringMax)
                println("The minimum value is " + stringMin)
                println("The time is in " + result)
                assert true
            } else {
                println("FAIL: It is not in minutes")
                assert false
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInOutsTransGraphDays() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if Importer tab is present.")
        then:
        if(isImporterTabVisible) {
            if(isChartDisplayed()) {
                when:
                count = 0
                page.importerGraphView.value("Days")
                waitForTime(10)
                then:
                count = 0
                while (count < 3) {
                    count++
                    try {
                        waitFor(waitTime) {
                            $('#chartOutTransaction').isDisplayed()
                        }
                        waitFor(waitTime) {
                            chartOutsTransMax.isDisplayed()
                            chartOutsTransMin.isDisplayed()
                        }
                        stringMax = chartOutsTransMax.text()
                        stringMin = chartOutsTransMin.text()

                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }
                println("stringMax: " + stringMax)
                String monthMax = page.changeToMonth(stringMax)
                String monthMin = page.changeToMonth(stringMin)

                String dateMax = page.changeToDate(stringMax)
                String dateMin = page.changeToDate(stringMin)

                int intDateMax = Integer.parseInt(dateMax)
                int intDateMin = Integer.parseInt(dateMin)

                if (monthMax.equals(monthMin)) {
                    if (intDateMax > intDateMin) {
                        println("The maximum value is " + stringMax)
                        println("The minimum value is " + stringMin)
                        println("The time is in Days")
                    } else {
                        println("FAIL: Date of Max is less than that of date of Min for same month")
                        assert false
                    }
                } else {
                    if (intDateMax < intDateMin) {
                        println("Success")
                    } else {
                        println("FAIL: Date of Max is more than that of date of Min for new month")
                        assert false
                    }
                }
            } else {
                waitFor(10){ noChartMsg.isDisplayed() }
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInSuccessRateGraphSeconds() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if Importer is present.")
        then:
        if(isImporterTabVisible) {
            if(isChartDisplayed()) {
                when:
                count = 0
                page.chooseGraphView("Seconds")
                then:
                while (count < 3) {
                    count++
                    try {
                        waitFor(10) {
                            $('#chartSuccessRate').isDisplayed()
                        }
                        waitFor(waitTime) {
                            chartSuccessRateMax.isDisplayed()
                            chartSuccessRateMin.isDisplayed()
                        }
                        stringMax = chartSuccessRateMax.text()
                        stringMin = chartSuccessRateMin.text()

                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }

                String result = page.compareTime(stringMax, stringMin)

                if (result.equals("seconds")) {
                    println("The maximum value is " + stringMax)
                    println("The minimum value is " + stringMin)
                    println("The time is in " + result)
                    assert true
                } else {
                    println("FAIL: It is not in seconds")
                    assert false
                }
            } else {
                waitFor(10){ noChartMsg.isDisplayed() }
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInSuccessRateGraphMinutes() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if DR is present")
        then:
        if(isImporterTabVisible) {
            if(isChartDisplayed()) {
                when:
                page.chooseGraphView("Minutes")
                driver.navigate().refresh();
                waitForTime(5)
                then:
                count = 0
                while (count < 3) {
                    count++
                    try {
                        waitFor(20) {
                            $('#chartSuccessRate').isDisplayed()
                        }
                        waitFor(5) {
                            chartSuccessRateMax.isDisplayed()
                            chartSuccessRateMin.isDisplayed()
                        }
                        report 'hello'
                        stringMax = chartSuccessRateMax.text()
                        stringMin = chartSuccessRateMin.text()

                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }
                String result = page.compareTime(stringMax, stringMin)

                if (result.toLowerCase().equals("minutes")) {
                    println("The maximum value is " + stringMax)
                    println("The minimum value is " + stringMin)
                    println("The time is in " + result)
                    assert true
                } else {
                    println("FAIL: It is not in minutes")
                    assert false
                }
            } else {
                waitFor(10){ noChartMsg.isDisplayed() }
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInSuccessRateGraphDays() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if Importer tab is present")
        then:
        if(isImporterTabVisible) {
            if(isChartDisplayed()) {
                when:
                count = 0
                page.importerGraphView.value("Days")
                driver.navigate().refresh();
                then:
                count = 0
                while (count < 3) {
                    count++
                    try {
                        waitFor(waitTime) {
                            $('#chartSuccessRate').isDisplayed()
                        }
                        waitFor(waitTime) {
                            chartSuccessRateMax.isDisplayed()
                            chartSuccessRateMin.isDisplayed()
                        }
                        stringMax = chartSuccessRateMax.text()
                        stringMin = chartSuccessRateMin.text()

                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
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
                        println("The maximum value is " + stringMax)
                        println("The minimum value is " + stringMin)
                        println("The time is in Days")
                    }
                    else {
                        println("FAIL: Date of Max is less than that of date of Min for same month")
                        assert false
                    }
                }
                else {
                    if (intDateMax < intDateMin) {
                        println("Success")
                    }
                    else {
                        println("FAIL: Date of Max is more than that of date of Min for new month")
                        assert false
                    }
                }
            } else {
                waitFor(10){ noChartMsg.isDisplayed() }
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInFailureRateGraphSeconds() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if importer tab is present")
        then:
        if(isImporterTabVisible) {
            if(isChartDisplayed()) {
                when:
                count = 0
                page.chooseGraphView("Seconds")
                then:
                while (count < 3) {
                    count++
                    try {
                        waitFor(10) {
                            $('#chartFailureRate').isDisplayed()
                        }
                        waitFor(waitTime) {
                            chartFailureRateMax.isDisplayed()
                            chartFailureRateMin.isDisplayed()
                        }
                        stringMax = chartFailureRateMax.text()
                        stringMin = chartFailureRateMin.text()

                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }

                String result = page.compareTime(stringMax, stringMin)

                if (result.equals("seconds")) {
                    println("The maximum value is " + stringMax)
                    println("The minimum value is " + stringMin)
                    println("The time is in " + result)
                    assert true
                } else {
                    println("FAIL: It is not in seconds")
                    assert false
                }
            } else {
                waitFor(10){ noChartMsg.isDisplayed() }
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInFailureRateGraphMinutes() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if DR is present")
        then:
        if(isImporterTabVisible) {
            if(isChartDisplayed()) {
                when:
                page.importerGraphView.value("Minutes")
                driver.navigate().refresh();
                waitForTime(5)
                then:
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        waitFor(waitTime) {
                            $('#chartFailureRate').isDisplayed()
                        }
                        waitFor(waitTime) {
                            chartFailureRateMax.isDisplayed()
                            chartFailureRateMin.isDisplayed()
                        }
                        report 'hello'
                        stringMax = chartFailureRateMax.text()
                        stringMin = chartFailureRateMin.text()

                        break
                    } catch (geb.waiting.WaitTimeoutException e) {
                        println("WaitTimeoutException")
                    }
                }

                String result = page.compareTime(stringMax, stringMin)
                println(stringMax)
                println(stringMin)
                println(result)
                if (result.toLowerCase().equals("minutes")) {
                    println("The maximum value is " + stringMax)
                    println("The minimum value is " + stringMin)
                    println("The time is in " + result)
                    assert true
                } else {
                    println("FAIL: It is not in minutes")
                    assert false
                }
            } else {
                waitFor(10){ noChartMsg.isDisplayed() }
            }
        } else {
            println("Importer tab is not available.")
        }
    }

    def checkMinAndMaxValueInFailureRateGraphDays() {
        String stringMax
        String stringMin
        int count = 0
        when:
        println("Process only if Importer tab is present")
        then:
        if(isImporterTabVisible) {
            when:
            page.chooseGraphView("Days")
            driver.navigate().refresh();
            waitForTime(6)
            then:
            waitFor(20) {
                $('#chartFailureRate').isDisplayed()
            }

            waitFor(waitTime) {
                chartFailureRateMax.isDisplayed()
                chartFailureRateMin.isDisplayed()
            }
            stringMax = chartFailureRateMax.text()
            stringMin = chartFailureRateMin.text()


            String monthMax = page.changeToMonth(stringMax)
            String monthMin = page.changeToMonth(stringMin)

            String dateMax = page.changeToDate(stringMax)
            String dateMin = page.changeToDate(stringMin)

            int intDateMax = Integer.parseInt(dateMax)
            int intDateMin = Integer.parseInt(dateMin)

            if(monthMax.equals(monthMin)) {
                if(intDateMax > intDateMin) {
                    println("The maximum value is " + stringMax)
                    println("The minimum value is " + stringMin)
                    println("The time is in Days")
                }
                else {
                    println("FAIL: Date of Max is less than that of date of Min for same month")
                    assert false
                }
            }
            else {
                if (intDateMax < intDateMin) {
                    println("Success")
                }
                else {
                    println("FAIL: Date of Max is more than that of date of Min for new month")
                    assert false
                }
            }
        } else {
            println("Importer tab is not available.")
        }
    }
}
