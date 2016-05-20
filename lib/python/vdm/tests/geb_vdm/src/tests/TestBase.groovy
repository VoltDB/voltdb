/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

import geb.spock.GebReportingSpec
import java.text.SimpleDateFormat
import java.util.Date
import java.util.List
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import geb.Page
import geb.spock.GebReportingSpec

import org.junit.Rule
import org.junit.rules.TestName
import org.openqa.selenium.Dimension

class TestBase extends GebReportingSpec {
    static int numberOfTrials = 5
    static int waitTime = 10
    int count

    String create_DatabaseTest_File = "src/resources/create_DatabaseTest.csv"
    int indexOfNewDatabase = 0
    int indexOfLocal = 1

    static final boolean DEFAULT_DEBUG_PRINT = false
    static final int DEFAULT_WINDOW_WIDTH  = 1500
    static final int DEFAULT_WINDOW_HEIGHT = 1000
    static final int MAX_SECS_WAIT_FOR_PAGE = 60
    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    def setupSpec() { // called once (per test class), before any tests
        def winSize = driver.manage().window().size
        driver.manage().window().setSize(new Dimension(DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT))
    }

    def String getTestingUrl() {
        String testingName
        baseUrl = System.getProperty("geb.build.baseUrl")

        if(baseUrl.contains("localhost")) {
            testingName = "127.0.0.1"
        }
        else {
            String[] temp = baseUrl.split(':')
            testingName = temp[1]
            testingName = testingName.substring(2)
        }
        return testingName
    }
}