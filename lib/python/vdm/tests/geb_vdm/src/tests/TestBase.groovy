/*
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
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
}