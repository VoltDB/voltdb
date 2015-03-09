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

/**
 * This class contains tests of the 'DB Monitor' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */
class DbMonitorTest extends TestBase {

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage
    }

    def 'confirm Graph area and Data area open initially'() {
        expect: 'Graph area open initially'
        page.isGraphAreaOpen()

        and: 'Data area open initially'
        page.isDataAreaOpen()
    }

    def openAndCloseGraphArea() {
        when: 'ensure the Graph area is open'
        if (!page.isGraphAreaOpen()) {
            page.openGraphArea()
        }
        then: 'Graph area is open (initially)'
        page.isGraphAreaOpen()

        when: 'click Show/Hide Graph (to close)'
        page.closeGraphArea()
        then: 'Graph area is closed'
        !page.isGraphAreaOpen()

        when: 'click Show/Hide Graph (to open)'
        page.openGraphArea()
        then: 'Graph area is open (again)'
        page.isGraphAreaOpen()

        when: 'click Show/Hide Graph (to close again)'
        page.closeGraphArea()
        then: 'Graph area is closed (again)'
        !page.isGraphAreaOpen()
    }

    def openAndCloseDataArea() {
        when: 'ensure the Data area is open'
        if (!page.isDataAreaOpen()) {
            page.openDataArea()
        }
        then: 'Data area is open (to start test)'
        page.isDataAreaOpen()

        when: 'click Show/Hide Data (to close)'
        page.closeDataArea()
        then: 'Data area is closed'
        !page.isDataAreaOpen()

        when: 'click Show/Hide Data (to open again)'
        page.openDataArea()
        then: 'Data area is open (again)'
        page.isDataAreaOpen()

        when: 'click Show/Hide Data (to close again)'
        page.closeDataArea()
        then: 'Data area is closed (again)'
        !page.isDataAreaOpen()
    }

    def checkActiveMissingJoining() {
        expect: '1 Active server (at least)'
        page.getActive() >= 1

        and: '0 Missing servers (initially)'
        page.getMissing() == 0

        and: 'Joining servers not shown (for now)'
        page.getJoining() == -1
    }

    def openAndCloseServerList() {
        expect: 'Server list closed initially'
        !page.isServerListOpen()
        
        when: 'click Server button (to open list)'
        page.openServerList()
        then: 'Server list is open'
        page.isServerListOpen()

        when: 'click Server button (to close list)'
        page.closeServerList()
        then: 'Server list is closed (again)'
        !page.isServerListOpen()
    }

    def triggerAlert() {
        expect: 'no Alerts shown, initially'
        page.getAlert() == -1

        // TODO: add more testing here, setting threshold
    }

    def checkServerNamesAndMemoryUsage() {
        expect: 'Server list closed initially'
        !page.isServerListOpen()

        // TODO: make this a real test, not just printing values
        List<String> serverNames = page.getServerNames()
        debugPrint "Server Names            : " + serverNames
        debugPrint "Memory Usages           : " + page.getMemoryUsages()
        debugPrint "Memory Usage Percents   : " + page.getMemoryUsagePercents()
        debugPrint "Memory Usage (0)        : " + page.getMemoryUsage(serverNames.get(0))
        debugPrint "Memory Usage Percent (0): " + page.getMemoryUsagePercent(serverNames.get(0))
    }
}
