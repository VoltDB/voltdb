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

package vmcTest.pages

import geb.navigator.Navigator

class ImporterPage extends VoltDBManagementCenterPage {
    static content = {
        importer                    { $('#importer') }
        graphChartImporter          { $('#graphChartImporter') }
        divNoImportDataMsg          { $('#divNoImportDataMsg') }
        noChartMsg                  { $('#divNoImportDataMsg > div') }
        chartOutTransaction         { $('#chartOutTransaction') }
        chartSuccessRate            { $('#chartSuccessRate') }
        chartFailureRate            { $('#chartFailureRate') }
        mainImporterGraphBlock      { $('#mainImporterGraphBlock') }
        showHideImporterGraphBlock  { $('#showHideImporterGraphBlock') }
        importerGraphView           { $('#importerGraphView')}
        chartOutsTransDownloadBtn   { $('#chartOutTransHeader > h1 > a') }
        chartSuccessDownloadBtn     { $('#chartSuccessRateHeader > h1 > a') }
        chartFailureDownloadBtn     { $('#chartFailureRate > div.chartHeader > h1 > a') }
        chartOutsTransMin           { $('#visualisationOutTrans > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMin-x > text') }
        chartOutsTransMax           { $('#visualisationOutTrans > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMax-x > text') }
        chartSuccessRateMin         { $('#visualisationSuccessRate > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMin-x > text') }
        chartSuccessRateMax         { $('#visualisationSuccessRate > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMax-x > text') }
        chartFailureRateMin         { $('#visualisationFailureRate > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMin-x > text') }
        chartFailureRateMax         { $('#visualisationFailureRate > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMax-x > text') }
    }
    static at = {
        importerTab.displayed
        importerTab.attr('class') == 'active'
    }

    def boolean isImporterChartSectionDisplayed(){
        return mainImporterGraphBlock.displayed
    }

    def boolean isChartDisplayed(){
        return graphChartImporter.displayed
    }

    def boolean chooseGraphView(String choice) {
        importerGraphView.value(choice)
    }

    def String compareTime(String stringTwo, String stringOne) {
        int hourOne = changeToHour(stringOne)
        int hourTwo = changeToHour(stringTwo)
        int minuteOne = changeToMinute(stringOne)
        int minuteTwo = changeToMinute(stringTwo)

        String result = ""

        if(hourTwo-hourOne == 0) {
            result = "seconds"
        }
        else {
            if((minuteOne - minuteTwo) > 20 ) {
                result = "seconds"
            }
            else {
                result = "minutes"
            }
        }

        return result
    }

    def int changeToHour(String string) {
        String hour = string.substring(0, string.length()-6)
        int hourInt = Integer.parseInt(hour)
        return hourInt
    }

    def int changeToMinute( String string ) {
        String minute = string.substring(3, string.length()-3)
        int minuteInt = Integer.parseInt(minute)
        return minuteInt
    }

    def String changeToMonth(String string) {
        String date = string.substring(3, string.length()-9)
        return date
    }

    def int changeToDate(String string) {
        String date = string.substring(0, 2)
        int dateInt = Integer.parseInt(date)
        return dateInt
    }
}
