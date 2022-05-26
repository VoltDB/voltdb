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

class DrPage extends VoltDBManagementCenterPage {
    static content = {
        clusterRelArr       { $('#drArrow_0') }
        drCLusterId         { $('#drCLusterId') }
        latencyTime         { $('#latencyDR') }
        clusterId           { $('#dRProducerName') }
        drPending           { $('#drPending_0') }
        drArrow             { $('#drArrow_0') }
        drArrowParagraph    { $('#drArrow_0 p') }
        drMode              { $('#drModeName') }
        dRHeaderName        { $('#dRHeaderName_0') }
        dRRemoteHeaderName  { $('#dRRemoteHeaderName_0') }

        divDrReplication    { $("#divDrReplication") }
        drSection           { $("#drSection") }
        showHideData        { $('#ShowHideBlock') }
        showHideDrBlock     { $("#showHideDrBlock") }

        ascendingDT         { $(class:"sorting_asc") }
        descendingDT        { $(class:"sorting_desc") }

        partitionID         { $("#partitionID") }
        mbOnDisk            { $("#mbOnDisk") }
        replicaLatencyMs    { $("#replicaLatencyMs") }
        replicaLatencyTrans { $("#replicaLatencyTrans") }
        replicaServer       { $("#replicaServer") }

        drMasterSection     {$("#drMasterSection")}
        drReplicaSection    (required:false) { $("#drReplicaSection") }

        replicationRate1    { $("#replicationRate1") }
        replicationRate5    {$("#replicationRate5")}
        replicaStatus       {$("#replicaStatus")}
        drReplicaTitle      {$("#drReplicaTitle")}
        filterReplicaServerRows{$("#tblDrReplica").find(class:"sorting_1")}
        drMasterTitle       {$("#drMasterTitle")}
        partitionIdRows     {$("#tblDrMAster").find(class:"sorting_1")}
        filterPartitionId   {$("#filterPartitionId")}

        //graph
        drGraphView         { $("#drGraphView")}
        chartDrMin          { $("#visualizationDrReplicationRate_1 > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMin-x > text") }
        chartDrMax          { $("#visualizationDrReplicationRate_1 > g > g > g.nv-x.nv-axis.nvd3-svg > g > g.nv-axisMaxMin.nv-axisMaxMin-x.nv-axisMax-x > text") }

    }
    static at = {
        drTab.displayed
        drTab.attr('class') == 'active'
    }

    def boolean isDrSectionOpen(divId){
        return $("#divDrReplication" + divId).displayed
    }

    def boolean isDrPendingPresent(divId){
        return drPending.displayed
    }

    def boolean isDrAreaOpen(divId) {
        return $("#drSection_" + divId).displayed
    }

    def boolean isDrMasterSectionOpen(divId) {
        return $("#drMasterSection_" + divId).displayed
    }

    def boolean isDrReplicaSectionOpen(divId) {
        return $("#drReplicaSection_" + divId).displayed
    }

    def boolean closeDrArea(divId) {
        clickToNotDisplay($("#showHideDrBlock_" + divId), $("#drSection_" + divId))
        return true
    }

    def boolean openDrArea(divId){
        clickToDisplay($("#showHideDrBlock_" + divId), $("#drSection_" + divId))
        return true
    }

    /*
     *  click the partitionID column in database table
     */
    def boolean clickPartitionID(divId) {
        $("#partitionID_" + divId).click()
    }

    /*
     *  click the replicaLatencyMs column in DR MAster table
     */
    def boolean clickReplicaLatencyMs(divId) {
        $("#replicaLatencyMs_" + divId).click()
    }

    /*
     *  click the status column in DR MAster table
     */
    def boolean clickMbOnDisk(divId) {
        $("#mbOnDisk_" + divId).click()
    }

    /*
     *  click the replicaLatencyTrans column in DR MAster table
     */
    def boolean clickReplicaLatencyTrans(divId) {
        $("#replicaLatencyTrans_" + divId).click()
    }

    /*
     *  click the server column in DR Replica table
     */

    def boolean clickReplicaServer(divId) {
        $("#replicaServer_" + divId).click()
    }

    /*
     *   return true if table is in ascending order
     *  to check ascending order, check the class "sorting_asc" displayed
     */
    def boolean tableInAscendingOrderDT() {
        if ( ascendingDT.displayed )
            return true
        else
            return false
    }

    /*
     *  return true if table is in descending order
     *  to check descending order, check the class "sorting_desc" displayed
     */
    def boolean tableInDescendingOrderDT() {
        if ( descendingDT.displayed )
            return true
        else
            return false
    }

    /*
     *  click the status column in Replication Rate 1min table
     */

    def boolean clickReplicationRate1(divId) {
        $("#replicationRate1_" + divId).click()
    }

    /*
     * click the ReplicationRate5 column in DR Replica table
     */

    def boolean clickReplicationRate5(divId) {
        $("#replicationRate5_" + divId).click()
    }

    /*
     *  click the status column in DR Replica table
     */

    def boolean clickReplicaStatus(divId) {
        $("#replicaStatus_" + divId).click()
    }

    def boolean drReplicaTitleDisplayed(divId){
        return $("#drReplicaTitle_" + divId).displayed

    }

    def boolean drMasterTitleDisplayed(divId){
        return $("#drMasterTitle_" + divId).displayed
    }

    def boolean chooseGraphView( String choice ) {
        drGraphView.value(choice)
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

    def boolean isDrHeaderExpandedOrCollapsed(divId){
        return $("#showHideDrBlock_" + divId).displayed
    }

}
