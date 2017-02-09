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

package vmcTest.pages

import geb.navigator.Navigator

class DrPage extends VoltDBManagementCenterPage {
    static content = {
        clusterRelArr       { $('#drArrow_0') }
        drCLusterId         { $('#drCLusterId') }
        latencyTime         { $('#latencyDR') }
        clusterId           { $('#dRProducerName') }

        drMode              { $('#dbDrMode') }

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
    }
    static at = {
        drTab.displayed
        drTab.attr('class') == 'active'
    }

    def boolean isDrSectionOpen(){
        return divDrReplication.displayed
    }

    def boolean isDrAreaOpen() {
        return drSection.displayed
    }

    def boolean isDrMasterSectionOpen() {
        return drMasterSection.displayed
    }

    def boolean isDrReplicaSectionOpen() {
        return drReplicaSection.displayed
    }

    def boolean closeDrArea() {
        clickToNotDisplay(showHideDrBlock, drSection)
        return true
    }

    def boolean openDrArea(){
        clickToDisplay(showHideDrBlock, drSection)
        return true
    }

    /*
     *  click the partitionID column in database table
     */
    def boolean clickPartitionID() {
        partitionID.click()
    }

    /*
     *  click the replicaLatencyMs column in DR MAster table
     */
    def boolean clickReplicaLatencyMs() {
        replicaLatencyMs.click()
    }

    /*
     *  click the status column in DR MAster table
     */
    def boolean clickMbOnDisk() {
        mbOnDisk.click()
    }

    /*
     *  click the replicaLatencyTrans column in DR MAster table
     */
    def boolean clickReplicaLatencyTrans() {
        replicaLatencyTrans.click()
    }

    /*
     *  click the server column in DR Replica table
     */
    def boolean clickReplicaServer() {
        replicaServer.click()
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
    def boolean clickReplicationRate1() {
        replicationRate1.click()
    }

    /*
     * click the ReplicationRate5 column in DR Replica table
     */
    def boolean clickReplicationRate5() {
        replicationRate5.click()
    }

    /*
     *  click the status column in DR Replica table
     */
    def boolean clickReplicaStatus() {
        replicaStatus.click()
    }

    def boolean drReplicaTitleDisplayed(){
        return drReplicaTitle.displayed
    }

    def boolean drMasterTitleDisplayed(){
        return drMasterTitle.displayed
    }
}
