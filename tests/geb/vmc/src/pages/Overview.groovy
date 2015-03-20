package vmcTest.pages

import geb.Module
import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by anrai on 2/16/15.
 */

class Overview extends Module {
    static content = {
        title				{ $("h1", text:"Overview") }
        sitePerHost			{ $(class:"configLabel", text:"Sites Per Host") }
        ksafety				{ $(class:"configLabel", text:"K-Safety") }
        partitionDetection	{ $(class:"configLabel", text:"Partition Detection") }
        security			{ $(class:"labelCollapsed", text:"Security") }
        httpAccess			{ $(class:"labelCollapsed", text:"HTTP Access") }
        autoSnapshots		{ $(class:"labelCollapsed", text:"Auto Snapshots") }
        commandLogging		{ $(class:"labelCollapsed", text:"Command Logging") }
        export				{ $(class:"labelCollapsed", text:"Export") }
        advanced			{ $(class:"labelCollapsed", text:"Advanced") }


        sitePerHostValue			{ $("#sitePerHost") }
        ksafetyValue				{ $("#kSafety") }
        partitionDetectionValue		{ $("#partitionDetectionLabel") }
        securityValue				{ $("#spanSecurity") }
        httpAccessValue				{ $("#httpAccessLabel") }
        autoSnapshotsValue			{ $("#txtAutoSnapshot") }
        commandLoggingValue			{ $("#commandLogLabel") }


        securityEdit				{ $("#securityEdit") }
        securityEditCheckbox		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td.securitytd div.icheckbox_square-aero.checked.customCheckbox ins.iCheck-helper") }
        securityEditOk				{ $("#btnEditSecurityOk") }
        securityEditCancel			{ $("#btnEditSecurityCancel") }

        autoSnapshotsEdit			{ $("#autoSnapshotEdit") }
        autoSnapshotsEditCheckbox1 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td#autoSnapshotOption.snapshottd div.icheckbox_square-aero.customCheckbox ins.iCheck-helper") }

        autoSnapshotsEditOk 		{ $("#btnEditAutoSnapshotOk") }
        autoSnapshotsEditCancel 	{ $("#btnEditAutoSnapshotCancel") }


        filePrefixField             { $(id:"txtPrefix") }
        frequencyField              { $(id:"txtFrequency") }
        frequencyUnitField          { $(id:"ddlfrequencyUnit") }
        retainedField               { $(id:"txtRetained") }

        filePrefix                  { $(id:"prefixSpan") }
        frequency                   { $(id:"frequencySpan") }
        frequencyUnit               { $(id:"spanFrequencyUnit") }
        retained                    { $(id:"retainedSpan") }

        hour                        { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }
        minute                      { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }
        second                      { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }

        // SECURITY POPUP
        securityPopupOk             { $(id:"btnSecurityOk") }
        securityPopupCancel         { $(id:"btnPopupSecurityCancel") }

        // AUTO SNAPSHOTS EDIT POPUP

        autoSnapshotsPopup			{ $("html body div.popup_cont div.popup div.popup_content") }
        autoSnapshotsPopupTitle		{ $("html body div.popup_cont div.popup div.popup_content div.overlay-title.icon-alert") }
        autoSnapshotsPopupDisplay	{ $("html body div.popup_cont div.popup div.popup_content div.overlay-content.confirmationHeight p.txt-bold") }
        autoSnapshotsPopupClose		{ $("html body div.popup_cont div.popup_close") }
        autoSnapshotsPopupOk		{ $(id:"btnSaveSnapshot", text:"Ok") }
        autoSnapshotsPopupCancel	{ $("html body div.popup_cont div.popup div.popup_content div.overlay-btns a#btnPopupAutoSnapshotCancel.btn.btn-gray") }

        // HTTP ACCESS EXPANSION

        jsonApi			{ $(class:"configLabel", text:"JSON API") }
		jsonApiStatus	{ $("#txtJsonAPI") }
	
        // AUTO SNAPSHOTS EXPANSION



        // COMMAND LOGGING EXPANSION

		logFrequencyTime			{ $(class:"configLabel", text:"Log Frequency Time") }
		logFrequencyTransactions	{ $(class:"configLabel", text:"Log Frequency Transactions") }
		logSize						{ $(class:"configLabel", text:"Log Size") }
		
		logFrequencyTimeValue			{ $("#commandLogFrequencyTime") }
		logFrequencyTransactionsValue	{ $("#commandLogFrequencyTxns") }
		logSizeValue					{ $("#commandLogSegmentSize") }

        // EXPORT EXPANSION

        // ADVANCED EXPANSION

        maxJavaHeap				{ $(class:"configLabel", text:"Max Java Heap") }
		heartbeatTimeout		{ $(class:"configLabel", text:"Heartbeat Timeout") }
		queryTimeout			{ $(class:"configLabel", text:"Query Timeout") }
		maxTempTableMemory		{ $(class:"configLabel", text:"Max Temp Table Memory") }
		snapshotPriority		{ $(class:"configLabel", text:"Snapshot Priority") }
		
		maxJavaHeapValue		{ $("#maxJavaHeap") }
		heartbeatTimeoutValue	{ $("#formHeartbeatTimeout") }
		queryTimeoutValue		{ $("#queryTimeOutUnitSpan") }
		maxTempTableMemoryValue	{ $("#temptablesmaxsizeUnit") }
		snapshotPriorityValue	{ $("#snapshotpriority") }

        // heartbeat timeout
        heartTimeoutEdit		{ $("#btnEditHrtTimeOut") }
        heartTimeoutValue		{ $("#hrtTimeOutSpan") }
        heartTimeoutField		{ $("#txtHrtTimeOut") }
        heartTimeoutOk			{ $("#btnEditHeartbeatTimeoutOk") }
        heartTimeoutCancel		{ $("#btnEditHeartbeatTimeoutCancel") }
        heartTimeoutPopupOk		{ $("#btnPopupHeartbeatTimeoutOk") }
        heartTimeoutPopupCancel	{ $("#btnPopupHeartbeatTimeoutCancel") }

        // query timeout
        queryTimeoutEdit		{ $("#btnEditQueryTimeout") }
        queryTimeoutValue		{ $("#queryTimeOutSpan") }
        queryTimeoutField		{ $("#txtQueryTimeout") }
        queryTimeoutOk			{ $("#btnEditQueryTimeoutOk") }
        queryTimeoutCancel		{ $("#btnEditQueryTimeoutCancel") }
        queryTimeoutPopupOk		{ $("#btnPopupQueryTimeoutOk") }
        queryTimeoutPopupCancel	{ $("#btnPopupQueryTimeoutCancel") }

        // error message
        errorMsgHeartbeat		{ $("#errorHeartbeatTimeout") }
        errorQuery				{ $("#errorQueryTimeout") }
    }
}

