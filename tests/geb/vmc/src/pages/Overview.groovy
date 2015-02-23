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
    	title						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.headerAdminContent h1") }
		sitePerHost					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr td.configLabel") }
		ksafety						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr td.configLabel") }
		partitionDetection			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr td.configLabel") }
		security					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td.configLabel a.labelCollapsed") }
		httpAccess					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-1.hasSubLabel.parent td.configLabel a.labelCollapsed") }
		autoSnapshots				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td.configLabel a.labelCollapsed") }	
		commandLogging				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-3.hasSubLabel.parent td.configLabel a.labelCollapsed") }
		export						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-4.hasSubLabel.parent td.configLabel a.labelCollapsed") }
		advanced					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-5.hasSubLabel.parent td.configLabel a.labelCollapsed") }
		

		sitePerHostValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr td#sitePerHost") }
		ksafetyValue				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr td#kSafety") }
		partitionDetectionValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr td#partitionDetectionLabel") }
		securityValue				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td span#spanSecurity") }
		httpAccessValue				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-1.hasSubLabel.parent td#httpAccessLabel") }
		autoSnapshotsValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td div#txtAutoSnapshot.SnapshotsOn") }
		commandLoggingValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-3.hasSubLabel.parent td#commandLogLabel") }
		exportValue					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-4.hasSubLabel.parent td#txtExportLabel") }


		securityEdit				{ $("#securityEdit") }
		securityEditCheckbox		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td.securitytd div.icheckbox_square-aero.checked.customCheckbox ins.iCheck-helper") }
		securityEditOk				{ $("#btnEditSecurityOk") }
		securityEditCancel			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td a#btnEditSecurityCancel.editCancel") }
		

		autoSnapshotsEdit			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#autoSnapshotEdit.edit") }
		autoSnapshotsEditCheckbox1 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td#autoSnapshotOption.snapshottd div.icheckbox_square-aero.customCheckbox ins.iCheck-helper") }
		autoSnapshotsEditCheckbox 	{ $(class:"icheckbox_square-aero customCheckbox") }

		autoSnapshotsEditOk 		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotOk.editOk") }
		autoSnapshotsEditCancel 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotCancel.editCancel") }


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
		
		jsonApi 					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-1.subLabelRow td.configLabel") }
		jsonApiValue 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-1.subLabelRow td#txtJsonAPI") }

	// AUTO SNAPSHOTS EXPANSION



	// COMMAND LOGGING EXPANSION
		
		lftime 						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td.configLabel") }
		lftransactions 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td.configLabel") }
		lssize						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td.configLabel") }

		lftimeValue 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td span#commandLogFrequencyTime") }
		lftransactionsValue 		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td#commandLogFrequencyTxns") }
		lssizeValue					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td span#commandLogSegmentSize") }

	

	// EXPORT EXPANSION

		target 						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-4.subLabelRow td.configLabel") }
		properties 					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-4.subLabelRow td.configLabel") }
		propertiesSub 				{ $("tbody tr.propertyLast td") }

	// ADVANCED EXPANSION

		maxJavaHeap 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td.configLabel") }
		heartbeatTimeOut			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td.configLabel") }
		queryTimeOut				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td.configLabel") }
		maxTempTableMemory			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td.configLabel") }
		snapshotPriority			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td.configLabel") }

		maxJavaHeapValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td") }
		heartbeatTimeOutValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td.heartbeattd form#formHeartbeatTimeout span#hrtTimeOutSpan.unit") }
		queryTimeOutValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td.queryTimeOut form#formQueryTimeout") }
		maxTempTableMemoryValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td span#temptablesmaxsize") }
		snapshotPriorityValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td#snapshotpriority") }

		heartbeatTimeOutEdit		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td a#btnEditHrtTimeOut.edit") }
		heartbeatTimeOutBox			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td.heartbeattd form#formHeartbeatTimeout input#txtHrtTimeOut.exitableTxtBx.valid") }
		heartbeatTimeOutOk			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td a#btnEditHeartbeatTimeoutOk.editOk") }
		heartbeatTimeOutCancel		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td a#btnEditHeartbeatTimeoutCancel.editCancel") }

		queryTimeOutEdit			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td a#btnEditQueryTimeout.edit") }
		queryTimeOutBox				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td.queryTimeOut form#formQueryTimeout input#txtQueryTimeout.exitableTxtBx.valid") }
		queryTimeOutOk				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td a#btnEditQueryTimeoutOk.editOk") }
		queryTimeOutCancel			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td a#btnEditQueryTimeoutCancel.editCancel") }
	}
}

