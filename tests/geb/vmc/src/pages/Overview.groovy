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

		// SECURITY EXPANSION
		
		securityExpanded			{ $(class:"labelCollapsed labelExpanded", text:"security") }
        securityUsername			{ $("th", text:"Username") }
		securityRole				{ $("th", text:"Role") }
		securityAdd					{ $("#addNewUserLink1") }

		securityUserList			{ $("#UsersList") }
		
		errorUsernameMessage		{ $("#errorUser") }
		errorPasswordMessage		{ $("#errorPassword") }
		
		userPopupUsernameField		{ $("#txtUser") }
		userPopupPasswordField		{ $("#txtPassword") }
		userPopupSelectRole			{ $("#selectRole") }
		userPopupSave				{ $("#btnSaveUser") }
		userPopupCancel				{ $("#btnCancelUser") }
		userPopupDelete				{ $("#deleteSecUser") }
		
		editUser					{ $(class:"edit", onclick:onClickValue) }
		editUserNext				{ $(class:"edit", onclick:onClickValueNext) }
		saveDelete					{ $("#userSaveDelete") }
		userPopupConfirmYes			{ $("#btnSaveSecUser") }
		userPopupConfirmNo			{ $("btnCancelSaveSecUser") }
		
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

        export			            { $(class:"labelCollapsed", text:"Export") }
        exportNoConfigAvailable     { $(class:"configLabel", text:"No configuration available.") }
        exportConfiguration         { $("#exportConfiguration") }
        exportConfig                { $(class:"configLabel expoStream", title:"Click to expand/collapse", number) }

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
    
    int waitTime = 30
    int five = 5
    int count = 0
    String onClickValue = getName()
    String onClickValueNext = getNameNext()
    
    def String getName() {
    	return "addUser(1,'" + getUsernameOneForSecurity() + "','" + getRoleOneForSecurity() + "');"
    }

	def String getNameNext() {
    	return "addUser(1,'" + getUsernameTwoForSecurity() + "','" + getRoleTwoForSecurity() + "');"
    }
    
    // Get usernameone for security
    def String getUsernameOneForSecurity() {
    	BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
    	 
    	String username

        while((username = br.readLine()) != "#usernameOne") {}

        username = br.readLine()

        return username
    }

	// Get usernametwo for security
    def String getUsernameTwoForSecurity() {
    	BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
    	 
    	String username
    	 
    	while((username = br.readLine()) != ("#usernameTwo")) {}

        username = br.readLine()

        return username
    }
    
    // Get passwordone for security
    def String getPasswordOneForSecurity() {
    	BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
    	 
    	String password

        while((password = br.readLine()) != "#passwordOne") {}

        password = br.readLine()

        return password
    }

	// Get passwordtwo for security
    def String getPasswordTwoForSecurity() {
    	BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
    	 
    	String password
    	 
    	while((password = br.readLine()) != ("#passwordTwo")) {}

        password = br.readLine()

        return password
    }
    
    // Get roleone for security
    def String getRoleOneForSecurity() {
    	BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
    	 
    	String role
    	 
    	while((role = br.readLine()) != ("#roleOne")) {}

        role = br.readLine()

        return role
    }
    
    // Get roletwo for security
    def String getRoleTwoForSecurity() {
    	BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
    	 
    	String role
    	 
    	while((role = br.readLine()) != ("#roleTwo")) {}

        role = br.readLine()

        return role
    }
    
    /**
     * Click security to expand, if already it is not expanded
     */
     def boolean expandSecurity() {	
     	if (checkIfSecurityIsExpanded() == false)
     		security.click()
     	
     }
     
     
     /**
     * Click security to collapse, if already it is expanded
     */
     def boolean collapseSecurity() {	
     	if (checkIfSecurityIsExpanded() == true)
     		security.click()
     }
     
     /**
     * Check if security is expanded or not
     */
     def boolean checkIfSecurityIsExpanded() {
     	try {
		 	securityExpanded.isDisplayed()
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(org.openqa.selenium.StaleElementReferenceException e) {
     		return true
     	}
     }
     
     /**
     * Click and open security Add popup
     */
     def boolean openSecurityAdd() {
     	try {
		 	waitFor(waitTime) { securityAdd.isDisplayed() }
		 	securityAdd.click()
		 	checkSecurityAddOpen()
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
     	}
     }
     
     /**
     * Check if Security Add is open or not
     */
     def boolean checkSecurityAddOpen() {
     	try {
		 	waitFor(waitTime) {
		 		userPopupUsernameField.isDisplayed()
				userPopupPasswordField.isDisplayed()
				userPopupSave.isDisplayed()
		 	}
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
     	}
     }
     
     /**
     * Enter the username, password, and role save it
     */
     def boolean enterUserCredentials(String username, String password, String role) {
     	try {
			checkSecurityAddOpen()
		 	waitFor(waitTime) {
		 		userPopupUsernameField.value(username)
				userPopupPasswordField.value(password)
				userPopupSelectRole.value(role)
		 	}
		 	userPopupSave.click()
		 	
		 	waitFor(waitTime) {
				saveDelete.text().equals("save")
				userPopupConfirmYes.isDisplayed()
			}
			
			count = 0
			while(count < five) {
				count++
				try {
					waitFor(waitTime) { 
						userPopupConfirmYes.click()
					}
				} catch(geb.error.RequiredPageContentNotPresent e) {
					break
				} catch(org.openqa.selenium.ElementNotVisibleException e) {
					break
				} catch(geb.waiting.WaitTimeoutException e) {
					break
				}
			}
			
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
     	}
    }
     
     
    /**
    * Returns the list of all the users in Security
    */
    def String getListOfUsers() {
    	String list = ""
    	try {
    		//expandSecurity()
     		
     		waitFor(waitTime) { securityUserList.isDisplayed() }
     		
     		list = securityUserList.text()
     		return list
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		println("Error when getting list of users")
     		return ""
     	} catch(geb.waiting.WaitTimeoutException e) {
     		println("Error when getting list of users")
     		return ""
     	}
    }
     
     
    def boolean checkListForUsers(String username) {
    	
    	try {
    		String list = getListOfUsers()
    		if(list.contains(username)) {
    			println("The user " + username + " was created" )
    			return true
    		}
    		else {
    			return false
    		}
    	} catch(geb.error.RequiredPageContentNotPresent e) {
    		println("Error when getting list of users")
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		println("Error when getting list of users")
     		return false
     	}
    }
     
     
    /**
    * Check if Security Edit is open or not
    */
    def boolean checkSecurityEditOpen() {
    	try {
		 	waitFor(waitTime) {
		 		userPopupUsernameField.isDisplayed()
				userPopupPasswordField.isDisplayed()
				userPopupSave.isDisplayed()
		 	}
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
     	}
    }
     
     
    def boolean openEditUser() {
    	try {
		 	waitFor(waitTime) { editUser.isDisplayed() }
		 	editUser.click()
		 	checkSecurityAddOpen()
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
    	}
    }
     
    def boolean openEditUserNext() {
		try {
		 	waitFor(waitTime) { editUserNext.isDisplayed() }
		 	editUserNext.click()
		 	checkSecurityAddOpen()
     		return true
     	} catch(geb.error.RequiredPageContentNotPresent e) {
     		return false
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
    	}
    }
     
    def boolean cancelPopup() {
    	try {
	     	waitFor(waitTime) { userPopupCancel.click() }
    	} catch(geb.error.RequiredPageContentNotPresent e) {
     		println("Cancel button isn't present")
     		return true
     	}
    }
    
    def boolean deleteUserSecurityPopup() {
    	try {
	     	waitFor(waitTime) { userPopupDelete.click() }
	     	
	     	waitFor(waitTime) {
				saveDelete.text().equals("delete")
				userPopupConfirmYes.isDisplayed()
			}
			
			count = 0
			while(count < five) {
				count++
				try {
					waitFor(waitTime) { 
						page.overview.userPopupConfirmYes.click()
					}
				} catch(geb.error.RequiredPageContentNotPresent e) {
					break
				} catch(org.openqa.selenium.ElementNotVisibleException e) {
					break
				} catch(geb.waiting.WaitTimeoutException e) {
					break
				}
			}	
    	} catch(geb.error.RequiredPageContentNotPresent e) {
     		println("Delete button isn't present")
     		return true
     	} catch(geb.waiting.WaitTimeoutException e) {
     		return false
    	}
    }
}

