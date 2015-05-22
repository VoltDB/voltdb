package vmcTest.tests

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*


class AdminExport extends TestBase {
	def "Check export Click and check its value"(){
		
		when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage
				
		when:
		waitFor(waitTime){page.exportbtn.isDisplayed()}
		page.exportbtn.click()
		
		then:
		waitFor(waitTime){page.noconfigtxt.isDisplayed()}
		if(page.noconfigtxt.text()=="No configuration available.")
		{println("Currently, No configurations are available in export")}else 
		{println("Early presence of Configuration settings detected!")}
		page.exportbtn.click()
	}


	def "Check export add configuration for empty values and validate errors" (){
	
	when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage
				
		when:
		at AdminPage
		
		waitFor(waitTime){	page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()}
				page.addconfig.click()
		then:
		waitFor(waitTime){	page.addconfigpopup.isDisplayed()
				page.addconfigtxt.isDisplayed()
				page.streamtxt.isDisplayed()
				page.typetxt.isDisplayed()
				page.propertiestxt.isDisplayed()
				page.nametxt.isDisplayed()
				page.valuetxt.isDisplayed()
				page.deletetxt.isDisplayed()
			}
			if(page.addconfigtxt.text().equals("Add Configuration") && page.streamtxt.text().equals("Stream") && page.typetxt.text().equals("Type") && page.propertiestxt.text().equals("Properties") && page.nametxt.text().equals("Name") && page.valuetxt.text().equals("Value") && page.deletetxt.text().equals("Delete"))
			{println("ALL Title and labeled of ADD Configuration verified!!")}
		
			else println("None of the title and label verified")

		waitFor(waitTime){	page.saveconfig.isDisplayed()
				page.cancelconfig.isDisplayed()}
				page.saveconfig.click()
		
		waitFor(waitTime){	page.reqfielderror.isDisplayed()}
				if(page.reqfielderror.text()=="This field is required"){
				println("error message verified")}
				page.cancelconfig.click()	

	}



	def "Verify export add configuration for given values from files and save in ON state" (){
	
	when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage				
		when:
		at AdminPage
		String streamValue = page.getStream()
		String typeValue   = page.getType()
		String nameValue   = page.getExportName()
		String valueValue  = page.getValue()

		waitFor(waitTime){	page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()}
				page.addconfig.click()
		then:
		waitFor(waitTime){	page.addconfigpopup.isDisplayed()
				page.addconfigtxt.isDisplayed()
				if(page.streamtxt.isDisplayed())	{page.inputstream.value(streamValue)}
				if(page.typetxt.isDisplayed())		{page.inputtype.value(typeValue)}
				page.propertiestxt.isDisplayed()
				if(page.nametxt.isDisplayed())		{page.inputnamefrst.value(nameValue)}
				if(page.valuetxt.isDisplayed())		{page.inputvaluefrst.value(valueValue)}
				page.deletetxt.isDisplayed()
			}
				
								
		waitFor(waitTime){	page.saveconfig.isDisplayed()
				page.cancelconfig.isDisplayed()}
				page.saveconfig.click()

		waitFor(waitTime){	page.confirmpopupask.isDisplayed()
				page.confirmnobtn.isDisplayed()
				page.confirmyesbtn.isDisplayed()				
			}
				
				if(page.confirmpopupask.text()=="Are you sure you want to save?"){
				page.confirmyesbtn.click()
				println("Records saved successfully")}
				
				try{	while(true){
				page.confirmyesbtn.click()
				if(page.confirmpopupask.isDisplayed()==false)	
				break
				}}catch(org.openqa.selenium.StaleElementReferenceException e){
				}catch(geb.error.RequiredPageContentNotPresent e){}
				
				
				
		try{		if(!page.exportbtn.isDisplayed() || !page.dbmonitorerrormsgpopup.isDisplayed()){page.confirmyesbtn.click()}
				else println("confirm pop up for save Yes clicked")

				if(waitFor(waitTime){page.dbmonitorerrormsgpopup.isDisplayed() && page.clickdbmonitorerrormsg.isDisplayed()}){
				page.clickdbmonitorerrormsg.click()
				println("Since due to late timing, dbMonitor error message displayed")}

			}catch (geb.error.RequiredPageContentNotPresent e)
			{println("This occurs due to error message of dbmonitor")}
			catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")}
				
				waitFor(waitTime){page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()
				page.ksafetyValue.isDisplayed()}
				page.exportbtn.click()

//for slow monitor case

		
		try{		if(!page.exportbtn.isDisplayed() || !page.dbmonitorerrormsgpopup.isDisplayed()){page.confirmyesbtn.click()}
				else println("confirm pop up for save Yes clicked")

				if(waitFor(waitTime){page.dbmonitorerrormsgpopup.isDisplayed() && page.clickdbmonitorerrormsg.isDisplayed()}){
				page.clickdbmonitorerrormsg.click()
				println("Since due to late timing, dbMonitor error message displayed")}

			}catch (geb.error.RequiredPageContentNotPresent e)
			{println("This occurs due to error message of dbmonitor")}
			catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")}
				
			
				waitFor(waitTime){page.belowexportbtn.isDisplayed()
					    }
					if(!page.belowexportbtn.isDisplayed()){page.exportbtn.click()}else
				page.belowexportbtn.click()
			
					
		try{
		waitFor(waitTime){	page.addconfig.isDisplayed()
				if(page.onstatetxt.isDisplayed()){println("It is in ON STATE")}else println("It is in OFF STATE")				
				page.belowexportnametxt.isDisplayed()
				page.belowexportvaluetxt.isDisplayed()}
			}catch (geb.error.RequiredPageContentNotPresent e){println("element couldn't found")
			}catch(geb.waiting.WaitTimeoutException e){println("Took time due to condition mismatch")}
				if(page.belowexportnametxt.text().equals(nameValue) && page.belowexportvaluetxt.text().equals(valueValue))
				{println("Name and its value given from the file matched in ON STATE!! \n Name: " +belowexportnametxt.text())
				 println("It's value: " +page.belowexportvaluetxt.text())}
				else println("Value from the file did not matched")
	}



	def "Verify export add configuration for given duplicate values of Stream from file and validating the errors" (){
	
	when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage				
		when:
		at AdminPage
		String streamValue = page.getStream()
		String typeValue   = page.getType()
		String nameValue   = page.getExportName()
		String valueValue  = page.getValue()

		waitFor(waitTime){	page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()}
				page.addconfig.click()
		then:
		waitFor(waitTime){	page.addconfigpopup.isDisplayed()
				page.addconfigtxt.isDisplayed()
				if(page.streamtxt.isDisplayed())	{page.inputstream.value(streamValue)}
				if(page.typetxt.isDisplayed())		{page.inputtype.value(typeValue)}
				page.propertiestxt.isDisplayed()
				if(page.nametxt.isDisplayed())		{page.inputnamefrst.value(nameValue)}
				if(page.valuetxt.isDisplayed())		{page.inputvaluefrst.value(valueValue)}
				page.deletetxt.isDisplayed()
			}
				
								
		waitFor(waitTime){	page.saveconfig.isDisplayed()
				page.cancelconfig.isDisplayed()}
				page.saveconfig.click()

		waitFor(waitTime){	page.confirmpopupask.isDisplayed()
				page.confirmnobtn.isDisplayed()
				page.confirmyesbtn.isDisplayed()				
			}
				if(page.confirmpopupask.text()=="Are you sure you want to save?"){
				page.confirmyesbtn.click()}
				
				try{	while(true){
				page.confirmyesbtn.click()
				if(page.confirmpopupask.isDisplayed()==false)	
				break
				}}catch(org.openqa.selenium.StaleElementReferenceException e){
				}catch(geb.error.RequiredPageContentNotPresent e){}
				
		
		waitFor(waitTime){	page.samestreamnameerrorpopup.isDisplayed()
				page.samestreamnameerrorOk.isDisplayed()}			
				page.samestreamnameerrorOk.click()
				println("same stream error name verified")
							
	}



	

	


		def "Verify export add configuration for given values from files and save in OFF state" (){
	
	when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage				
		when:
		at AdminPage
		String streamValueScnd	= page.getStreamNxt()
		String typeValue   	= page.getType()
		String nameValue   	= page.getExportName()
		String valueValue  	= page.getValue()

		waitFor(waitTime){	page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()}
				page.addconfig.click()
		then:
		waitFor(waitTime){	page.addconfigpopup.isDisplayed()
				page.addconfigtxt.isDisplayed()
				if(page.streamtxt.isDisplayed())	{page.inputstream.value(streamValueScnd)}
				if(page.typetxt.isDisplayed())		{page.inputtype.value(typeValue)}
				page.propertiestxt.isDisplayed()
				if(page.nametxt.isDisplayed())		{page.inputnamefrst.value(nameValue)}
				if(page.valuetxt.isDisplayed())		{page.inputvaluefrst.value(valueValue)}
				page.deletetxt.isDisplayed()
			}
			try{	
		waitFor(waitTime){   	if(page.addconfigcheckonbox.value()=="on" && page.addconfigcheckonbox.isChecked()){page.checkboxtest.click()

				println("clicked")}}

		waitFor(waitTime){	if(page.checkboxofftxt.text()=="Off"){println("OFF state")}else
				page.checkboxtest.click()}
			}catch (geb.error.RequiredPageContentNotPresent e)
			{println("This occurs due to element not found")}
			catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")}					
				page.checkboxtest.click()
				println("check box clicked verified! for OFF state")
				
		
		   		if(page.checkboxofftxt.text()=="Off"){println("OFF state")}else 
				page.checkboxtest.click()

		waitFor(waitTime){	page.saveconfig.isDisplayed()
				page.cancelconfig.isDisplayed()}
				page.saveconfig.click()

		waitFor(waitTime){	page.confirmpopupask.isDisplayed()
				page.confirmnobtn.isDisplayed()
				page.confirmyesbtn.isDisplayed()				
			}
				if(page.confirmpopupask.text()=="Are you sure you want to save?"){
				page.confirmyesbtn.click()
				println("Records saved successfully")}
				
			try{	while(true){
				page.confirmyesbtn.click()
				if(page.confirmpopupask.isDisplayed()==false)	
				break
				}}catch(org.openqa.selenium.StaleElementReferenceException e){
				}catch(geb.error.RequiredPageContentNotPresent e){}
				
		try{		if(!page.exportbtn.isDisplayed() || !page.dbmonitorerrormsgpopup.isDisplayed()){page.confirmyesbtn.click()}
				else println("confirm pop up for save Yes clicked")

				if(waitFor(waitTime){page.dbmonitorerrormsgpopup.isDisplayed() && page.clickdbmonitorerrormsg.isDisplayed()}){
				page.clickdbmonitorerrormsg.click()
				println("Since due to late timing, dbMonitor error message displayed")}

			}catch (geb.error.RequiredPageContentNotPresent e)
			{println("This occurs due to error message of dbmonitor")}
			catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")}
				
				waitFor(waitTime){page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()
				page.ksafetyValue.isDisplayed()}
				page.exportbtn.click()

//for slow processing in dbmonitor 

		
		try{		if(!page.exportbtn.isDisplayed() || !page.dbmonitorerrormsgpopup.isDisplayed()){page.confirmyesbtn.click()}
				else println("confirm pop up for save Yes clicked")

				if(waitFor(waitTime){page.dbmonitorerrormsgpopup.isDisplayed() && page.clickdbmonitorerrormsg.isDisplayed()}){
				page.clickdbmonitorerrormsg.click()
				println("Since due to late timing, dbMonitor error message displayed")}

			}catch (geb.error.RequiredPageContentNotPresent e)
			{println("This occurs due to error message of dbmonitor")}
			catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")}
				
			
				waitFor(waitTime){page.belowexportbtn.isDisplayed()
					    }
					if(!page.belowexportbtn.isDisplayed()){page.exportbtn.click()}else
				page.belowexportbtn.click()
			
					
		try{
		waitFor(waitTime){	page.addconfig.isDisplayed()}
				if(page.offstatetxt.isDisplayed()){println("It is in OFF STATE")}else println("It is in ON STATE")						
	}catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")
	}catch (geb.error.RequiredPageContentNotPresent e){println("element content not found")}

		
}



	def "Verify export EDIT configuration and add property from files, save it and delete property in ON state" (){
	
	when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage				
		when:
		at AdminPage
		String streamValue = page.getStream()
		String typeValue   = page.getType()
		String nameValue   = page.getExportName()
		String valueValue  = page.getValue()
		String nameScnd	   = page.getExportNamescnd()
		String valueScnd   = page.getExportValuescnd()

		waitFor(waitTime){	page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()}
				page.exportbtn.click()
		then:
		
		try{		
		waitFor(waitTime){	page.exporteditbtn.isDisplayed()
				page.onstatetxt.isDisplayed()}
				if(page.onstatetxt.text()=="On")	{page.exporteditbtn.click()}
			}catch (geb.error.RequiredPageContentNotPresent e){println("element couldn't found")
			}catch(geb.waiting.WaitTimeoutException e){println("Took time due to condition mismatch")}	
				
				
				if(page.inputnamefrst.isDisplayed() && page.inputvaluefrst.isDisplayed())
				{page.addproperty.click()}
		try {
		waitFor(waitTime){    if(!page.inputnamescnd.isDisplayed() && !page.inputvaluescnd.isDisplayed()){page.addproperty.click()}
				}
			}catch (geb.error.RequiredPageContentNotPresent e){println("element couldn't found")
			}catch(geb.waiting.WaitTimeoutException e){println("Took time due to condition mismatch")}	
		waitFor(waitTime){	if(page.inputnamescnd.isDisplayed()){page.inputnamescnd.value(nameScnd)}
				if(page.inputvaluescnd.isDisplayed()){page.inputvaluescnd.value(valueScnd)}
			     }
		
							
		waitFor(waitTime){	page.saveconfig.isDisplayed()
				page.cancelconfig.isDisplayed()}
				page.saveconfig.click()

		waitFor(waitTime){	page.confirmpopupask.isDisplayed()
				page.confirmnobtn.isDisplayed()
				page.confirmyesbtn.isDisplayed()				
			}
				if(page.confirmpopupask.text()=="Are you sure you want to save?"){
				page.confirmyesbtn.click()
				println("Records saved successfully")}

			try{	while(true){
				page.confirmyesbtn.click()
				if(page.confirmpopupask.isDisplayed()==false)	
				break
				}}catch(org.openqa.selenium.StaleElementReferenceException e){
				}catch(geb.error.RequiredPageContentNotPresent e){}
				
	
		try{		if(waitFor(waitTime){page.dbmonitorerrormsgpopup.isDisplayed() && page.clickdbmonitorerrormsg.isDisplayed()}){
				page.clickdbmonitorerrormsg.click()
				println("Since due to late timing, dbMonitor error message displayed")}

			}catch (geb.error.RequiredPageContentNotPresent e)
			{println("This occurs due to error message of dbmonitor")}
			catch(geb.waiting.WaitTimeoutException e){println("Time exceed more than the given waiting time")}
				
				waitFor(waitTime){page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()
				page.ksafetyValue.isDisplayed()}
				
				waitFor(waitTime){page.belowexportbtn.isDisplayed()
					    }
					if(!page.belowexportbtn.isDisplayed()){page.exportbtn.click()}else
				page.belowexportbtn.click()
			
					
		try{
		waitFor(waitTime){	page.addconfig.isDisplayed()
				if(page.onstatetxt.isDisplayed() && page.onstatetxt.text()=="On"){println("It is in ON STATE")}else println("It is in OFF STATE")				
				page.belowexportnametxt.isDisplayed()
				page.belowexportvaluetxt.isDisplayed()}
			}catch (geb.error.RequiredPageContentNotPresent e){println("element couldn't found")
			}catch(geb.waiting.WaitTimeoutException e){println("Took time due to condition mismatch")}


				if(page.belowexportnametxt.text().equals(nameValue) && page.belowexportvaluetxt.text().equals(valueValue))
				{println("Name and its value given from the file matched in ON STATE!! \n Name: " +belowexportnametxt.text())
				 println("It's value: " +page.belowexportvaluetxt.text())}

				if(page.belowexportnamescndtxt.text().equals(nameScnd) && page.belowexportvaluescndtxt.text().equals(valueScnd))
				{println("second Name and its value given from the file matched in ON STATE!! \n Second Name: " +belowexportnamescndtxt.text())          
				println("It's value: " +page.belowexportvaluescndtxt.text())}                   
								
				
				else println("Value from the file did not matched")	

			when:'edit button again clicked for delete property'	
			waitFor(waitTime){page.exporteditbtn.isDisplayed()
				page.onstatetxt.isDisplayed()}
				if(page.onstatetxt.text()=="On")	{page.exporteditbtn.click()}
			waitFor(waitTime){if(page.inputnamescnd.isDisplayed() && page.inputvaluescnd.isDisplayed()){page.deletescndproperty.click()}
				}			

				waitFor(waitTime){	page.saveconfig.isDisplayed()
				page.cancelconfig.isDisplayed()}
				page.saveconfig.click()

		waitFor(waitTime){	page.confirmpopupask.isDisplayed()
				page.confirmnobtn.isDisplayed()
				page.confirmyesbtn.isDisplayed()				
			}
				if(page.confirmpopupask.text()=="Are you sure you want to save?"){
				page.confirmyesbtn.click()
				println("Edited Records saved successfully")}

			then:'check the updated results'
				
		try{
				if(waitFor(waitTime){!page.belowexportnametxt.isDisplayed()}){page.belowexportbtn.click()}
					    
									
			}catch(geb.waiting.WaitTimeoutException e){println("Took time for element to find.")}
					
		try{
		waitFor(waitTime){	page.addconfig.isDisplayed()
				page.belowexportnametxt.isDisplayed()
				page.belowexportvaluetxt.isDisplayed()}
			}catch (geb.error.RequiredPageContentNotPresent e){println("element couldn't found")
			}catch(geb.waiting.WaitTimeoutException e){println("Took time due to condition mismatch")}

			if(page.belowexportnametxt.text().equals(nameValue) && page.belowexportvaluetxt.text().equals(valueValue))
				{println("updated name where second name and its value is deleted \n Updated Name: " +belowexportnametxt.text())          
				println("updated value: " +page.belowexportvaluetxt.text())}     
			else println("updated file name and its value mis matched")
	}	

	


	
	def "verify Delete Configuration for export setting in ON STATE"(){
		when: 'click the Admin link (if needed)'
		page.openAdminPage()
	
		then: 'should be on Admin page'
		at AdminPage
		when:
		at AdminPage

		waitFor(waitTime){	page.exportbtn.isDisplayed()
			   	page.addconfig.isDisplayed()}
				page.exportbtn.click()
		then:
				
		waitFor(waitTime){	page.exporteditbtn.isDisplayed()}
		page.exporteditbtn.click()

		waitFor(waitTime){	page.deleteconfigurations.isDisplayed()}

		int count = 0
		while (count < numberOfTrials) {
			count++
			try {
				waitFor(waitTime) {
					page.deleteconfigurations.click()
					page.deleteYes.isDisplayed()
					page.deleteNo.isDisplayed()
				}
				break
			}
			catch(geb.waiting.WaitTimeoutException e) {
				println("Retrying")
			}
		}

		page.deleteYes.isDisplayed()
		page.deleteNo.isDisplayed()
		if(page.deleteconfirmation.text().equals("Are you sure you want to delete?")){println("delete confirmation verified in ON STATE")}
		page.deleteYes.click()
		waitFor(waitTime){	page.noconfigtxt.isDisplayed()}
		if(page.noconfigtxt.text()=="No configuration available."){println("No Configuration verified")}
	}


	def "verify Delete Configuration for export setting in OFF STATE"(){

	when: 'click the Admin link (if needed)'
			page.openAdminPage()
	
		then: 'should be on Admin page'
			at AdminPage				
		when:
		at AdminPage
		waitFor(waitTime){
			page.exportbtn.isDisplayed()
			page.addconfig.isDisplayed()
		}
		page.exportbtn.click()
		then:
		waitFor(waitTime){	page.exporteditbtn.isDisplayed()}
		page.exporteditbtn.click()
	
		waitFor(waitTime){	page.deleteconfigurations.isDisplayed()}

		int count = 0
		while (count < numberOfTrials) {
			count++
			try {
				waitFor(waitTime) {
					page.deleteconfigurations.click()
					page.deleteYes.isDisplayed()
					page.deleteNo.isDisplayed()
				}
				break
			}
			catch(geb.waiting.WaitTimeoutException e) {
				println("Retrying")
			}
		}

		if(page.deleteconfirmation.text().equals("Are you sure you want to delete?")){println("delete confirmation verified in OFF STATE")}
		page.deleteYes.click()
		waitFor(waitTime){	page.noconfigtxt.isDisplayed()}
				if(page.noconfigtxt.text()=="No configuration available."){println("No Configuration verified")}
	}
}
