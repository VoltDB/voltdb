import geb.*
import geb.spock.*
import geb.driver.CachingDriverFactory
import org.openqa.selenium.interactions.Actions
import spock.lang.*

class StudioWebPage extends Page {

   
    static url = 'file://localhost/' + new File("../../../obj/release/dist/tools/studio.web/index.htm").getCanonicalPath()
    static at = { title == "VoltDB Web Studio" }
}

class StudioWebDiag extends GebReportingSpec {

    def resources = "src/test/resources"
    def dist = "../../../obj/release/dist"
    def studioweb = "${dist}/tools/studio.web"

    ///// Server MUST have been initiated/////
        def "To StudioWeb"() {

        setup: "Open Studio web"
        to StudioWebPage

        expect: "to be on studio web page"
        at StudioWebPage
    }

    def "Contains important elements"(){

        setup: "Get elements from page. Elements are defined by order, tag, class and visibilty"
        def chk = getEles()

        and: "define correct file of elements"
        def eleFile = new File("${resources}/elems.txt")

        and: "compare page contents to file contents"
        def result = chkElements(chk, eleFile)

        expect: "them to be the same"
        assert result
        
    }

    def "Connect to server"(){

        when: "connect button is clicked"
        $("span.connect").click() // connect button

        then: "test connection"
        $("span.ui-button-text", text: "Test Connection").click() //test connection button

        then: "wait for server response"
        waitFor {$("td.validateTips").text().contains("successful") | $("td.validateTips").text().contains("Unable")}

        then: "get server state"
        def state = $("td.validateTips").text()

        when: "if server available"
        state.contains("successful") //check connection state

        then: "connect to server"
        $("span.ui-button-text", text: "OK").click() //OK button
    }

    def "Check readMe file"(){

        setup: "find correct readme file from test directory"
        def correctFile = new File("${resources}/readme.htm")

        and: "obtain readme file from studio.web directory"
        def chkFile = new File("${studioweb}/readme.htm")

        expect: "them to be the same"
        assert correctFile.getText() == chkFile.getText() & correctFile.size() != 0 

    }

    def "System Stored Procedures"(){

        setup: "make stored procedures visible"
        expandFolds()

        and: "obtain their locations"
        def nav = $("span", text: "Programmability").siblings().find("li",0).find("li",0).find("ul", 0).children().children("span")

        and: "prepare list for procedures"
        def procs = []

        and: "they are compared to correct list of procecures"
        def correctProcs =[]

        and: "procedures are retrieved from correct list in file"
        new File("${resources}/storedProcs.txt").eachLine() {line -> correctProcs.add(line)} //the styored procedure file

        and: "add procedures from page to list to test"
        procs = nav*.text()

        and: "expect them to be the same (no extras, none missing)"
        assert (correctProcs.containsAll(procs) & procs.containsAll(correctProcs))
    }

    def "Disconnect from server"(){

        setup: "Actions interface to enable right click"
        def actions = new Actions(this.getDriver())

        and: "find element in sidebar to right click"
        def clickMe = $("span", text: "Programmability").firstElement()

        when: "context menu is brought up"
        actions.contextClick(clickMe).build().perform()

        then: "find disconnect option"
        def disconn = $("ul", id: "objectbrowsermenu").find("li.disconnect").find("a")

        when: "disconnect is visible"
        disconn.isDisplayed()

        then: "click to disconnect from server"
        disconn.click()

        and: "expect server content to be unavailable"
        assert $("ul#dbbrowser.treeview").children().isEmpty()
    }

    def cleanupSpec(){}

    void expandFolds(){
        def fold1 = $("span", text: "Programmability").siblings("div")
        def fold2 = fold1.siblings().find("li",0).children("div")
        def fold3 = fold2.siblings().find("li",0).children("div")
        fold1.click()
        fold2.click()
        fold3.click()
    }

    def getEles(){

        def count = 0; def attributes = {"${ it.attr("id") }"}
        def all = $('[id]:not(canvas)')
        def returnMe = []
        returnMe = all.collect(attributes)
        returnMe
    }

    def chkElements(def eles, def file){

        def correct = []
        /*right now this generates a new list of procedures if none exits.  This functionality will be moved to 
        a separate method that will also replace readme file and list of stored procedures on test failure  */
        if(file.size() == 0){file.withWriter {out -> eles.each() {out.writeLine(it)} } }
        file.eachLine() {line -> correct.add(line)}
        if(correct == eles){return true
        }else{correct.removeAll(eles) // if correct is not empty, then there are elements mssing from the page
        if (correct.size != 0){ return !(correct.size()) } //groovy coerces nonzero ints to true
        }
    }
}
