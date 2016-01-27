package vmcTest.pages

import geb.Module
import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by anrai on 2/12/15.
 */

class Header extends Module {
    static content = {
        banner                    	{ $("#headerMain") }
        image                     	{ $(class:"logo") }
        tabDBMonitor              	{ $("#navDbmonitor") }
        tabAdmin                  	{ $("#navAdmin") }
        tabSchema                 	{ $("#navSchema") }
        tabSQLQuery               	{ $("#navSqlQuery") }
        usernameInHeader            { $("#btnlogOut") }
        logout                    	{ $("#logOut") }
        showHelp                 	{ $("#showMyHelp") }
        help                        { $("#userSection > li:nth-child(4) > div > ul > li > a") }
        popup                     	{ $(class:"popup_content10") }
        popupTitle                	{ $(class:"overlay-title helpIcon ", text:"Help") }
        popupClose                	{ $(class:"popup_close") }
        logoutPopup               	{ $(class:"popup_content2") }
        logoutPopupTitle          	{ $(class:"overlay-title ") }
        logoutPopupOkButton       	{ $("#A1") }
        logoutPopupCancelButton   	{ $("#btnCancel") }
    }

	def String getUsername() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/users.txt"))
        String user

        while((user = br.readLine()) != "#username") {
        }

        user = br.readLine()

        return user
    }
    
    def String getPassword() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/users.txt"))
        String password

        while((password = br.readLine()) != "#password") {
        }

        password = br.readLine()

        return password
    }

    def boolean checkShowHelp() {
        waitFor(30) { help.isDisplayed() }
        help.click()
        waitFor(30) { showHelp.isDisplayed() }
    }
    
    def boolean checkIfHelpIsOpen() {
        checkShowHelp()
        showHelp.click()
        
        popupTitle.isDisplayed()
        popupClose.isDisplayed()
        popupTitle.text().toLowerCase().contains("help".toLowerCase());
    }
}
