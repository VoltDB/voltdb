
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
class Footer extends Module {
    static content = {
        banner    	{ $("#mainFooter") }
        text      	{ $("html body div#mainFooter.footer p") }
    }
}
