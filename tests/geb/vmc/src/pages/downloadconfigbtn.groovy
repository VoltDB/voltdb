package vmcTest.pages

import geb.Module

import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by lavthaiba on 2/20/2015.
 */
class downloadconfigbtn extends Module {
    static content = {
        downloadconfigurationbutton { $("a", text:"Download Configuration") }
    }
}
