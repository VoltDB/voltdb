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

class Directories extends Module {
    static content = {
        title { $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.headerAdminContent > h1") }

       	rootTitle { $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        snapshotTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        exportOverflowTitle { $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        commandLogsTitle { $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
        commandLogSnapshotTitle { $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(5) > td:nth-child(1)") }

        rootValue { $("#voltdbroot") }
        snapshotValue { $("#snapshotpath") }
        exportOverflowValue { $("#exportOverflow") }
        commandLogsValue { $("#commandlogpath") }
        commandLogSnapshotValue { $("#commandlogsnapshotpath") }
    }

}
