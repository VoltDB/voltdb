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
        title 					{ $("h1", text:"Directories") }

        rootTitle 				{ $("td", text:"Root (Destination)") }
        snapshotTitle 			{ $("td", text:"Snapshot") }
        exportOverflowTitle 	{ $("td", text:"Export Overflow") }
        commandLogsTitle 		{ $("td", text:"Command Log") }
        commandLogSnapshotTitle { $("td", text:"Command Log Snapshots") }

        rootValue 				{ $("#voltdbroot") }
        snapshotValue 			{ $("#snapshotpath") }
        exportOverflowValue 	{ $("#exportOverflow") }
        commandLogsValue 		{ $("#commandlogpath") }
        commandLogSnapshotValue { $("#commandlogsnapshotpath") }
    }

}
