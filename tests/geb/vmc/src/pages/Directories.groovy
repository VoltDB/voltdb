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

        rootTitle 				{ $("#voltdbroot").previous() }
        snapshotTitle 			{ $("#snapshotpath").previous() }
        exportOverflowTitle 	{ $("#exportOverflow").previous() }
        commandLogsTitle 		{ $("#commandlogpath").previous() }
        commandLogSnapshotTitle { $("#commandlogsnapshotpath").previous() }
        drOverflowTitle { $("#droverflowpath").previous() }

        rootValue 				{ $("#voltdbroot") }
        snapshotValue 			{ $("#snapshotpath") }
        exportOverflowValue 	{ $("#exportOverflow") }
        commandLogsValue 		{ $("#commandlogpath") }
        commandLogSnapshotValue { $("#commandlogsnapshotpath") }
        drOverflowValue { $("#droverflowpath") }
    }

}
