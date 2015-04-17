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

class NetworkInterfaces extends Module {

    static content = {
        title			                { $("h1", text:"Network Interfaces") }

        portNameTitle			        { $("th", text:"Port Name") }
        clusterSettingTitle		        { $("th", text:"Cluster Settings") }
        serverSettingTitle		        { $("th", text:"Server Settings") }
        clientPortTitle			        { $("#clientport").previous() }
        adminPortTitle			        { $("#adminport").previous() }
        httpPortTitle 			        { $("#httpport").previous() }
        internalPortTitle		        { $("#internalPort").previous() }
        zookeeperPortTitle		        { $("#zookeeperPort").previous() }
        replicationPortTitle	        { $("#replicationPort").previous() }

        clusterClientPortValue			{ $("#clusterClientport") }
        clusterAdminPortValue			{ $("#clusterAdminport") }
        clusterHttpPortValue			{ $("#clusterHttpport") }
        clusterInternalPortValue		{ $("#clusterInternalPort") }
        clusterZookeeperPortValue		{ $("#clusterZookeeperPort") }
        clusterReplicationPortValue		{ $("#clusterReplicationPort") }

        serversettingclientvalue		{ $("#clientport")}
        serversettingadminvalue			{ $("#adminport")}
        serversettinghttpvalue			{ $("#httpport")}
        serversettinginternalvalue		{ $("#internalPort")}
        serversettingzookeepervalue		{ $("#zookeeperPort")}
        serversettingreplicationvalue	{ $("#replicationPort")}
    }

}
