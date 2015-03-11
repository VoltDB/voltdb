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
	 	title			{ $("h1", text:"Network Interfaces") }

		portNameTitle			{ $(text:"Port Name") }
		clusterSettingTitle		{ $(text:"Cluster Settings") }
		serverSettingTitle		{ $(text:"Server Settings") }
		clientPortTitle			{ $(text:"Client Port") }
		adminPortTitle			{ $(text:"Admin Port") }
		httpPortTitle 			{ $(text:"HTTP Port") }
		internalPortTitle		{ $(text:"Internal Port") }
		zookeeperPortTitle		{ $(text:"Zookeeper Port") }
		replicationPortTitle	{ $(text:"Replication Port") }
	
		clusterClientPortValue			{ $("#clusterClientport") }
		clusterAdminPortValue			{ $("#clusterAdminport") }
		clusterHttpPortValue			{ $("#clusterHttpport") }
		clusterInternalPortValue		{ $("#clusterInternalPort") }
		clusterZookeeperPortValue		{ $("#clusterZookeeperPort") }
		clusterReplicationPortValue		{ $("#clusterReplicationPort") }	     
    }

}
