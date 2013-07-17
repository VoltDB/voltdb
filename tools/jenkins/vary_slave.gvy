import jenkins.*
import jenkins.model.*
import hudson.*
import hudson.model.*
import hudson.slaves.*

def jenkins = jenkins.model.Jenkins.instance;

def stack1 = ["volt12a","volt12b-RH6","volt12c","volt12d","volt12e-U10.04","volt12f-U10.04"];
String controller1 = "volt12a-controller-of-volt12a-b-c-d-e-f"

def stack2 = ["volt12i","volt12j-RH6","volt12k","volt12l","volt12m-U10.04","volt12n-U10.04"];
String controller2 = "volt12i-controller-of-volt12i-j-k-l-m-n"

def stack3 = ["volt3a","volt3b","volt3c","volt3d","volt3e","volt3f", "volt3g", "volt3h", "volt3i"];
String controller3 = "volt3j-controller-of-volt3a-b-c-d-e-f-g-h-i"

def stack4 = ["volt12q-C6","volt12r-C6","volt12s-C6","volt12t-C6","volt12u-C6","volt12v-C6"];
String controller4 = "volt12q-controller-of-votl12q-r-s-t-u-v"

def stack5 = ["volt12o-C6","volt12p-C6"];
String controller5 = "volt12o-controller-of-votl12o-p"


void setHostOffline(host, OfflineIsTrue) {

       slave = Jenkins.instance.getComputer(host)
       msg =  OfflineIsTrue ? "OFFLINE" : "ONLINE"
       println("Slave '$slave.nodeName' going " + msg + "...")
       slave.setTemporarilyOffline(OfflineIsTrue, new OfflineCause.ByCLI("by jenkins"))
}

void waitForIdle(host) {

   slave = Jenkins.instance.getComputer(host)

   //computer.setAcceptingTasks(true); // this sets that suspended thing not sure what it is useful for
   println("Slave '$slave.nodeName' checking for IDLE...");

   //wait loop for busy
    while (!slave.isIdle()) {
            Thread.yield();
            Thread.sleep(60000);
   }
}

void setRotation(controller, slavelist) {

    h = new Date().getHours()
    println ("hour: " + h)

    if (h<17) {

       // day rotation
       setHostOffline(controller, true)
       for (slave in slavelist) {setHostOffline(slave, true) } 
       if (slavelist) waitForIdle(controller)
       for (slave in slavelist) { setHostOffline(slave, false) }

    } else {

       // night rotation
       setHostOffline(controller, true)
       for (slave in slavelist) { setHostOffline(slave, true) }
       for (slave in slavelist) {waitForIdle(slave) }
       if (controller) setHostOffline(controller, false)
    }
}


setRotation(controller1, stack1)
//setRotation(controller2, null)
//setRotation(controller3, null)
//setRotation(controller4, null)

/*

slavelist = stack1
setSlavesOffline = false
controller = controller1
setControllerOffline = true //!setSlavesOffline

   Jenkins.instance.getComputer(controller).setTemporarilyOffline(setControllerOffline, new OfflineCause.ByCLI("by jenkins"))

    for (slave in slavelist)
    {   
        jenkins.model.Jenkins.instance.getComputer(slave).setTemporarilyOffline(setSlavesOffline, new OfflineCause.ByCLI("by jenkins"))
    }
*/
