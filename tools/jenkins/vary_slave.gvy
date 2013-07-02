import hudson.slaves.*

def jenkins = jenkins.model.Jenkins.instance;

stack1 = ["volt12a","volt12b-RH6","volt12c","volt12d","volt12e-U10.04","volt12f-U10.04"];
controller1 = 'volt12a-controller-of-volt12a-b-c-d-e-f'

stack2 = ["volt12i","volt12j-RH6","volt12k","volt12l","volt12m-U10.04","volt12n-U10.04"];
controller2 = 'volt12i-controller-of-volt12i-j-k-l-m-n'

stack3 = ["volt3a","volt3b","volt3c","volt3d","volt3e","volt3f", "volt3g", "volt3h"];
controller3 = 'volt3j-controller-of-volt3a-b-c-d-e-f-g-h-i'

stack4 = ["volt12q-C6","volt12r-C6","volt12s-C6","volt12t-C6","volt12u-C6","volt12v-C6"];
controller4 = 'volt12q-controller-of-votl12q-r-s-t-u-v'

stacks = [controller1, stack1, controller2, stack2, controller3, stack3, controller4, stack4]

slavelist = stack4
setSlavesOffline = false
controller = controller4
setControllerOffline = !setSlavesOffline

   c = Jenkins.instance.getComputer(controller).setTemporarilyOffline(setControllerOffline, new OfflineCause.ByCLI("by jenkins"))

    for (slave in slavelist)
    {   
        Jenkins.instance.getComputer(slave).setTemporarilyOffline(setSlavesOffline, new OfflineCause.ByCLI("by jenkins"))
    }
/*
   //computer.setAcceptingTasks(true); // this sets that suspended thing not sure what it is useful for
   //println("Slave '$slave.nodeName' going OFFLINE");

//wait loop for busy
    for (ex in computer.getExecutors()) {
      if (ex.isBusy()) {
            println ("$slave.nodeName is busy");
            //Thread.sleep(60000);
      }

   computer.setTemporarilyOffline(false, new OfflineCause.ByCLI("by jenkins"));

    }
*/
