import jenkins.*
import jenkins.model.*
import hudson.*
import hudson.model.*
import hudson.slaves.*

def jenkins = jenkins.model.Jenkins.instance;


def stack1 = ["volt12a-controller-of-volt12a-b-c-d-e-f", "volt12a-U12.04","volt12b-RH6","volt12c-RH5","volt12d-U12.04","volt12e-U10.04","volt12f-U10.04"];
//String controller1 = [ stack1[0] ]

def stack2 = ["volt12i-controller-of-volt12i-j-k-l-m-n", "volt12i-U12.04","volt12j-RH6","volt12k-U12.04","volt12l-U12.04","volt12m-U10.04","volt12n-U10.04"];
//String controller2 = [ stack2[0] ]

//def stack3 = [ "volt3j-controller-of-volt3a-b-c-d-e-f-g-h-i", "volt3a","volt3b","volt3c","volt3d","volt3e","volt3f", "volt3g", "volt3h", "volt3i"];
def stack3 = ["volt3j-controller-of-volt3a-b-c-d-e-f-g-h-i"]

def stack4 = [ "volt12q-controller-of-votl12q-r-s-t-u-v","volt12q-C6","volt12r-C6","volt12s-C6","volt12t-C6","volt12u-C6","volt12v-C6"];
def controller4 = [ stack4[0] ]

def controller5 = [ "volt13a-controller-of-votl13a-b-c-d-e-f" ]

def controller6 = [ "volt5a-controller-of-volt5b-c-d-volt7b-c-d-frankencluster" ]

def controller7 = [ "volt13i-controller-of-volt13i-j-k-l-m-n" ]

def controller8 = [ "volt10d-controller-of-volt10d-c-b-a" ]

void varyHost(host, OfflineIsTrue) {
       slave = Jenkins.instance.getComputer(host)
       msg =  OfflineIsTrue ? "OFFLINE" : "ONLINE"
       println("Slave '$slave.nodeName' going " + msg + "...")
       slave.setTemporarilyOffline(OfflineIsTrue, new OfflineCause.ByCLI("by jenkins"))
}

boolean isIdle(host) {

   slave = Jenkins.instance.getComputer(host)
   //computer.setAcceptingTasks(true); // this sets that suspended thing not sure what it is useful for
   println("Slave '$slave.nodeName' checking for IDLE...");
   return slave.isIdle()

}

void setRotation(hour, stacks) {

   for (s in stacks) {

      if (hour.equals("day")) {

         // set controller offline
         varyHost(s[0], true)

      } else {

         // set slaves offline
         for ( h in s.subList(1, s.size())) { varyHost(h, true) }

      }
   }

   while (stacks.size() > 0) {
      //println ("stacks: $stacks")

      def it = stacks.listIterator();

      while (it.hasNext()) {

         s = it.next()

         if (hour.equals("day")) {

               // daytime: wait for controller idle, vary slaves online
               if (isIdle(s[0])) {
                  for (h in s.subList(1, s.size())) { varyHost(h, false) }
                  stacks.remove(s)
                  it = stacks.listIterator();
                  continue
               }

         } else {

           // nighttime: wait for slaves idle, vary controller online
            ready = true
            for (h in s.subList(1, s.size())) {
               if (!isIdle(h)) {
                  ready = false
                  break
               }
            }
            if (ready) {
               varyHost(s[0], false)
               stacks.remove(s)
               it = stacks.listIterator();
               continue
            }
         }
      } // for
      if (stacks.size() == 0) { return }
      sleep(60000)
   } //while
}

h = new Date().getHours()
println ("hour: " + h)
if (h < 17)
   //setRotation("night", [  stack4 ])
   setRotation("day", [ stack1, stack2, stack3 ])
else
   setRotation("night", [ stack1, stack2, stack3, controller5, controller6, controller7, controller8 ])

