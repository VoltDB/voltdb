import jenkins.*
import jenkins.model.*
import hudson.*
import hudson.model.*
import hudson.slaves.*
import java.text.SimpleDateFormat


def jenkins = jenkins.model.Jenkins.instance;

// first jenkins host in a stack is controller, remaining are its hosts

stack_map = [
        other:    [ "",
                    "VMC-Firefox",
                    "jepsen15d",
                    "kafka2",
                    "volt11a",
                    "volt14p-C6",
                    "volt15a-C6",
                    "volt15d-C7",
                    // "volt17a-U16.04","volt17b-U16.04",
                    "volt18c-C7",
                    "volt18d-C7",
                    "volt23cc1","volt23cc2","volt23cc3","volt23cc4","volt23cc5","volt23cc6","volt23cc7","volt23cc8",
                    "volt23dc1","volt23dc2","volt23dc3","volt23dc4","volt23dc5","volt23dc6","volt23dc7","volt23dc8",
                    "volt3k-U14.04",
                    "volt3l-v1-U14.04-java7",
                    "volt5f",
                    "volt7a-U10.04",
                    "voltmini",
                    "voltmini2",
                    "w8kr2-test-odbc",
        ],
        svolt3j:  [ "volt3j-controller-of-volt3a-b-c-d-e-f-g-h-i", /*, "volt3a","volt3b","volt3c","volt3d","volt3e","volt3f", "volt3g", "volt3h", "volt3i"*/],
        svolt5a:  [ "volt5a-controller-of-volt5b-c-d-volt7b-c-d", ],
        svolt10d: [ "volt10d-controller-of-volt10d-c-b-a", /*, "volt10a", "volt10b", "volt10c", "volt10d"*/],
        svolt12a: [ "volt12a-controller-of-volt12a-b-c-d-e-f", "volt12a-U16.04","volt12b-RH6","volt12c-C7","volt12d-U14.04","volt12e-C7","volt12f-U14.04"],
        svolt12i: [ "volt12i-controller-of-volt12i-j-k-l-m-n", "volt12i-U14.04","volt12j-RH6","volt12k-C7","volt12l-U16.04","volt12m-C7","volt12n-U16.04"],
        svolt12q: [ "volt12q-controller-of-volt12q-r-s-t-u-v", "volt12q-C6","volt12r-C6","volt12s-C6","volt12t-C6","volt12u-C6","volt12v-C6"],
        svolt12o: [ "volt12o-controller-of-volt12g-h-o-p-volt13g-h", "volt12o-C6","volt12p-C7","volt13g-C6","volt13h-U14.04","volt12g-U16.04","volt12h-U18.04"],
        svolt12q: [ "volt12q-controller-of-volt12q-r-s-t-u-v", "volt12q-C6","volt12r-C6","volt12s-C6","volt12t-C6","volt12u-C6","volt12v-C6"],
        svolt13a: [ "volt13a-controller-of-volt13a-b-c-d-e-f" ],
        svolt13i: [ "volt13i-controller-of-volt13i-j-k-l-m-n" ],
        svolt14c: [ "volt14c-controller-of-volt14c-b-e-f-g-h", "volt14c-C7","volt14b-C7","volt14e-C7","volt14f-C7","volt14g-C7","volt14h-C7"],
        svolt14i: [ "volt14i-controller-of-volt14i-j-k-l-m-n", "volt14i-cos6-kvm1", "volt14j-C6","volt14k-C6","volt14l-U16.04","volt14m-U14.04","volt14n-U14.04"],
        svolt16a: [ "volt16a-controller-of-volt16a-b-c-d-volt18a-b", "volt16a-C7", "volt16b-C7", "volt16c-C7", "volt16d-C7", "volt18a-C7", "volt18b-C7" ],
        svolt17cc1: ["volt17cc1-controller-of-volt17cc1-cc2-cc3-cc4-cc5-cc6","volt17cc1","volt17cc2","volt17cc3","volt17cc4","volt17cc5","volt17cc6"],
        svolt17dc1: ["volt17dc1-controller-of-volt17dc1-dc2-dc3-dc4-dc5-dc6","volt17dc1","volt17dc2","volt17dc3","volt17dc4","volt17dc5","volt17dc6"],
        svolt18cd: [ "", "volt18c-C7", "volt18d-C7" ],
        svolt19ac1: [ "volt19ac1-controller-of-ac2-thru-ac7","volt19ac1-U18.04","volt19ac2-U18.04","volt19ac3-U18.04","volt19ac4-U18.04","volt19ac5-U18.04","volt19ac6-U18.04" ,"volt19ac7-U18.04","volt19ac8-U18.04"],
        svolt19bc1: ["volt19bc1-controller-of-volt19bc1-bc2-bc3-bc4-bc5-bc6","volt19bc1-U16.04","volt19bc2","volt19bc3","volt19bc4","volt19bc5","volt19bc6"],
        svolt19cc1: ["volt19cc1-controller-of-volt19cc1-c2-c3-c4-c5-c6","volt19cc1-U16.04","volt19cc2","volt19cc3","volt19cc4","volt19cc5","volt19cc6"],
        svolt19dc1: ["volt19dc1-controller-of-volt19dc1-c2-c3-c4-c5-c6","volt19dc1-U16.04","volt19dc2","volt19dc3","volt19dc4","volt19dc5","volt19dc6"],
        svolt20a: [ "volt20ac1-controller-of-volt20ac1-ac2-bc1-bc2-cc1-cc2-dc1-dc2-ec1-ec2-fc1-fc2",
                    "volt20ac1",
                    "volt20ac2",
                    "volt20bc1",
                    "volt20bc2",
                    "volt20cc1",
                    "volt20cc2",
                    "volt20dc1",
                    "volt20dc2",
                    "volt20ec1",
                    "volt20ec2",
                    "volt20fc1",
                    "volt20fc2"],
        sjepsen15d: [ "jepsen15d" ],
        svolt9_ec2: [ "volt9-ec2" ],
        svolt22a:   [ "volt22ac1-controller-of-volt22ac1-ac2-bc1-bc2-cc1-cc2-dc1-dc2-volt23ac1-ac2-bc1-bc2",
                      "volt22ac1",
                      "volt22bc1",
                      "volt22cc1",
                      "volt22dc1",
                      "volt23ac1",
                      "volt23bc1"],
]

def day_hour = 		    Integer.valueOf(build.buildVariableResolver.resolve("day_hour"))
def night_hour = 		Integer.valueOf(build.buildVariableResolver.resolve("night_hour"))

def manual = 			build.buildVariableResolver.resolve("manual").replace(" ","").tokenize(',')
def day_cluster = 		build.buildVariableResolver.resolve("day_cluster").replace(" ","").tokenize(',')
def day_notcluster = 	build.buildVariableResolver.resolve("day_notcluster").replace(" ","").tokenize(',')
def night_cluster = 	build.buildVariableResolver.resolve("night_cluster").replace(" ","").tokenize(',')
def night_notcluster = 	build.buildVariableResolver.resolve("night_notcluster").replace(" ","").tokenize(',')
def always_offline =    build.buildVariableResolver.resolve("offline").replace(" ", "").tokenize(',')

def exitcode = 0

dryRun = false
Offline = true
Online = false

OFFINE_CAUSE_BY_ROTATION = "by rotation"
OFFLINE_CAUSE_BY_MAINT = "offline for maintenance in all rotations"
OFFLINE_CAUSE_BY_CLUSTER = "cluster is reserved"
DATE_PATTERN = "MM/dd/yy HH:mm"

sdf = new SimpleDateFormat(DATE_PATTERN)


void updateOffineReason(host, reason) {
    if (!host)
        return
    computer = Jenkins.instance.getComputer(host)
    if (!computer.isOffline())
        throw new RuntimeException("slave '$host' is not offline but should be")
    computer.setTemporarilyOffline(true, new OfflineCause.ByCLI(reason))
    println("[$host] offline cause: $reason")
    }


Date getSlaveReservation(slave) {
    computer = Jenkins.instance.getComputer(slave)
    if (computer.isOffline()) {
        reason = computer.getOfflineCauseReason()
        println("offline cause: " + reason)
        group = (reason =~ /\s+until\s+(\w.*\w)\s*$/)
        if (group.hasGroup() && group.count > 0) {
            println(group[0])
            if (group[0][1].contains("forever"))
                reservedUntil = new Date(Long.MAX_VALUE)
            else {
                try {
                    reservedUntil = sdf.parse(group[0][1])
                } catch (java.text.ParseException ex) {
                    println ("ERROR sorry can't parse your date: " + group[0][1] + " format must be: $DATE_PATTERN")
                    return null
                }
            }
            println("$slave is reserved until $reservedUntil")
            return reservedUntil
        }
    }
    return null
}

boolean isReserved(hosts) {
    // list of hosts, return true if any are reserved
    Date date = new Date()
    for (h in hosts) {
        if (h) {
            reservation = getSlaveReservation(h)
            if (reservation != null) {
                if (reservation.after(date)) {
                    println("$reservation $date")
                    return true
                } else {
                    println("$h expired reservation reset: $reservation $date")
                    updateOffineReason(h, OFFINE_CAUSE_BY_ROTATION)
                }
            }
        }
    }
    return false
}

void varyHost(host, OfflineIsTrue) {
    if (!host || host.equals("master") || host.equals(""))
        return
    // nb. host="" will resturn the slave object for 'master', which we should never touch
    slave = Jenkins.instance.getComputer(host)
    if (!slave) {
        throw new RuntimeException("ERROR host '$host' is not a valid jenkins slave")
    }
    msg =  OfflineIsTrue ? "OFFLINE" : "ONLINE"
    println("varyHost $host to $msg")
    // if we're already offline going offline we'll preserve the OfflineReasonCause
    reason = slave.getOfflineCauseReason()
    // if going online and host is in offline list, turn it/keep it offline

    always_offline =    build.buildVariableResolver.resolve("offline").replace(" ", "").tokenize(',')

    if (!OfflineIsTrue && (host in always_offline)) {
        OfflineIsTrue = true
        reason = OFFLINE_CAUSE_BY_MAINT
        println ("slave '$host' is in offline list...")
    }
    if (!OfflineIsTrue && isReserved([host])) {
        OfflineIsTrue = true
        //reason = the reservation itself
        println("slave is reserved: $host, reason: $reason")
        return
    }
    println("Slave '$slave.nodeName' going '$msg' with reason: '$reason'")
    if (!dryRun)
        slave.setTemporarilyOffline(OfflineIsTrue, new OfflineCause.ByCLI(reason ? reason : OFFINE_CAUSE_BY_ROTATION))
}

boolean isIdle(host) {
    if (!host)
        return true
    slave = Jenkins.instance.getComputer(host)
    //computer.setAcceptingTasks(true); // this sets that suspended thing not sure what it is useful for
    if (slave == null) {
        throw new RuntimeException("slave is null: $host")
    }
    println("Slave '$slave.nodeName' checking for IDLE...");
    if (dryRun)
        return true
    return slave.isIdle()
}


void setRotation(rotation, stacks_lit) {

    def always_offline =    build.buildVariableResolver.resolve("offline").replace(" ", "").tokenize(',')
    // if cluster members are in the maintenance list, find the cluster master and add it to the offline list too
    adds = []
    for (o in always_offline) {
        stack_map.each { name, stack ->
            if (o in stack && !(name in (always_offline + adds))) {
                adds.add(name)
            }
        }
    }
    always_offline.addAll(adds)
    println(always_offline)

    /* rotation can be:
        notcluster - day rotation is selected: controller offline/nodes online
        cluster - night rotation is selected: controller online/nodes offline
        offline - controller and all nodes left offline
    */

    println "stacks in rotation: ${stacks_lit}"

    stacks = []

    for (sl in stacks_lit) {
        if (sl in stack_map)
            s = stack_map[sl]
        else
            throw new RuntimeException("expected a stack got $sl")
            //s = [sl]  // just a host name ?

        // put everything controllers and slaves offline
        // offline cause is preserved
        for (h in s)
            varyHost(h, Offline)

        // if any of our stack members are in the offline list
        if (sl in always_offline) {
            println("stack offline: $s")
            for (h in s)
                updateOffineReason(h, OFFLINE_CAUSE_BY_MAINT)  // flag all in offline mode
        } else {
            stacks << s
        }
    }

    println "active stacks: ${stacks}"

    // evaluate reservations
    // drop stack(s) that are reserved
    reservations = []
    nstacks = []
    for (stack in stacks) {  //copy?

        println("evaluate reservations on stack: $stack")

        if (stack == null || stack.equals(""))
            continue

        controller = stack.subList(0, 1)
        slaves = stack.subList(1, stack.size())

        reserved_slaves = []
        for (s in slaves) {
            if (isReserved([s]))
                reserved_slaves << s   //reservation conflict possible
        }
        println("r: $reserved_slaves")

        // if the controller is reserved, leave the stack offline in all rotations
        // if any slave(s) are reserved, then stacks other slaves may go online in some rotations

        if (isReserved([controller])) {
            // if any slaves are sumultaneously reserved, flag error and leave the stack offline
            if (reserved_slaves.size() > 0)
                updateOffineReason(controller, "ERROR OFFLINE reservation conflict with slaves: $reserved_slaves")
                for (s in reserved) {
                        updateOffineReason(s, "ERROR OFFLINE reservation conflict with $controller and $reserved_slaves")
                    }
                // and forget this stack
                continue

            // update the offline reason for all slaves under implied reservation
            for (s in slaves)
                updateOffineReason(s, OFFLINE_CAUSE_BY_CLUSTER + " $controller")

            // the controller is reserved so we're done with this stack, we leave it offline in all rotations
            println("controller is reserved: $controller")

            // the reservation is the offlineReason so no need to update the controller
            reservations << [controller]

            // we are done with this stack, no further actions are needed

        } else if (reserved_slaves.size() > 0) {
            // if any slaves are reserved block the controller
            updateOffineReason(controller, "node(s) on this cluster have been reserved: $reserved_slaves")

            // rewrite stack removing controller and all reserved nodes
            nstacks << [""] + slaves.minus(reserved_slaves)

        } else {
            nstacks << stack
        }
    }

    stacks = nstacks

    println "unreserved stacks: ${stacks}"
    println "setting rotation ${rotation}"

    // we wait in this loop for the jenkins slave(s) to go idle and then apply the new configuration
    // drop stacks when the configuration has been applied
    while (stacks.size() > 0) {

        def it = stacks.listIterator();

        while (it.hasNext()) {

            stack = it.next()
            println "stack: $stack"
            if (!stack) {
                stacks.remove(stack)
                it = stacks.listIterator()
                continue
                }

            controller = stack[0]
            slaves = stack.subList(1, stack.size())

            if (rotation.equals("notcluster")) {

                // notcluster: wait for controller idle, vary slaves online
                updateOffineReason(controller, OFFINE_CAUSE_BY_ROTATION)
                if (!controller || isIdle(controller)) {
                    for (h in slaves) {
                        varyHost(h, Online)
                    }
                    stacks.remove(stack)
                    it = stacks.listIterator()
                }


            } else if (rotation.equals("cluster")) {

                // nighttime: wait for slaves idle, vary controller online
                // if any of the cluster's nodes are in the offline list, the cluster cant be used
                if (controller) {
                    idle = true
                    for (h in slaves)
                        idle = idle && isIdle(h)
                    if (!idle)
                        continue

                    varyHost(controller, Online)
                }

                // indicated the slaves are allocated to a cluster even if the controller is hidden (null)
                // due to a reservation or
                for (h in slaves)
                    updateOffineReason(h, "allocated to cluster '$controller'")
                stacks.remove(stack)
                it = stacks.listIterator()


            } else if (rotation.equals("offline")) {
                return
            }


        }

        if (stacks.size() == 0) { return }

        //Thread.sleep(30000)
        break  //only one pass, rescheule in jenkins once a minute
    }
}


void nightRotation(cluster, notcluster) { /* all nights and weekends */
    println "setting night rotation"
    setRotation("notcluster", notcluster)
    setRotation("cluster", cluster)
}

void dayRotation(cluster, notcluster) { /* weekday days */
    println "setting day rotation"
    setRotation("cluster", cluster)
    setRotation("notcluster",   notcluster)
}

void killme() {
    str_search = ["admintools-vary-slaves"]

    for (item in Hudson.instance.items) {
        if (item.disabled)
            continue
        for (name in str_search) {
            if(item.getName() == name) {
                if (item.isInQueue()) {
                    println "Removing from queue " + item.getName()
                    try {
                        item.getQueueItem().doCancelQueue()
                    } catch (Exception e) {}
                }
            }
        }
    }

    for (item in Hudson.instance.items) {
        if (item.disabled)
            continue
        for (name in str_search) {
            if (item.getName() == name) {
                if (item.isBuilding() && item.build.number != build.number) {
                    println "Killing " + item.getName() + " #"+ item.number
                    try {
                        item.builds.getLastBuild().getExecutor().doStop()
                    } catch (Exception e) {}
                }
            }
        }
    }
}

if (manual.size() > 0) {

    /* manual field is order:stack, order:stack, ... */
    println manual.size()
    println manual
    for (m in manual) {
        println m
        def orders = m.tokenize(":")
        def rotation = orders[0]
        def stk = orders[1]
        stack = []
        if (stk == "*") {
            for (s in stack_map) {
                stack << s.key
            }
        }
        else
            stack << stk
        setRotation(rotation, stack)
    }

} else {

    // kill or unqueue any previous rotation
    //killme()

    //setRotation("offline", always_offline)

    d = new Date().getDay() // 0-6 sunday=0
    weekday = (d > 0 && d < 6)
    h = new Date().getHours()

    if (h >= day_hour && h < night_hour) {
        // daytime
        if (weekday){
            // always run the night rotation
            dayRotation(day_cluster, day_notcluster)
        } else {
            nightRotation(night_cluster, night_notcluster)
        }
    } else {
        // night time
        if (weekday) {
            //varyAllServers(true) //offline is True
            nightRotation(night_cluster, night_notcluster)
        } else {
            nightRotation(night_cluster, night_notcluster)
        }
    }
}
return (exitcode==0)

