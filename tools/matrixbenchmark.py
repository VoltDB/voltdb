#!/usr/bin/env python

import os, random, signal
import boto


#
# Documented at https://hzproject.com/trac/wiki/VoltDBOnEC2
#

class HostPool(object):
    """Manages the set of available hosts"""
    def __init__(self):
        object.__init__(self)
        return

    def cleanup(self):
        return

    def getRemotePathString(self):
        return ""

    def hostStringFromHostCount(self, count):
        return ""
        
    def clientStringFromClientCount(self, count):
        return ""

class LocalPool(HostPool):
    """Manages the local VoltDB.com cluster"""
    def __init__(self):
        HostPool.__init__(self)
        # self.hostList =  ['volt3a', 'volt3b', 'volt3c', 'volt3d', 'volt3e', 'volt3f']
        self.hostList =  ['volt3a', 'volt3b', 'volt3c', 'volt3d', 'volt3e']
        self.clientList =  ['volt4a', 'volt4b', 'volt4c']

    def cleanup(self):
        print "Terminating local cluster."
        return

    def getRemotePathString(self):
        return "REMOTEPATH=voltbin/ "

    def hostStringFromHostCount(self, count):
        retval = "HOSTCOUNT=%d " % count
        for i in range(count):
            retval += "HOST=%s " % self.hostList[i]
        return retval
        
    def clientStringFromClientCount(self, count):
        retval = "CLIENTCOUNT=%d " % count
        for i in range(count):
            retval += "CLIENTHOST=%s " % self.clientList[i]
        return retval


class EC2Pool(HostPool):
    """Manages hosts allocated on EC2"""
    def __init__(self):
        HostPool.__init__(self)
        # export AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        self.botoConnection = boto.connect_ec2()
        # assumes our lone AMI
        self.botoImage  = self.botoConnection.get_all_images(image_ids=['ami-2501e04c'])[0]
        # assumes keypair name
        self.botoReservation = self.botoImage.run(min_count=9, max_count=9, 
                                                  key_name="gsg-keypair", instance_type="m1.large")

        # wait for the running state
        print "Waiting for instances to start."
        for instance in self.botoReservation.instances:
            while True:
                instance.update()
                if instance.state == "running":
                    break
        print "Instances are all in running state."

        # push voltbin
        for instance in self.botoReservation.instances:
            print "copying voltbin to " + instance.public_dns_name
            os.system("scp -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ./* root@" + 
                      instance.public_dns_name + ":/opt/voltbin")
        
        # splice up the list
        self.hostList = self.botoReservation.instances[:6]
        self.clientList = self.botoReservation.instances[6:]


    def cleanup(self):
        # tear down all instances
        botoReservation.stop_all()
        return

    def getRemotePathString(self):
        return "REMOTEPATH=/opt/voltbin/ REMOTEUSER=root "

    def hostStringFromHostCount(self, count):
        retval = "HOSTCOUNT=%d " % count
        for i in range(count):
            retval += "HOST=%s " % self.hostList[i].public_dns_name
        return retval
        
    def clientStringFromClientCount(self, count):
        retval = "CLIENTCOUNT=%d " % count
        for i in range(count):
            retval += "CLIENTHOST=%s " % self.clientList[i].public_dns_name
        return retval



def range_inc(start, stop):
    return range(start, stop+1)

def getTPCCCommandLineOpts(hostcount, sitesperhost):
    warehouses = hostcount * sitesperhost
    return ["loadthreads=4 warehouses=%d INTERVAL=10000 DURATION=60000 " % warehouses];
    
def getOverheadCommandLineOpts():
    opts = ["measureOverheadMultipartition"]
    return ["transaction=%s INTERVAL=10000 DURATION=30000 " % s for s in opts]

def getBenchmarkBaseCommand(hostpool, hostcount, clientcount, processesperclient, sitesperhost, driver):
    cmd = "java -ea -Xmx1024m -cp voltdbfat.jar:mysql.jar:log4j.jar org.voltdb.benchmark.BenchmarkController "
    cmd += hostpool.getRemotePathString(); 
    cmd += hostpool.hostStringFromHostCount(hostcount)
    cmd += hostpool.clientStringFromClientCount(clientcount)
    cmd += "PROCESSESPERCLIENT=%d " % processesperclient
    cmd += "SITESPERHOST=%d " % sitesperhost
    cmd += "CLIENT=%s " % driver
    return cmd

def getCommandLineOptsForDriver(driver, hostcount, sitesperhost):
    if driver == "org.voltdb.benchmark.tpcc.TPCCClient":
        return getTPCCCommandLineOpts(hostcount, sitesperhost)
    elif driver == "org.voltdb.benchmark.overhead.OverheadClient":
        return getOverheadCommandLineOpts()


def signalHandler(signum, frame):
    raise Exception, "Interrupted by SIGINT."

def main():
    # abstract away the available hosts
    hostpool = LocalPool()
    # hostpool = EC2Pool()

    # try really hard to cleanup the hostpool.
    signal.signal(signal.SIGINT, signalHandler)

    try:
        # matrix of values to test with
        # hostcounts = [1,2,6]
        hostcounts = [1,2,5]
        # clientcounts = [1,3]
        clientcounts = [1,2]
        processesperclients = [1,2,6]
        sitesperhosts = [9,12]
        drivers = ["org.voltdb.benchmark.tpcc.TPCCClient", 
                   "org.voltdb.benchmark.overhead.OverheadClient"]
        
        cmds = []
        for hostcount in hostcounts:
            for clientcount in clientcounts:
                for processesperclient in processesperclients:
                    for sitesperhost in sitesperhosts:
                        for driver in drivers:
                            cmd = getBenchmarkBaseCommand(hostpool, hostcount, clientcount, 
                                                          processesperclient, sitesperhost, driver)
                            for postfix in getCommandLineOptsForDriver(driver, hostcount, sitesperhost):
                                cmds += [cmd + postfix]
        
        print "Running %d benchmarks...\n" % len(cmds)
        for i in range(len(cmds)):
            print "Running: \n%s" % cmds[i]
            os.system(cmds[i])
            print "Finished %d / %d" % (i, len(cmds))

    finally:
        # should catch ctrl-c, kill, 
        hostpool.cleanup()

if __name__ == "__main__":
    main()
