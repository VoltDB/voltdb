#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.

# A command line tool for reserving equipment via jenkins REST API
# usage:  resv help

import jenkins
import re
import json
import sys
import getopt
import time
import getpass
import os
from six.moves.http_client import BadStatusLine
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError
from six.moves.urllib.parse import quote, urlencode, urljoin, urlparse
from six.moves.urllib.request import Request, urlopen
import httplib
from optparse import OptionParser

NODE_LIST_INFO = 'computer/api/json?depth=2'
NODE_STATE_BUSY = 'busy'

class VJenkins(jenkins.Jenkins) :
    def get_node_data(self,retry=10):
        '''Get a list of nodes connected to the Master

        Each node is a dict with keys 'name' and 'offline'

        :returns: List of nodes, ``[ { str: str, str: bool} ]``
        '''
        try:
            data = self.jenkins_open(Request(self._build_url(NODE_LIST_INFO)));
            #print("data:"+str(data))
            nodes_data = json.loads(data)
            return nodes_data['computer']
        except (HTTPError, BadStatusLine):
            raise BadHTTPException("Error communicating with server[%s]" % self.getServer())
        except (jenkins.JenkinsException):
            print("Authentication Exception? use the '-u <username>' and '-p <password>' options to reserve/release");
            raise jenkins.JenkinsException("Authentication Exception:")
        except (ValueError,httplib.IncompleteRead):
            # retry
            if ( retry > 0 ) :
                time.sleep(3)
                return self.get_node_data(retry = retry -1)
            else :
                raise jenkins.JenkinsException("Could not parse JSON info for server[%s]" % str(self.getServer()))



class Controller() :
    def __init__(self) :
        # the first node of the controllers
        self.master = None;
        # a list of nodes that are part of this controller
        self.slaves = [];
        self.reason = None;

class Resv() :
    # you may need to install the following pip modules:
    # multi_key_dict
    # pbr
    # python-jenkins
    # six
    # if the modules are installed locally, you can will need to add them to your PYTHONPATH as follows:
    # export PYTHONPATH=~/jenkins/python-jenkins-0.4.12/

    # if you have 'six' already install on your MAC, you may need to override it's installation as follows:
    # sudo pip install --upgrade six --ignore-install  # the --ignore-install because it's already installed
    # or you may need to completely reinstall it in a different directory:
    # pip install --install-option="--prefix=~/jenkins/six" --ignore-installed six
    # pbr may also need to be upgraded as follows:
    # sudo pip install --upgrade pbr


    def __init__(self) :
        # contains a list of controllers
        self.controllers = [];
        self.username = None ;
        self.passwd = None;
        self.jhost='http://ci.voltdb.lan/';
        self._server = None;
        self.node_controller_map = {};
        self.node_state_map = {};

    def getOpts(self) :
        parser = OptionParser()
        parser.add_option("-u","--username", dest="username",default=None);
        parser.add_option("-p","--passwd", dest="passwd",default=None);
        return parser.parse_args();


    def runCommand(self,command,host=None) :
        cmdHelp = """usage: resv [options] help|list|reserve|release|all
            options:
            -u <username> defaults to local user
            -p <passwd>
            commands:
            help - display help
            list - list just available machines
            all - list all machines
            reserve <host> - take an available host offline and reserve it.
            release <host> - put a host back into the pool
    """
        #print("command: "+command);
        if command == "help" :
            print(cmdHelp)
        elif command == "list" :
            self.printNodes("available");
        elif command == "all" :
            self.printNodes("all")
        elif command == "res" or command == "reserve" :
            self.reserve(host);
        elif command == "rel" or command == "release" :
            self.release(host);
        else :
            self.printNodes();
            print(cmdHelp);
            exit(0);


    def getServer(self) :
        if self._server == None :
            self.connect()
        return self._server;

    def connect(self):
        # check if we can log in with kerberos
        # sudo pip install kerberos
        #self._server = jenkins.Jenkins(self.jhost)

        if self._server == None :
            self._server = VJenkins(self.jhost, username=self.username, password=self.passwd)

        # test it
        # print self._server.jobs_count()

    def getNodes(self,filter=all) :
        self.printNodes(filter)

    def isBusy(self,name) :
        busy = "available";
        controller = None;
        short_name = ""
        short_matchobj = re.match(r"(volt\d+[a-z]{1,2})",name)
        isBusyController = False;
        if short_matchobj != None :
            short_name = short_matchobj.group(1);

        if name in self.node_controller_map :
            controller = self.node_controller_map[name]
            if self.node_state_map[controller] == NODE_STATE_BUSY :
                busy = NODE_STATE_BUSY
                isBusyController = True

        if short_name in self.node_controller_map :
            controller = self.node_controller_map[short_name]
            if self.node_state_map[controller] == NODE_STATE_BUSY :
                busy = NODE_STATE_BUSY
                isBusyController = True

        if name in self.node_state_map and self.node_state_map[name] == NODE_STATE_BUSY :
            busy = NODE_STATE_BUSY

        if short_name in self.node_state_map and self.node_state_map[short_name] == NODE_STATE_BUSY :
            busy = NODE_STATE_BUSY

        return [busy,isBusyController]

    def getData(self) :
        nodes = self.getServer().get_node_data();
        self.parseControllers(nodes);
        self.parseState(nodes);
        return nodes;

    def printNodes(self,filter=all) :
        nodes = self.getData();
        # work with nodes

        print("%50s %-10s %-20s" % ("Name","Available","Who/Description"))
        for node in nodes:
            #print("node: "+str(node));
            # the name returned is not the name to lookup when
            # dealing with master :/
            name = str(node['displayName'])
            executing = ""
            owner = ""
            if node['displayName'] == 'master':
                name = '(master)'
                continue

            try:
                busy,isBusyController = self.isBusy(name)

                if busy == NODE_STATE_BUSY:
                    if node['executors'][0]['currentExecutable'] != None :
                        executing = node['executors'][0]['currentExecutable']['fullDisplayName']

                        # figure out who's branch started the build, we are assuming it's using
                        # org.jenkinsci.plugins.multiplescms.MultiSCMChangeLogSet and hudson.plugins.git.GitChangeSet
                        # and the first change in this list is from the owner
                        if node['executors'][0]['currentExecutable']['changeSet'] != None and len(node['executors'][0]['currentExecutable']['changeSet']['items']) > 0:
                            owner = node['executors'][0]['currentExecutable']['changeSet']['items'][0]['author']['fullName']

                        # if we don't have the changeset owner, culprits will give us a list of people
                        # who had checkin's, but it's not necessarily the branch owner.
                        if owner == "" and len(node['executors'][0]['currentExecutable']['culprits']) > 0:
                            owner = node['executors'][0]['currentExecutable']['culprits'][0]['fullName']
                    else:
                        if str(executing) == "":
                            if not isBusyController:
                                executing = "idle="+str(node['executors'][0]['idle'])

                reason = node['offlineCauseReason']
                if str(reason) == "" and node['offlineCause'] != None :
                    reason = node['offlineCause']['description']
                if str(reason) == "" and isBusyController :
                    reason = "busy controller"

                print("%50s %-10s %-20s %-10s %-20s" % (name,busy,reason,executing,owner))
            except Exception as e:
                # Jenkins may 500 on depth >0. If the node info comes back
                # at depth 0 treat it as a node not running any jobs.
                if ('[500]' in str(e) and
                    self.getServer().get_node_info(node_name, depth=0)):
                    continue
                else:
                    raise

    def getController(self,nodeName) :
        for ctlr in self.controllers :
            if nodeName in ctlr.slaves :
                return ctlr

        return None;

    def parseControllers(self,node_data) :
        for node in node_data :
            #print node;
            name = node['displayName']
            #nodeList[node['name']] = {'state':node['offline'],'name':''}
            # treat controllers special
            # parse volt12o-controller-of-volt12g-h-o-p-volt13g-h
            controlMatch = re.match(r"(volt\d+[a-z]{1,2})-controller",name)
            if controlMatch != None :
                controller = str(name)
                controller_volt = controlMatch.group(1);
                base="volt"
                num=""
                letter=""
                for w in str.split(controller,"-") :
                    base_matchobj = re.match(r"volt(\d+)([a-z]{1,2})",w)
                    sub_matchobj = re.match(r"([a-z]{1,2})",w);
                    if base_matchobj != None :
                        num=base_matchobj.group(1);
                        letter=base_matchobj.group(2);
                    elif sub_matchobj != None :
                        letter=sub_matchobj.group(1);

                    self.node_controller_map["volt"+num+letter] = str(controller);

    def parseState(self,node_data) :
        for node in node_data :
            name = node['displayName'];
            if node['displayName'] == 'master':
                name = '(master)'

            offline = node['offline'];
            if offline :
                busy = NODE_STATE_BUSY
            else :
                busy = "available"

            offlineCause = node['offlineCause'];
            offlineCauseReason = node['offlineCauseReason'];
            reason = node['offlineCauseReason']

            self.node_state_map[name] = busy
            # also add it as the short name
            short_matchobj = re.match(r"(volt\d+[a-z]{1,2})",name)
            if short_matchobj != None :
                short_name = short_matchobj.group(1);
                self.node_state_map[short_name ] = busy

    def getBuildInfo(self) :
        # list the running builds
        builds = self.getServer().get_running_builds()
        for build in builds :
            # it has this structure
            # {'url': u'http://ci:8080/job/performance-nextrelease-tpcc/1259/', 'node': u'volt3j-controller-of-volt3a-b-c-d-e-f-g-h-i', 'executor': 0, 'name': u'performance-nextrelease-tpcc', 'number': 1259}
            #print build
            #print "node: %s url: %s" % (build['node'],build['url']);
            nodeList[build['node']] = build['name'];

    def reserve(self,nodeNames) :
        if nodeNames == None :
            print("you must enter a device(s) to reserve");
            return;

        nodes = self.getData();
        me = getpass.getuser()


        for nodeName in str.split(nodeNames,",") :
            busy,isBusyController = self.isBusy(nodeName)
            if  busy == NODE_STATE_BUSY:
                print("Can't reserve "+nodeName+" it's busy");
                continue;
            else :
                print("reserving "+nodeName);

            try :
                self.getServer().disable_node(nodeName,"reserved by " + me)
            except jenkins.NotFoundException:
                print("Can't find device '"+nodeName+"', use 'resv list' to view available devices");


    def release(self,nodeNames) :
        if nodeNames == None :
            print("you must enter a device to release");
            return;

        nodes = self.getData();
        for nodeName in str.split(nodeNames,",") :
            print("releasing "+nodeName);
            try :
                self.getServer().enable_node(nodeName)
            except (jenkins.JenkinsException):
                print("Authentication Exception? use the '-u <username>' and '-p <password>' options to reserve/release");
                raise jenkins.JenkinsException("Authentication Exception")
            except jenkins.NotFoundException:
                print("Can't find device '"+nodeName+"', use 'resv list' to view available devices");


if __name__ == '__main__' :
    resv = Resv()
    resv.username = getpass.getuser()
    host = None;
    command = "help"

    (options,args) = resv.getOpts()
    if options.username != None :
        resv.username = options.username;

    if options.passwd != None :
       resv.passwd = options.passwd

    if len(args) > 0 :
        command = args[0]

    if len(args) > 1 :
        host = args[1];

    resv.runCommand(command,host);

