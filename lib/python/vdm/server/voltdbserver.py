"""
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
"""

import DeploymentConfig
import HTTPListener

from flask import jsonify, abort, make_response
import json
import os
import psutil
import requests
import signal
import subprocess
import time
import traceback
import Log


class G:
    """
    Global variables used by voltdbserver
    """

    def __init__(self):
        pass

    OUTFILE_TIME = str(time.time())
    OUTFILE_COUNTER = 0


def ignore_signals():
    """
    Used when forking voltdbserver process to ignore certain signals
    """
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def get_voltdb_dir():
    """
    Utility method to get voltdb bin directory
    """
    return os.path.realpath(os.path.join(HTTPListener.Global.MODULE_PATH, '../../../..', 'bin'))


def check_snapshot_folder(database_id):
    deployment = [deployment for deployment in HTTPListener.Global.DEPLOYMENT if deployment['databaseid'] == database_id]
    if len(deployment) > 0:
        if 'paths' in deployment[0] and 'voltdbroot' in deployment[0]['paths'] and 'snapshots' in deployment[0]['paths']:
            voltdb_root = deployment[0]['paths']['voltdbroot']['path']
            snapshot = deployment[0]['paths']['snapshots']['path']

            outfilename = os.path.join(HTTPListener.Global.VOLT_SERVER_PATH, str(voltdb_root), str(snapshot))
            if os.path.isdir(outfilename):
                freshStart = False
            else:
                freshStart = True
            return freshStart
        else:
            return True
    else:
        return True


def create_response(statusstr, statuscode):
    """
    Utility method to create response JSON
    """
    return make_response(jsonify( { 'statusstring': statusstr } ), statuscode)


class VoltdbProcess:
    isProcessRunning = False
    processId = -1


class VoltDatabase:
    """Represents a Volt database"""

    def __init__(self, database_id):
        """
        :param database_id: the id of this database
        """
        self.database_id = database_id

    def start_database(self, recover=False):
        """
        Starts voltdb servers on all nodes configured for this database.
        Returns reponse with HTTP status code and error details json.
        """
        # sync deployment file first
        HTTPListener.sync_configuration() 
    
        members = []
        current_database = [database for database in HTTPListener.Global.DATABASES if database['id'] == self.database_id]
        if not current_database:
            return create_response('No database found for id: %u' % self.database_id, 404)
        else:
            members = current_database[0]['members']
        if not members:
            return create_response('No servers configured for the database: %u' % self.database_id, 404)

        # Check if there are valid servers configured for all ids
        for server_id in members:
            server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
            if not server:
                return create_response('Server details not found for id: %u' % server_id, 404)

        # Now start each server
        failed = False
        server_status = {}
        action = 'start'
        if recover:
            action = 'recover'
        for server_id in members:
            server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
            curr = server[0]
            try:
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%s?id=%u') % \
                                  (curr['hostname'], HTTPListener.__PORT__, self.database_id, action, server_id)
                response = requests.put(url)
                if response.status_code != requests.codes.ok:
                    failed = True
                server_status[curr['hostname']] = json.loads(response.text)['statusstring']
            except Exception, err:
                failed = True
                print traceback.format_exc()
                server_status[curr['hostname']] = str(err)

        if failed:
            url = ('http://%s:%u/api/1.0/databases/%u/status/') % \
                  (curr['hostname'], HTTPListener.__PORT__, self.database_id)
            response = requests.get(url)
            return create_response(response.text, 200)
        else:
            return create_response('Start request sent successfully to servers: ' +
                                   json.dumps(server_status), 200)

    def start_server(self, server_id, recover=False):
        """
        Sends start request to the specified server
        """
        members = []
        current_database = [database for database in HTTPListener.Global.DATABASES if database['id'] == self.database_id]
        if not current_database:
            return create_response('No database found for id: %u' % self.database_id, 404)
        else:
            members = current_database[0]['members']
        if not members or server_id not in members:
            return create_response('No server with id %u configured for the database: %u' % (server_id, self.database_id), 404)

        server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
        if not server:
            return create_response('Server details not found for id: %u' % server_id, 404)

        action = 'start'
        if recover:
            action = 'recover'
        try:
            url = ('http://%s:%u/api/1.0/databases/%u/servers/%s?id=%u') % \
                              (server[0]['hostname'], HTTPListener.__PORT__, self.database_id, action, server_id)
            response = requests.put(url)
            return create_response(json.loads(response.text)['statusstring'], response.status_code)
        except Exception, err:
            print traceback.format_exc()
            return create_response(str(err), 500)

    def check_and_start_local_server(self, sid, recover=False):
        """
        Checks if voltdb server is running locally and
        starts it if the server is not running. 
        If the server is running, this returns an error
        """
        if sid == -1:
            return create_response('A VoltDB Server not started wrong configuration', 500)

        if self.is_voltserver_running():
            return create_response('A VoltDB Server process is already running', 500)
    
        retcode = self.start_local_server(sid, recover)
        if (retcode == 0):
            return create_response('Success', 200)
        else:
            return create_response('Error', 500)
            # return create_response('Error starting server', 500)
    
    def is_voltserver_running(self):
        """
        Checks the set of running processes to find out if voltdb server is running
        """
        for proc in psutil.process_iter():
            try:
                cmd = proc.cmdline()
                if ('-DVDMStarted=true' in cmd) and ('java' in cmd[0]):
                    return True
            except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied) as e:
                #print traceback.format_exc()
                pass
    
        return False

    def Get_Voltdb_Process(self):
        VoltdbProcess.isProcessRunning = False
        VoltdbProcess.processId = -1
        for proc in psutil.process_iter():

            try:
                cmd = proc.cmdline()
                if ('-DVDMStarted=true' in cmd) and ('java' in cmd[0]):
                    VoltdbProcess.isProcessRunning = True
                    VoltdbProcess.processId = proc.pid
                    return VoltdbProcess
            except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied) as e:
                # print traceback.format_exc()
                pass

        return VoltdbProcess
    
    def start_local_server(self, sid, recover=False):
        """
        start a local server process. recover if recover is true else create.
        """
        # if server is not found bail out.
        server = [server for server in HTTPListener.Global.SERVERS if server['id'] == sid]
        if not server:
            return 1

        """
        Gets deployment.xml for this database and starts voltdb server locally
        """
        deploymentcontents = DeploymentConfig.DeploymentConfiguration.get_database_deployment(self.database_id)
        primary = self.get_first_hostname()
        filename = os.path.realpath(os.path.join(HTTPListener.Global.CONFIG_PATH, 'deployment.xml'))
        deploymentfile = open(filename, 'w')
        deploymentfile.write(deploymentcontents)
        deploymentfile.close()
        voltdb_dir = get_voltdb_dir()
        verb = 'create'
        if recover:
            verb = 'recover'
        voltdb_cmd = [ 'nohup', os.path.join(voltdb_dir, 'voltdb'), verb, '-d', filename, '-H', primary ]
        self.build_network_options(server[0], voltdb_cmd)

        G.OUTFILE_COUNTER = G.OUTFILE_COUNTER + 1
        outfilename = os.path.realpath(os.path.join(HTTPListener.Global.CONFIG_PATH,
                                                    ('voltserver.output.%s.%u') % (G.OUTFILE_TIME, G.OUTFILE_COUNTER)))

        server_data_path = self.get_volt_server_data_folder(sid)
        HTTPListener.Global.VOLT_SERVER_PATH = server_data_path
        voltserver = self.run_voltserver_process(voltdb_cmd, outfilename, server_data_path)
    
        initialized = False
        rfile = open(outfilename, 'r')
        # Wait till server is ready or process exited due to error.
        # Wait for a couple of seconds to see if the server errors out
        endtime = time.time() + 2
        while ((endtime-time.time()>0) and
               (voltserver.returncode is None) and (not initialized)):
            time.sleep(0.5)
            voltserver.poll()
            initialized = 'Server completed initialization' in rfile.readline()

        rfile.close()
        if (voltserver.returncode is None):
            return 0
        else:
            return 1

    def get_volt_server_data_folder(self, sid):
        folder_name = 'server' + '_' + str(sid)
        folder_path = os.path.join(HTTPListener.Global.DATA_PATH, folder_name)
        if not os.path.isdir(str(folder_path)):
            try:
                os.makedirs(folder_path)
            except Exception, err:
                print('Exception (%s): %s\n' % (err.__class__.__name__, str(err)))
        return folder_path

    # Build network options for command line.
    def build_network_options(self, sconfig, voltdb_cmd):
        self.add_voltdb_option(sconfig, 'internal-interface', '--internalinterface', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'external-interface', '--externalinterface', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'internal-listener', '--internal', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'placement-group', '--placement-group', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'zookeeper-listener', '--zookeeper', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'http-listener', '--http', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'client-listener', '--client', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'admin-listener', '--admin', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'replication-listener', '--replication', voltdb_cmd)
        self.add_voltdb_option(sconfig, 'public-interface', '--publicinterface', voltdb_cmd)

    # Given server config and option name add corrorponding cli switch for building command.
    def add_voltdb_option(self, sconfig, option_name, cli_switch, voltdb_cmd):
        opt_val = sconfig[option_name]
        if  opt_val != None and len(opt_val) > 0:
            voltdb_cmd.append(cli_switch)
            voltdb_cmd.append(opt_val)

    def run_voltserver_process(self, voltdb_cmd, outfilename, server_data_folder):
        """
        Utility method to start voltdb process given the cmd details
        and output file for console output
        """
        outfile = open(outfilename, 'w')
    
        # Start server in a separate process
        oldwd = os.getcwd()
        os.chdir(server_data_folder)
        try:
            my_env = os.environ.copy()
            my_env['VOLTDB_OPTS'] = os.getenv('VOLTDB_OPTS', '') +  ' -DVDMStarted=true'
            return subprocess.Popen(voltdb_cmd, stdout=outfile, stderr=subprocess.STDOUT,
                                          env=my_env, preexec_fn=ignore_signals, close_fds=True)
        finally:
            os.chdir(oldwd)
    
    def get_first_hostname(self):
        """
        Gets the first hostname configured in the deployment file for a given database
        """

        current_database = [database for database in HTTPListener.Global.DATABASES if database['id'] == self.database_id]
        if not current_database:
            abort(404)
    
        server_id = current_database[0]['members'][0]
        server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
        if not server:
            abort(404)

        return server[0]['hostname']

    def stop_database(self):
        """
        Stops voltdb cluster for this database
        """
        members = []
        server_status ={}
        current_database = [database for database in HTTPListener.Global.DATABASES if database['id'] == self.database_id]
        if not current_database:
            abort(404)
        else:
            members = current_database[0]['members']
        if not members:
            return create_response('No servers configured for the database', 500)
    
        server_id = members[0]
        server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
        if not server:
            return create_response('Server details not found for id ' + server_id, 404)

        for server_id in members:
            server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
            curr = server[0]
            try:
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%u/%s?force=false') % \
                                  (curr['hostname'], HTTPListener.__PORT__, self.database_id, server_id, 'stop')
                response = requests.put(url)
                server_status[curr['hostname']] = response.text

            except Exception, err:
                print traceback.format_exc()
                server_status[curr['hostname']] = response.text

        return create_response(json.dumps(server_status), 200)

    def run_voltdb_cmd(self, cmd, verb, args, outfilename, server):
        """
        Runs the given voltdb command using admin user, as needed
        """
        user_options = []
        if self.is_security_enabled():
            admin = self.get_admin_user()
            if admin is None:
                raise Exception('No admin users found')
            user_options = [ '-u', admin['name'], '-p', admin['password'] ]
    
        voltdb_dir = get_voltdb_dir()
        voltdb_cmd = [ os.path.join(voltdb_dir, cmd), verb ] + user_options + args

        admin_listener = server['admin-listener']

        if server['admin-listener'] != "":
            if ":" in admin_listener:
                self.add_voltdb_option(server, 'admin-listener', '--host', voltdb_cmd)
            else:
                opt_val = admin_listener
                if opt_val != None and len(opt_val) > 0:
                    voltdb_cmd.append('--host')
                    voltdb_cmd.append(server['hostname'] + ":"+ admin_listener)

        shutdown_proc = subprocess.Popen(voltdb_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (output, error) = shutdown_proc.communicate()
        exit_code = shutdown_proc.wait()
        target = open(outfilename, 'w')
        target.write(str(output + error))
        return output + error

    def is_security_enabled(self):
        """ 
        Looks at the deployment details and finds out if security is enabled for this database
        """
        security_config = HTTPListener.Global.DEPLOYMENT[self.database_id-1]['security']
        if not security_config:
            return False
    
        return security_config['enabled']
    
    def get_admin_user(self):
        """
        Returns an admin user configured for this user.
        Returns None if there are no admin users configured
        """
        users_outer = HTTPListener.Global.DEPLOYMENT[self.database_id-1]['users']
        if not users_outer:
            return None
    
        users = users_outer['user']
        if not users:
            return None
    
        admins = [auser for auser in users if auser['roles'] == 'Administrator']
        if not admins:
            return None
        else:
            return admins[0]

    def stop_server(self, server_id):
        """
        Stops voltdb server running locally for this database
        """
        members = []
        current_database = [database for database in HTTPListener.Global.DATABASES if database['id'] == self.database_id]
        if not current_database:
            abort(404)
        else:
            members = current_database[0]['members']
        if not members:
            return create_response('No servers configured for the database', 500)
    
        server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
        if not server:
            return create_response('Server details not found for id ' + server_id, 404)
    
        args = [ '-H', server[0]['hostname']]
        # This needs to look at authentication and port information.

        G.OUTFILE_COUNTER = G.OUTFILE_COUNTER + 1
        outfilename = os.path.join(HTTPListener.Global.CONFIG_PATH,
                                   ('voltserver.output.%s.%u') % (G.OUTFILE_TIME, G.OUTFILE_COUNTER))
        return self.run_voltdb_cmd('voltadmin', 'shutdown', args, outfilename, server[0])

    def kill_database(self, database_id):
        members = []
        current_database = [database for database in HTTPListener.Global.DATABASES if database['id'] == database_id]
        if not current_database:
            abort(404)
        else:
            members = current_database[0]['members']
        if not members:
            return create_response('No servers configured for the database', 500)

        server_id = members[0]
        server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
        if not server:
            return create_response('Server details not found for id ' + server_id, 404)

        # Now stop each server
        failed = False
        server_status = {}
        action = "stop"
        for server_id in members:
            server = [server for server in HTTPListener.Global.SERVERS if server['id'] == server_id]
            curr = server[0]
            try:
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%u/%s?force=true') % \
                      (curr['hostname'], HTTPListener.__PORT__, database_id, server_id, action)
                response = requests.put(url)
                if (response.status_code != requests.codes.ok):
                    failed = True
                server_status[curr['hostname']] = json.loads(response.text)['statusstring']
            except Exception, err:
                failed = True
                print traceback.format_exc()
                server_status[curr['hostname']] = str(err)

        if failed:
            return create_response('There were errors stopping servers: ' + json.dumps(server_status), 500)
        else:
            return create_response('Stop request sent successfully to servers: ' + json.dumps(server_status), 200)

    def kill_server(self, server_id):
        try:
            processId = self.Get_Voltdb_Process().processId
            if processId is not None and processId != -1:
                os.kill(processId, signal.SIGKILL)
                return create_response('success', 200)
            else:
                return create_response('process not found', 200)
        except Exception, err:
            return create_response(str(err), 500)

