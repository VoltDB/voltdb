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

def create_response(statusstr, statuscode):
    """
    Utility method to create response JSON
    """
    return make_response(jsonify( { 'statusstring': statusstr } ), statuscode)
    
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
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%s') % \
                                  (curr['hostname'], HTTPListener.__PORT__, self.database_id, action)
                response = requests.put(url)
                if (response.status_code != requests.codes.ok):
                    failed = True
                server_status[curr['hostname']] = json.loads(response.text)['statusstring']
            except Exception, err:
                failed = True
                print traceback.format_exc()
                server_status[curr['hostname']] = str(err)

        if failed:
            return create_response('There were errors starting servers: ' + str(server_status), 500)
        else:
            return create_response('Start request sent successfully to servers: ' + str(server_status), 200)

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
            url = ('http://%s:%u/api/1.0/databases/%u/servers/%s') % \
                              (server[0]['hostname'], HTTPListener.__PORT__, self.database_id, action)
            response = requests.put(url)
            return create_response(json.loads(response.text)['statusstring'], response.status_code)
        except Exception, err:
            print traceback.format_exc()
            return create_response(str(err), 500)

    def check_and_start_local_server(self, recover=False):
        """
        Checks if voltdb server is running locally and
        starts it if the server is not running. 
        If the server is running, this returns an error
        """
        if self.is_voltserver_running():
            return create_response('A VoltDB Server process is already running', 500)
    
        retcode = self.start_local_server(recover)
        if (retcode == 0):
            return create_response('Success', 200)
        else:
            return create_response('Error starting server', 500)
    
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
    
    def start_local_server(self, recover=False):
        """
        Gets deployment.xml for this database and starts voltdb server locally
        """
        deploymentcontents = DeploymentConfig.DeploymentConfiguration.get_database_deployment(self.database_id)
        primary = self.get_first_hostname()
        filename = os.path.join(HTTPListener.Global.PATH, 'deployment.xml')
        deploymentfile = open(filename, 'w')
        deploymentfile.write(deploymentcontents)
        deploymentfile.close()
        voltdb_dir = get_voltdb_dir()
        verb = 'create'
        if recover:
            verb = 'recover'
        voltdb_cmd = [ 'nohup', os.path.join(voltdb_dir, 'voltdb'), verb, '-d', filename, '-H', primary ]
    
        G.OUTFILE_COUNTER = G.OUTFILE_COUNTER + 1
        outfilename = os.path.join(HTTPListener.Global.PATH,
                ('voltserver.output.%s.%u') % (G.OUTFILE_TIME, G.OUTFILE_COUNTER))
        voltserver = self.run_voltserver_process(voltdb_cmd, outfilename)
    
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
    
    def run_voltserver_process(self, voltdb_cmd, outfilename):
        """
        Utility method to start voltdb process given the cmd details
        and output file for console output
        """
        outfile = open(outfilename, 'w')
    
        # Start server in a separate process
        oldwd = os.getcwd()
        os.chdir(HTTPListener.Global.PATH)
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
    
        args = [ '-H', server[0]['hostname'] ]
        return self.run_voltdb_cmd('voltadmin', 'shutdown', args)

    def run_voltdb_cmd(self, cmd, verb, args):
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
    
        shutdown_proc = subprocess.Popen(voltdb_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (output, error) = shutdown_proc.communicate()
        exit_code = shutdown_proc.wait()
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
    
        args = [ '-H', server[0]['hostname'], server[0]['name'] ]
        return self.run_voltdb_cmd('voltadmin', 'stop', args)
