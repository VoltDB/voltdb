# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import DeploymentConfig
import HTTPListener
from flask import jsonify, abort, make_response
import json
import os
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
    deployment = HTTPListener.Global.DEPLOYMENT.get(database_id)
    if deployment is not None:
        if 'paths' in deployment and 'voltdbroot' in deployment['paths'] and 'snapshots' in deployment['paths']:
            voltdb_root = deployment['paths']['voltdbroot']['path']
            snapshot = deployment['paths']['snapshots']['path']
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
    return make_response(jsonify({'status': statuscode, 'statusString': statusstr}), statuscode)


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

    def start_database(self, is_pause, is_force):
        """
        Starts voltdb servers on all nodes configured for this database.
        Returns reponse with HTTP status code and error details json.
        """
        # sync deployment file first
        HTTPListener.sync_configuration()

        members = []
        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            return create_response('No database found for id: %u' % self.database_id, 404)
        else:
            members = current_database['members']
        if not members:
            return create_response('No servers configured for the database: %u' % self.database_id, 404)

        result = self.check_other_database_status(self.database_id)

        if not result:
            return make_response(jsonify({'status': 404, 'statusString': 'Error'}), 404)

        # Check if there are valid servers configured for all ids
        for server_id in members:
            server = HTTPListener.Global.SERVERS.get(server_id)
            if not server:
                return create_response('Server details not found for id: %u' % server_id, 404)

        # Now start each server
        server_unreachable = False
        error_msg = ''
        failed = False
        server_status = {}
        action = 'start'

        for server_id in members:
            server = HTTPListener.Global.SERVERS.get(server_id)
            curr = server
            try:
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%s?id=%u&pause=%s&force=%s') % \
                                  (curr['hostname'], HTTPListener.__PORT__, self.database_id, action, server_id, is_pause, is_force)
                response = requests.put(url)
                if response.status_code != requests.codes.ok:
                    failed = True

                db_status = json.loads(response.text)['statusString']
                server_status[curr['hostname']] = db_status
            except Exception, err:
                if 'ConnectionError' in str(err):
                    error_msg = "Could not connect to the server " + curr['hostname'] + ". " \
                                "Please ensure that all servers are reachable."
                    server_unreachable = True
                    break
                failed = True
                print traceback.format_exc()
                server_status[curr['hostname']] = str(err)
        if server_unreachable:
            return make_response(jsonify({'status': 500, 'statusString': error_msg}), 500)
        elif failed:
            url = ('http://%s:%u/api/1.0/databases/%u/status/') % \
                  (HTTPListener.__IP__, HTTPListener.__PORT__, self.database_id)
            response = requests.get(url)
            return make_response(jsonify({'status': 200, 'statusString': response.text}), 200)
        else:
            return make_response(
                jsonify({'status': 200, 'statusString': 'Start request sent successfully to servers: ' +
                                                        json.dumps(server_status)}), 200)

    def start_server(self, server_id, pause, is_forced, add_server=False):
        """
        Sends start request to the specified server
        """
        members = []
        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            return create_response('No database found for id: %u' % self.database_id, 404)
        else:
            members = current_database['members']
        if not members or server_id not in members:
            return create_response(
                'No server with id %u configured for the database: %u' % (server_id, self.database_id), 404)

        server = HTTPListener.Global.SERVERS.get(server_id)
        if not server:
            return create_response('Server details not found for id: %u' % server_id, 404)

        action = 'start'
        if add_server:
            action = 'add'
        try:
            url = ('http://%s:%u/api/1.0/databases/%u/servers/%s?id=%u&pause=%s&force=%s') % \
                              (server['hostname'], HTTPListener.__PORT__, self.database_id, action, server_id, pause, is_forced)
            response = requests.put(url)
            return create_response(json.loads(response.text)['statusString'], response.status_code)
        except Exception, err:
            if 'ConnectionError' in str(err):
               err = "Server " + server['hostname'] + " is currently unreachable."
            print traceback.format_exc()
            return create_response(str(err), 500)

    def check_and_start_local_server(self, sid, pause, database_id, is_forced, add_server=False):
        """
        Checks if voltdb server is running locally and
        starts it if the server is not running.
        If the server is running, this returns an error
        """
        if sid == -1:
            return create_response('A VoltDB Server not started wrong configuration', 500)

        if self.is_voltserver_running(database_id):
            return create_response('A VoltDB Server process is already running', 500)

        return_code = self.initialize_local_config(sid, is_forced)

        if return_code == 0:
            retcode = self.start_local_server(sid, pause, add_server)
            if (retcode == 0):
                HTTPListener.Global.SERVERS[sid]['isAdded'] = True
                HTTPListener.sync_configuration()
                return create_response('Success', 200)
            else:
                return create_response('Error', 500)
        else:
            # return create_response('Error starting server', 500)
            return create_response('Error', 500)

    def check_other_database_status(self, current_database_id):
        # Get members of current_database
        result = True
        database = HTTPListener.Global.DATABASES[current_database_id]

        SERVERS = []
        for id in database['members']:
            SERVERS.append(HTTPListener.Global.SERVERS[id])

        # Get database_id list other than current database
        for server in SERVERS:
            for key, value in HTTPListener.Global.DATABASES.iteritems():
                if key != current_database_id:
                    # Check if database is running
                    url = ('http://%s:%u/api/1.0/databases/%u/status/') % \
                          (HTTPListener.__IP__, HTTPListener.__PORT__, key)
                    response = requests.get(url)
                    if response.json()['status'] != 404:
                        if response.json()['dbStatus']['status'] == 'running':
                            for id in value['members']:
                                if HTTPListener.Global.SERVERS[id]['hostname'] == server['hostname']:
                                    # Check if same port is used
                                    if HTTPListener.Global.SERVERS[id]['client-listener'] == server['client-listener'] \
                                            or HTTPListener.Global.SERVERS[id]['admin-listener'] == server['admin-listener'] \
                                            or HTTPListener.Global.SERVERS[id]['zookeeper-listener'] == server[
                                                'zookeeper-listener'] \
                                            or HTTPListener.Global.SERVERS[id]['http-listener'] == server['http-listener'] \
                                            or HTTPListener.Global.SERVERS[id]['internal-listener'] == server[
                                                'internal-listener'] \
                                            or HTTPListener.Global.SERVERS[id]['replication-listener'] == server[
                                                'replication-listener']:
                                        return False
                        else:
                            result = True

        return result

    def is_voltserver_running(self, database_id):
        """
        Checks the set of running processes to find out if voltdb server is running
        """
        result = False
        process = subprocess.Popen("ps aux | grep 'java'", shell=True, stdout=subprocess.PIPE)
        process_list = process.communicate()[0].split('\n')
        for process_cmd in process_list:
            if '-DVDMStarted=true -DVDMDB=' + str(database_id) in process_cmd:
                result = True
                break
        return result

    def Get_Voltdb_Process(self, database_id):
        VoltdbProcess.isProcessRunning = False
        VoltdbProcess.processId = -1
        process = subprocess.Popen("ps aux | grep 'java'", shell=True, stdout=subprocess.PIPE)
        process_list = process.communicate()[0].split('\n')
        for process_cmd in process_list:

            if '-DVDMStarted=true -DVDMDB=' + str(database_id) in process_cmd:
                VoltdbProcess.isProcessRunning = True
                VoltdbProcess.processId = process_cmd.split()[1]
                break
        return VoltdbProcess

    def initialize_local_config(self, sid, is_forced):
        """
        Initialize a local server configuration.
        """
        server = HTTPListener.Global.SERVERS.get(sid)
        if not server:
            return 1

        # Gets deployment.xml for this database.
        deployment_content = DeploymentConfig.DeploymentConfiguration.get_database_deployment(self.database_id, sid)
        filename = os.path.realpath(os.path.join(HTTPListener.Global.CONFIG_PATH, 'deployment.xml'))
        config_path = os.path.realpath(self.get_volt_server_data_folder(sid))
        deployment_file = open(filename, 'w')
        deployment_file.write(deployment_content)
        deployment_file.close()
        volt_db_dir = get_voltdb_dir()
        verb = 'init'
        if is_forced == 'true':
            volt_db_cmd = ['nohup', os.path.join(volt_db_dir, 'voltdb'), verb, '--force', '-C', filename, '-D',
                           config_path]
        else:
            volt_db_cmd = ['nohup', os.path.join(volt_db_dir, 'voltdb'), verb, '-C', filename, '-D',
                           config_path]

        G.OUTFILE_COUNTER += 1
        out_filename = os.path.realpath(os.path.join(HTTPListener.Global.CONFIG_PATH,
                                                     'voltserver.output.%s.%u' % (G.OUTFILE_TIME, G.OUTFILE_COUNTER)))
        server_data_path = self.get_volt_server_data_folder(sid)
        HTTPListener.Global.VOLT_SERVER_PATH = server_data_path

        volt_init_status = self.run_voltserver_process(volt_db_cmd, out_filename, server_data_path, self.database_id)

        initialized = False
        read_file = open(out_filename)
        # Wait till server is ready or process exited due to error.
        while volt_init_status.poll() is None:
            time.sleep(.5)

        file_content = read_file.read()
        if 'Initialized VoltDB root directory' in file_content or \
                        'voltdbroot is already initialized' in file_content:
            initialized = True

        read_file.close()
        if volt_init_status.returncode is 0 or initialized:
            return 0
        else:
            return 1

    def start_local_server(self, sid, pause, add_server=False):
        """
        start a local server process. Add if add_server is true else just start the server.
        """
        # if server is not found bail out.
        config_path = os.path.realpath(self.get_volt_server_data_folder(sid))
        server = HTTPListener.Global.SERVERS.get(sid)
        if not server:
            return 1

        primary = self.get_first_hostname()
        voltdb_dir = get_voltdb_dir()
        verb = 'start'

        if server:
            url = ('http://%s:%u/api/1.0/databases/%u/status/') % \
                  (server['hostname'], HTTPListener.__PORT__, self.database_id)
            response = requests.get(url)
            is_running = response.json()['dbStatus']['status']

            server_ip = ''
            if is_running == 'running':
                for value in response.json()['dbStatus']['serverStatus']:
                    for key in value:
                        status = value[key]['status']
                        if status == 'running' and key != server['hostname']:
                            server_ip = key

        if add_server and server_ip != '':
            verb = 'add'
        host_count = self.get_host_count()
        host_list = self.get_host_list()
        if verb == 'start':
            if pause.lower() == 'true':
                voltdb_cmd = ['nohup', os.path.join(voltdb_dir, 'voltdb'), verb, '--pause', '-H', host_list, '-D', config_path]
            else:
                voltdb_cmd = ['nohup', os.path.join(voltdb_dir, 'voltdb'), verb, '-H', host_list, '-D', config_path]
        elif verb == 'add':
            voltdb_cmd = ['nohup', os.path.join(voltdb_dir, 'voltdb'), 'start', '--add', '--host=' + server_ip,
                          '-D', config_path]
        else:
            voltdb_cmd = ['nohup', os.path.join(voltdb_dir, 'voltdb'), verb, '-H', host_list, '-D', config_path]

        self.build_network_options(server, voltdb_cmd)

        G.OUTFILE_COUNTER = G.OUTFILE_COUNTER + 1
        outfilename = os.path.realpath(os.path.join(HTTPListener.Global.CONFIG_PATH,
                                                    ('voltserver.output.%s.%u') % (G.OUTFILE_TIME, G.OUTFILE_COUNTER)))

        server_data_path = self.get_volt_server_data_folder(sid)
        HTTPListener.Global.VOLT_SERVER_PATH = server_data_path
        voltserver = self.run_voltserver_process(voltdb_cmd, outfilename, server_data_path, self.database_id)

        initialized = False
        rfile = open(outfilename, 'r')
        # Wait till server is ready or process exited due to error.
        # Wait for a couple of seconds to see if the server errors out
        endtime = time.time() + 2
        while ((endtime - time.time() > 0) and
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
        if opt_val != None and len(opt_val) > 0:
            voltdb_cmd.append(cli_switch)
            voltdb_cmd.append(opt_val)

    def run_voltserver_process(self, voltdb_cmd, outfilename, server_data_folder, database_id):
        """
        Utility method to start voltdb process given the cmd details
        and output file for console output
        """
        outfile = open(outfilename, 'w')

        my_env = os.environ.copy()
        my_env['VOLTDB_OPTS'] = os.getenv('VOLTDB_OPTS', '') + ' -DVDMStarted=true' + ' -DVDMDB=' + str(database_id)
        return subprocess.Popen(voltdb_cmd, stdout=outfile, stderr=subprocess.STDOUT,
                                env=my_env, preexec_fn=ignore_signals, close_fds=True)

    def get_first_hostname(self):
        """
        Gets the first hostname configured in the deployment file for a given database
        """

        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            abort(404)

        server_id = current_database['members'][0]
        server = HTTPListener.Global.SERVERS.get(server_id)
        if not server:
            abort(404)

        return server['hostname']

    def kill_database(self):
        """
        Stops voltdb cluster for this database
        """
        members = []
        server_status = {}
        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            abort(404)
        else:
            members = current_database['members']
        if not members:
            return create_response('No servers configured for the database', 500)

        server_id = members[0]
        server = HTTPListener.Global.SERVERS.get(server_id)
        if not server:
            return create_response('Server details not found for id ' + server_id, 404)

        for server_id in members:
            server = HTTPListener.Global.SERVERS.get(server_id)
            curr = server
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
            user_options = ['-u', admin['name'], '-p', admin['password']]

        voltdb_dir = get_voltdb_dir()
        voltdb_cmd = [os.path.join(voltdb_dir, cmd), verb] + user_options + args

        admin_listener = server['admin-listener']

        if server['admin-listener'] != "":
            if ":" in admin_listener:
                self.add_voltdb_option(server, 'admin-listener', '--host', voltdb_cmd)
            else:
                opt_val = admin_listener
                if opt_val != None and len(opt_val) > 0:
                    voltdb_cmd.append('--host')
                    voltdb_cmd.append(server['hostname'] + ":" + admin_listener)

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
        security_config = HTTPListener.Global.DEPLOYMENT[self.database_id]['security']
        if not security_config:
            return False

        return security_config['enabled']

    def get_admin_user(self):
        """
        Returns an admin user configured for this user.
        Returns None if there are no admin users configured
        """
        users_outer = HTTPListener.Global.DEPLOYMENT[self.database_id]['users']
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

    def stop_db_server(self, server_id):
        """
        Stops voltdb server running locally for this database
        """
        members = []
        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            abort(404)
        else:
            members = current_database['members']
        if not members:
            return create_response('No servers configured for the database', 500)

        server = HTTPListener.Global.SERVERS.get(server_id)
        if not server:
            return create_response('Server details not found for id ' + server_id, 404)

        args = ['-H', server['hostname']]
        # This needs to look at authentication and port information.

        G.OUTFILE_COUNTER = G.OUTFILE_COUNTER + 1
        outfilename = os.path.join(HTTPListener.Global.CONFIG_PATH,
                                   ('voltserver.output.%s.%u') % (G.OUTFILE_TIME, G.OUTFILE_COUNTER))
        return self.run_voltdb_cmd('voltadmin', 'shutdown', args, outfilename, server)

    def stop_database(self, database_id):
        members = []
        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            abort(404)
        else:
            members = current_database['members']
        if not members:
            return create_response('No servers configured for the database', 500)

        server_id = members[0]
        server = HTTPListener.Global.SERVERS.get(server_id)
        if not server:
            return create_response('Server details not found for id ' + server_id, 404)

        # Now stop each server
        failed = False
        server_status = {}
        action = "stop"
        for server_id in members:
            server = HTTPListener.Global.SERVERS.get(server_id)
            curr = server
            try:
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%u/%s?force=true') % \
                      (curr['hostname'], HTTPListener.__PORT__, database_id, server_id, action)
                response = requests.put(url)
                if (response.status_code != requests.codes.ok):
                    failed = True
                server_status[curr['hostname']] = json.loads(response.text)['statusString']
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
            processId = self.Get_Voltdb_Process(self.database_id).processId
            if processId is not None and processId != -1:
                os.kill(int(processId), signal.SIGKILL)
                return create_response('success', 200)
            else:
                return create_response('process not found', 200)
        except Exception, err:
            return create_response(str(err), 500)

    def get_host_count(self):
        current_db = HTTPListener.Global.DATABASES.get(self.database_id)
        host_count = len(current_db['members'])
        return host_count

    def get_host_list(self):
        current_db = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_db:
            abort(404)

        host_count = 0
        host_list = ''
        for host in current_db['members']:
            current_server = HTTPListener.Global.SERVERS.get(int(host))
            if current_server['internal-listener'] != '':
                host_name = current_server['hostname'] + ':' + current_server['internal-listener']
            else:
                host_name = current_server['hostname']

            if host_count == 0:
                host_list = host_name
            else:
                host_list += ',' + host_name
            host_count += 1
        return host_list

    def stop_server(self, server_id, force):
        """
        Sends stop request to the specified server
        """
        current_database = HTTPListener.Global.DATABASES.get(self.database_id)
        if not current_database:
            return create_response('No database found for id: %u' % self.database_id, 404)
        else:
            members = current_database['members']

        if not members or server_id not in members:
            return create_response(
                'No server with id %u configured for the database: %u'
                % (server_id, self.database_id), 404)

        server = HTTPListener.Global.SERVERS.get(server_id)
        if not server:
            return create_response('Server details not found for id: %u'
                                   % server_id, 404)
        try:
            url = 'http://%s:%u/api/1.0/databases/%u/servers/stop?id=%u&force=%s' % \
                              (server['hostname'], HTTPListener.__PORT__, self.database_id, server_id, force)
            response = requests.put(url)
            return create_response(json.loads(response.text)['statusString'], response.status_code)
        except Exception, err:
            if 'ConnectionError' in str(err):
                err = "Server " + server['hostname'] + " is currently unreachable."
            print traceback.format_exc()
            return create_response(str(err), 500)
