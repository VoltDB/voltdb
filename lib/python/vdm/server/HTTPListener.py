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

import ast
from collections import defaultdict
from flask import Flask, render_template, jsonify, abort, make_response, request, Response
from flask_cors import CORS
from flask.views import MethodView
import json
import os
import os.path
import requests
import socket
import sys
import traceback
import urllib
from xml.etree.ElementTree import Element, SubElement, tostring, XML
from Validation import ServerInputs, DatabaseInputs, JsonInputs, UserInputs, ConfigValidation, ValidateDbFieldType, \
    ValidateServerFieldType
import DeploymentConfig
import voltdbserver
import glob
import Log

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../../voltcli'))
from voltcli import utility
import voltdbclient
import logging
from logging.handlers import RotatingFileHandler
from flask_logging import Filter
import Configuration
import signal
import thread


filter_log = Filter('/api/1.0/', 'GET')

APP = Flask(__name__, template_folder="../templates", static_folder="../static")
CORS(APP)

__PATH__ = ""

__IP__ = "localhost"

__PORT__ = 8000

ALLOWED_EXTENSIONS = ['xml']


def receive_signal(signum, stack):
    config_path = os.path.join(Global.CONFIG_PATH, 'voltdeploy.xml')
    Configuration.validate_and_convert_xml_to_json(config_path)
    thread.start_new(sync_configuration, ())
    # print 'Received:', signum


signal.signal(signal.SIGHUP, receive_signal)


@APP.errorhandler(400)
def not_found(error):
    """
    Gives error message when any bad requests are made.
    Args:
        error (string): The first parameter.
    Returns:
        Error message.
    """
    return make_response(jsonify({'status': 400, 'statusString': 'Bad request'}), 400)


@APP.errorhandler(404)
def not_found(error):
    """
    Gives error message when any invalid url are requested.
    Args:
        error (string): The first parameter.
    Returns:
        Error message.
    """
    return make_response(jsonify({'status': 404, 'statusString': 'Not found'}), 404)


@APP.route("/")
def index():
    """
    Gets to the index page of VDM when http://localhost:8000/ is hit.
    """
    return render_template("index.html")


def make_public_server(servers):
    """
    Get the server information in required format.
    Args:
        servers (server object): The first parameter.
    Returns:
        Server object in required format.
    """
    new_server = {}
    for field in servers:
        new_server[field] = servers[field]
    return new_server


def make_public_database(databases):
    """
    Get the database information in required format.
    Args:
        databases (database object): The first parameter.
    Returns:
        Database object in required format.
    """
    new_database = {}
    for field in databases:
        new_database[field] = databases[field]
    return new_database


def make_public_deployment(deployments):
    """
    Get the deployment information  in required format.
    Args:
        deployments (deployment object): The first parameter.
    Returns:
        Deployment object in required format.
    """
    new_deployment = {}
    for field in deployments:
        new_deployment[field] = deployments[field]
    return new_deployment


def map_deployment(request, database_id):
    """
    Map the deployment information from request to deployment object in required format.
    Args:
        request (request object): The first parameter.
        database_id (int): The second parameter
    Returns:
        Deployment object in required format.
    """

    deployment = Global.DEPLOYMENT.get(database_id)

    if 'cluster' in request.json and 'elastic' in request.json['cluster']:
        deployment['cluster']['elastic'] = request.json['cluster']['elastic']

    if 'cluster' in request.json and 'schema' in request.json['cluster']:
        deployment['cluster']['schema'] = request.json['cluster']['schema']

    if 'cluster' in request.json and 'sitesperhost' in request.json['cluster']:
        deployment['cluster']['sitesperhost'] = request.json['cluster']['sitesperhost']

    if 'cluster' in request.json and 'kfactor' in request.json['cluster']:
        deployment['cluster']['kfactor'] = request.json['cluster']['kfactor']

    if 'admin-mode' in request.json and 'adminstartup' in request.json['admin-mode']:
        deployment['admin-mode']['adminstartup'] = request.json['admin-mode']['adminstartup']

    if 'admin-mode' in request.json and 'port' in request.json['admin-mode']:
        deployment['admin-mode']['port'] = request.json['admin-mode']['port']

    if 'commandlog' in request.json and 'adminstartup' in request.json['commandlog']:
        deployment['commandlog']['adminstartup'] = request.json['commandlog']['adminstartup']

    if 'commandlog' in request.json and 'frequency' in \
            request.json['commandlog'] and 'time' in request.json['commandlog']['frequency']:
        deployment['commandlog']['frequency']['time'] = request.json['commandlog']['frequency']['time']

    if 'commandlog' in request.json and 'frequency' in \
            request.json['commandlog'] and 'transactions' in request.json['commandlog']['frequency']:
        deployment['commandlog']['frequency']['transactions'] = request.json['commandlog']['frequency'][
            'transactions']

    if 'commandlog' in request.json and 'enabled' in request.json['commandlog']:
        deployment['commandlog']['enabled'] = request.json['commandlog']['enabled']

    if 'commandlog' in request.json and 'logsize' in request.json['commandlog']:
        deployment['commandlog']['logsize'] = request.json['commandlog']['logsize']

    if 'commandlog' in request.json and 'synchronous' in request.json['commandlog']:
        deployment['commandlog']['synchronous'] = request.json['commandlog']['synchronous']

    if 'heartbeat' in request.json and 'timeout' in request.json['heartbeat']:
        deployment['heartbeat']['timeout'] = request.json['heartbeat']['timeout']

    if 'httpd' in request.json and 'enabled' in request.json['httpd']:
        deployment['httpd']['enabled'] = request.json['httpd']['enabled']

    if 'httpd' in request.json and 'jsonapi' in request.json['httpd'] and 'enabled' in request.json['httpd'][
        'jsonapi']:
        deployment['httpd']['jsonapi']['enabled'] = request.json['httpd']['jsonapi']['enabled']

    if 'httpd' in request.json and 'port' in request.json['httpd']:
        deployment['httpd']['port'] = request.json['httpd']['port']

    if 'partition-detection' in request.json and 'enabled' in request.json['partition-detection']:
        deployment['partition-detection']['enabled'] = request.json['partition-detection']['enabled']

    if 'partition-detection' in request.json and 'snapshot' in request.json['partition-detection'] \
            and 'prefix' in request.json['partition-detection']['snapshot']:
        deployment['partition-detection']['snapshot']['prefix'] = \
            request.json['partition-detection']['snapshot']['prefix']

    if 'paths' in request.json and 'commandlog' in request.json['paths'] and \
                    'path' in request.json['paths']['commandlog']:
        deployment['paths']['commandlog']['path'] = \
            request.json['paths']['commandlog']['path']

    if 'paths' in request.json and 'commandlogsnapshot' in request.json['paths'] and \
                    'path' in request.json['paths']['commandlogsnapshot']:
        deployment['paths']['commandlogsnapshot']['path'] = \
            request.json['paths']['commandlogsnapshot']['path']

    if 'paths' in request.json and 'droverflow' in request.json['paths'] and \
                    'path' in request.json['paths']['droverflow']:
        deployment['paths']['droverflow']['path'] = \
            request.json['paths']['droverflow']['path']

    if 'paths' in request.json and 'exportoverflow' in request.json['paths'] and \
                    'path' in request.json['paths']['exportoverflow']:
        deployment['paths']['exportoverflow']['path'] = \
            request.json['paths']['exportoverflow']['path']

    if 'paths' in request.json and 'snapshots' in request.json['paths'] and \
                    'path' in request.json['paths']['snapshots']:
        deployment['paths']['snapshots']['path'] = \
            request.json['paths']['snapshots']['path']

    if 'paths' in request.json and 'voltdbroot' in request.json['paths'] and \
                    'path' in request.json['paths']['voltdbroot']:
        deployment['paths']['voltdbroot']['path'] = \
            request.json['paths']['voltdbroot']['path']

    if 'security' in request.json and 'enabled' in request.json['security']:
        deployment['security']['enabled'] = request.json['security']['enabled']

    if 'security' in request.json and 'frequency' in request.json['security']:
        deployment['security']['frequency'] = request.json['security']['frequency']

    if 'security' in request.json and 'provider' in request.json['security']:
        deployment['security']['provider'] = request.json['security']['provider']

    if 'snapshot' in request.json and 'enabled' in request.json['snapshot']:
        deployment['snapshot']['enabled'] = request.json['snapshot']['enabled']

    if 'snapshot' in request.json and 'frequency' in request.json['snapshot']:
        deployment['snapshot']['frequency'] = request.json['snapshot']['frequency']

    if 'snapshot' in request.json and 'prefix' in request.json['snapshot']:
        deployment['snapshot']['prefix'] = request.json['snapshot']['prefix']

    if 'snapshot' in request.json and 'retain' in request.json['snapshot']:
        deployment['snapshot']['retain'] = request.json['snapshot']['retain']

    if 'systemsettings' in request.json and 'elastic' in request.json['systemsettings'] \
            and 'duration' in request.json['systemsettings']['elastic']:
        deployment['systemsettings']['elastic']['duration'] = request.json['systemsettings']['elastic'][
            'duration']

    if 'systemsettings' in request.json and 'elastic' in request.json['systemsettings'] \
            and 'throughput' in request.json['systemsettings']['elastic']:
        deployment['systemsettings']['elastic']['throughput'] = request.json['systemsettings']['elastic'][
            'throughput']

    if 'systemsettings' in request.json and 'query' in request.json['systemsettings'] \
            and 'timeout' in request.json['systemsettings']['query']:
        deployment['systemsettings']['query']['timeout'] = request.json['systemsettings']['query']['timeout']

    if 'systemsettings' in request.json and 'temptables' in request.json['systemsettings'] \
            and 'maxsize' in request.json['systemsettings']['temptables']:
        deployment['systemsettings']['temptables']['maxsize'] = request.json['systemsettings']['temptables'][
            'maxsize']

    if 'systemsettings' in request.json and 'snapshot' in request.json['systemsettings'] \
            and 'priority' in request.json['systemsettings']['snapshot']:
        deployment['systemsettings']['snapshot']['priority'] = request.json['systemsettings']['snapshot']['priority']

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings']:
        if 'resourcemonitor' not in deployment['systemsettings'] or deployment['systemsettings'][
            'resourcemonitor'] is None:
            deployment['systemsettings']['resourcemonitor'] = {}

        if 'memorylimit' in request.json['systemsettings']['resourcemonitor'] and \
                request.json['systemsettings']['resourcemonitor']['memorylimit']:
            deployment['systemsettings']['resourcemonitor']['memorylimit'] = {}
            if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings'] \
                    and 'memorylimit' in request.json['systemsettings']['resourcemonitor'] \
                    and 'size' in request.json['systemsettings']['resourcemonitor']['memorylimit']:
                if request.json['systemsettings']['resourcemonitor']['memorylimit']['size'] != '':
                    deployment['systemsettings']['resourcemonitor']['memorylimit']['size'] = \
                        request.json['systemsettings']['resourcemonitor']['memorylimit']['size']
                else:
                    deployment['systemsettings']['resourcemonitor']['memorylimit'] = {}

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings']:
        if 'resourcemonitor' not in deployment['systemsettings'] or deployment['systemsettings'][
            'resourcemonitor'] is None:
            deployment['systemsettings']['resourcemonitor'] = {}

        if 'disklimit' in request.json['systemsettings']['resourcemonitor']:
            if 'feature' in request.json['systemsettings']['resourcemonitor']['disklimit']:
                deployment['systemsettings']['resourcemonitor']['disklimit'] = {}
                deployment['systemsettings']['resourcemonitor']['disklimit']['feature'] = []
                if request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                    for feature in request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                        deployment['systemsettings']['resourcemonitor']['disklimit']['feature'].append(
                            {
                                'name': feature['name'],
                                'size': feature['size']
                            }
                        )
                else:
                    deployment['systemsettings']['resourcemonitor']['disklimit'] = {}

    if 'systemsettings' in deployment and 'resourcemonitor' in deployment['systemsettings']:
        result = False
        if 'memorylimit' in deployment['systemsettings']['resourcemonitor'] and \
                deployment['systemsettings']['resourcemonitor']['memorylimit']:
            result = True
        if 'disklimit' in deployment['systemsettings']['resourcemonitor'] and \
                deployment['systemsettings']['resourcemonitor']['disklimit']:
            result = True
        if result == False:
            deployment['systemsettings']['resourcemonitor'] = {}

    if 'import' in request.json:
        if 'import' not in deployment or deployment['import'] is None or deployment['import'] == "None":
            deployment['import'] = {}

    if 'import' in request.json and 'configuration' in request.json['import']:
        deployment['import']['configuration'] = []
        i = 0
        for configuration in request.json['import']['configuration']:
            if 'module' not in configuration:
                module = ''
            else:
                module = configuration['module']
            deployment['import']['configuration'].append(
                {
                    'enabled': configuration['enabled'],
                    'module': module,
                    'type': configuration['type'],
                    'format': configuration['format'],
                    'property': []
                }
            )

            if 'property' in configuration:
                for property in configuration['property']:
                    deployment['import']['configuration'][i]['property'].append(
                        {
                            'name': property['name'],
                            'value': property['value']
                        }
                    )
                i += 1
    if 'export' in request.json:
        if 'export' not in deployment or deployment['export'] is None or deployment['export'] == "None":
            deployment['export'] = {}

    if 'export' in request.json and 'configuration' in request.json['export']:
        try:
            deployment['export']['configuration'] = []
        except Exception, err:
            print err
        i = 0

        for configuration in request.json['export']['configuration']:
            if 'exportconnectorclass' not in configuration:
                export_connector_class = ''
            else:
                export_connector_class = configuration['exportconnectorclass']
            deployment['export']['configuration'].append(
                {
                    'enabled': configuration['enabled'],
                    'stream': configuration['stream'],
                    'type': configuration['type'],
                    'exportconnectorclass': export_connector_class,
                    'property': []
                }
            )

            if 'property' in configuration:
                for property in configuration['property']:
                    deployment['export']['configuration'][i]['property'].append(
                        {
                            'name': property['name'],
                            'value': property['value']
                        }
                    )
                i += 1

    if 'users' in request.json and 'user' in request.json['users']:
        deployment['users'] = {}
        deployment['users']['user'] = []
        for user in request.json['users']['user']:
            deployment['users']['user'].append(
                {
                    'name': user['name'],
                    'roles': user['roles'],
                    'password': user['password'],
                    'plaintext': user['plaintext']
                }
            )

    if 'dr' in request.json:
        if not request.json['dr']:
            deployment['dr'] = {}
        else:
            if 'dr' not in deployment or deployment['dr'] is None:
                deployment['dr'] = {}

            if 'connection' in request.json['dr'] and request.json['dr']['connection']:
                if not hasattr(deployment['dr'], 'connection'):
                    deployment['dr']['connection'] = {}

                if 'source' not in request.json['dr']['connection'] or \
                   ('source' in request.json['dr']['connection'] and
                        request.json['dr']['connection']['source'].strip() == ''):
                    deployment['dr']['connection'] = None
                else:
                    deployment['dr']['connection']['source'] = request.json['dr']['connection']['source']
            else:
                deployment['dr']['connection'] = None
            if 'id' in request.json['dr']:
                deployment['dr']['id'] = request.json['dr']['id']

            if 'listen' in request.json['dr']:
                deployment['dr']['listen'] = request.json['dr']['listen']
            else:
                deployment['dr']['listen'] = True

            if request.json['dr']:
                if 'port' in request.json['dr']:
                    deployment['dr']['port'] = request.json['dr']['port']
                else:
                    deployment['dr']['port'] = None

    return deployment


def map_deployment_users(request, user_id):
    if 'name' not in Global.DEPLOYMENT_USERS:
        Global.DEPLOYMENT_USERS[user_id] = {
            'userid': user_id,
            'databaseid': request.json['databaseid'],
            'name': request.json['name'],
            'password': urllib.unquote(str(request.json['password']).encode('ascii')).decode('utf-8'),
            'roles': request.json['roles'],
            'plaintext': request.json['plaintext']
        }

        deployment_user = Global.DEPLOYMENT_USERS.get(user_id)

    else:
        deployment_user = Global.DEPLOYMENT_USERS.get(user_id)

        if deployment_user is not None:
            deployment_user['name'] = request.json['name']
            deployment_user['password'] = request.json['password']
            deployment_user['plaintext'] = request.json['plaintext']
            deployment_user['roles'] = request.json['roles']

    return deployment_user


def get_volt_jar_dir():
    return os.path.realpath(os.path.join(Global.MODULE_PATH, '../../../..', 'voltdb'))


def get_port(ip_with_port):
    if ip_with_port != "":
        if ":" not in ip_with_port:
            return ip_with_port
        else:
            arr = ip_with_port.split(":")
            return arr[1]


def check_port_valid(port_option, server):
    if port_option == "http-listener":
        default_port = "8080"
    if port_option == "admin-listener":
        default_port = "21211"
    if port_option == "zookeeper-listener":
        default_port = "7181"
    if port_option == "replication-listener":
        default_port = "5555"
    if port_option == "client-listener":
        default_port = "21212"
    if port_option == "internal-listener":
        default_port = "3021"

    server_port = get_port(server[port_option])
    if server_port is None or server_port == "":
        server_port = default_port

    if port_option not in request.json:
        port = default_port
    else:
        port = get_port(request.json[port_option])

    if port == server_port:
        return jsonify(status= 401,
                       statusString="Port %s for the same host is already used by server %s for %s." % \
                              (port, server['hostname'], port_option))


def validate_server_ports(database_id, server_id=-1):
    arr = ["http-listener", "admin-listener", "internal-listener", "replication-listener", "zookeeper-listener",
           "client-listener"]

    specified_port_values = {
        "http-listener": get_port(request.json.get('http-listener', "").strip().lstrip("0")),
        "admin-listener": get_port(request.json.get('admin-listener', "").strip().lstrip("0")),
        "replication-listener": get_port(request.json.get('replication-listener', "").strip().lstrip("0")),
        "client-listener": get_port(request.json.get('client-listener', "").strip().lstrip("0")),
        "zookeeper-listener": get_port(request.json.get('zookeeper-listener', "").strip().lstrip("0")),
        "internal-listener": get_port(request.json.get('internal-listener', "").strip().lstrip("0"))
    }

    for option in arr:
        value = specified_port_values[option]
        for port_key in specified_port_values.keys():
            if option != port_key and value is not None and specified_port_values[port_key] == value:
                return jsonify(status=401, statusString="Duplicate port")
    database_servers = get_servers_from_database_id(database_id)
    if server_id == -1:
        servers = [servers for servers in database_servers if servers['hostname'] == request.json['hostname']]
    else:
        servers = [servers for servers in database_servers if servers['hostname'] ==
                   request.json['hostname'] and servers['id'] != server_id]
    for server in servers:
        for option in arr:
            result = check_port_valid(option, server)
            if result is not None:
                return result


def sync_configuration():
    headers = {'content-type': 'application/json'}
    url = 'http://%s:%u/api/1.0/voltdeploy/configuration/' % \
          (__IP__, __PORT__)
    response = requests.post(url, headers=headers)
    return response


def replace_last(source_string, replace_what, replace_with):
    head, sep, tail = source_string.rpartition(replace_what)
    return head + replace_with + tail


def check_size_value(value, key):
    per_idx = value.find('%')
    if per_idx != -1:
        try:
            str_value = replace_last(value, '%', '')
            int_value = int(str_value)
            min_value = 0
            error_msg = key + ' percent value must be between 0 and 99.'
            if key == 'memorylimit':
                min_value = 1
                error_msg = key + ' percent value must be between 1 and 99.'
            if int_value < min_value or int_value > 99:
                return jsonify({'error': error_msg})
            return jsonify({'status': 'success'})
        except Exception, exp:
            return jsonify({'error': str(exp)})
    else:
        try:
            int_value = int(value)
            min_value = 0
            error_msg = key + ' value must be between 0 and 2147483647.'
            if key == 'memorylimit':
                min_value = 1
                error_msg = key + ' value must be between 1 and 2147483647.'
            if int_value < min_value or int_value > 2147483647:
                return jsonify({'error': error_msg})
            return jsonify({'status': 'success'})
        except Exception, exp:
            return jsonify({'error': str(exp)})


def is_pro_version(deployment):
    ##############################################
    file_path = ''
    try:
        volt_jar = glob.glob(os.path.join(get_volt_jar_dir(), 'voltdb-*.jar'))
        if len(volt_jar) > 0:
            file_path = volt_jar[0]
        else:
            print 'No voltdb jar file found.'
    except Exception, err:
        print err
    if file_path != '':
        is_pro = utility.is_pro_version(file_path)
        if is_pro:
            if 'commandlog' in deployment and 'enabled' in deployment['commandlog'] and not \
                    deployment['commandlog']['enabled']:
                deployment['commandlog']['enabled'] = True
                ###############################################


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


def get_servers_from_database_id(database_id):
    servers = []
    database = Global.DATABASES.get(int(database_id))
    if database is None:
        return make_response(jsonify({'statusstring': 'No database found for id: %u' % int(database_id)}), 404)
    else:
        members = database['members']

    for server_id in members:
        server = Global.SERVERS.get(server_id)
        servers.append(server)
    return servers


def check_invalid_roles(roles):
    roles = str(request.json['roles']).split(',')
    for role in roles:
        if role.strip() == '':
            return False
    return True


class DictClass(dict):
    pass


IS_CURRENT_NODE_ADDED = False
IS_CURRENT_DATABASE_ADDED = False
IGNORETOP = {"databaseid": True, "users": True}


class Global:
    """
    Class to defined global variables for HTTPListener.
    """

    def __init__(self):
        pass

    SERVERS = {}
    DATABASES = {}
    DEPLOYMENT = {}
    DEPLOYMENT_USERS = {}
    CONFIG_PATH = ''
    DATA_PATH = ''
    MODULE_PATH = ''
    DELETED_HOSTNAME = ''
    VOLT_SERVER_PATH = ''
    DEFAULT_PATH = []


class ServerAPI(MethodView):
    """Class to handle requests related to server"""

    @staticmethod
    def get(database_id, server_id=None):
        """
        Get the members of the database with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            List of member ids related to specified database.
        """

        if server_id is None:
            servers = []
            database = Global.DATABASES.get(database_id)
            if database is None:
                return make_response(jsonify({'statusstring': 'No database found for id: %u' % database_id}), 404)
            else:
                members = database['members']

            for server_id in members:
                server = Global.SERVERS.get(server_id)
                if not server:
                    return make_response(jsonify({'statusstring': 'Server details not found for id: %u' % server_id}),
                                         404)
                servers.append(server)

            return jsonify({'status': 200, 'statusString': 'OK', 'members': servers})
        else:
            database = Global.DATABASES.get(database_id)
            if database is None:
                return make_response(jsonify({'statusstring': 'No database found for id: %u' % database_id}), 404)
            else:
                members = database['members']
            if server_id in members:
                server = Global.SERVERS.get(server_id)
                if not server:
                    abort(404)
                return jsonify({'status': 200, 'statusString': 'OK', 'server': make_public_server(server)})
            else:
                return jsonify({'statusstring': 'Given server with id %u doesn\'t belong to database with id %u.' % (
                    server_id, database_id)})

    @staticmethod
    def post(database_id):
        """
        Saves the server and associate it to database with given database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            Information and the status of server if it is saved otherwise the error message.
        """
        if 'id' in request.json:
            return make_response(jsonify({'status': 404, 'statusString': 'You cannot specify \'Id\' while creating server.'}), 404)

        server_type_error = ValidateServerFieldType(request.json)
        if 'status' in server_type_error and server_type_error['status'] == 'error':
            return jsonify(status=401, statusString=server_type_error['errors'])

        inputs = ServerInputs(request)
        if not inputs.validate():
            return jsonify(status=401,statusString=inputs.errors)

        result = validate_server_ports(database_id)
        if result is not None:
            return result

        if not Global.SERVERS:
            server_id = 1
        else:
            server_id = Global.SERVERS.keys()[-1] + 1

        Global.SERVERS[server_id] = {
            'id': server_id,
            'name': request.json.get('name', "").strip(),
            'description': request.json.get('description', "").strip(),
            'hostname': request.json.get('hostname', "").strip(),
            'enabled': True,
            'admin-listener': request.json.get('admin-listener', "").strip().lstrip("0"),
            'zookeeper-listener': request.json.get('zookeeper-listener', "").strip().lstrip("0"),
            'replication-listener': request.json.get('replication-listener', "").strip().lstrip("0"),
            'client-listener': request.json.get('client-listener', "").strip().lstrip("0"),
            'internal-interface': request.json.get('internal-interface', "").strip(),
            'external-interface': request.json.get('external-interface', "").strip(),
            'public-interface': request.json.get('public-interface', "").strip(),
            'internal-listener': request.json.get('internal-listener', "").strip().lstrip("0"),
            'http-listener': request.json.get('http-listener', "").strip().lstrip("0"),
            'placement-group': request.json.get('placement-group', "").strip(),
            'isAdded': False,
            'voltdbroot': request.json.get('voltdbroot', "").strip(),
            'snapshots': request.json.get('snapshots', "").strip(),
            'exportoverflow': request.json.get('exportoverflow', "").strip(),
            'commandlog': request.json.get('commandlog', "").strip(),
            'commandlogsnapshot': request.json.get('commandlogsnapshot', "").strip(),
            'droverflow': request.json.get('droverflow', "").strip()
        }

        # Add server to the current database
        current_database = Global.DATABASES.get(database_id)
        if current_database is None:
            abort(404)
        if not request.json:
            abort(400)
        current_database['members'].append(server_id)

        sync_configuration()
        Configuration.write_configuration_file()
        url = 'http://%s:%u/api/1.0/databases/%u/servers/%u/' % \
              (__IP__, __PORT__, database_id, server_id)

        resp = make_response(jsonify({'status': 201, 'statusString': 'OK', 'server': Global.SERVERS[server_id],
                                      'members': current_database['members']}), 201)
        resp.headers['Location'] = url

        return resp

    @staticmethod
    def delete(database_id, server_id):
        """
        Delete the server with specified server_id.
        Args:
            server_id (int): The first parameter.
        Returns:
            True if the server is deleted otherwise the error message.
        """
        database = Global.DATABASES.get(database_id)
        if database is None:
            return make_response(jsonify({'statusstring': 'No database found for id: %u' % database_id}), 404)
        else:
            members = database['members']
        if server_id in members:
            # delete a single server
            server = Global.SERVERS.get(server_id)
            if server is None:
                return make_response(
                    jsonify({'statusstring': 'No server found for id: %u in database %u' % (server_id, database_id)}),
                    404)
            # remove the server from given database member list
            url = 'http://%s:%u/api/1.0/databases/%u/servers/%u/status' % \
                  (server['hostname'], __PORT__, database_id, server_id)
            response = requests.get(url)

            if response.json()['serverStatus']['status'] == "running":
                return make_response(jsonify({'statusstring': 'Cannot delete a running server'}), 403)
            else:
                # remove the server from given database member list
                current_database = Global.DATABASES.get(database_id)
                current_database['members'].remove(server_id)
                Global.DELETED_HOSTNAME = server['hostname']
                del Global.SERVERS[server_id]
                sync_configuration()
                Configuration.write_configuration_file()
                return '', 204
        else:
            return make_response(
                jsonify({'statusstring': 'No server found for id: %u in database %u' % (server_id, database_id)}), 404)

    @staticmethod
    def put(database_id, server_id):
        """
        Update the server with specified server_id.
        Args:
            server_id (int): The first parameter.
        Returns:
            Information of server with specified server_id after being updated
            otherwise the error message.
        """
        if 'id' in request.json and server_id != request.json['id']:
            return make_response(jsonify({'status': 404, 'statusString': 'Server Id mentioned in the payload and url doesn\'t match.'}), 404)

        database = Global.DATABASES.get(database_id)
        if database is None:
            return make_response(jsonify({'statusstring': 'No database found for id: %u' % database_id}), 404)
        else:
            members = database['members']
        if server_id in members:
            server_type_error = ValidateServerFieldType(request.json)
            if 'status' in server_type_error and server_type_error['status'] == 'error':
                return jsonify(status=401, statusString=server_type_error['errors'])

            inputs = ServerInputs(request)
            if not inputs.validate():
                return jsonify(status=401, statusString=inputs.errors)
            current_server = Global.SERVERS.get(server_id)
            if current_server is None:
                abort(404)

            result = validate_server_ports(database_id, server_id)
            if result is not None:
                return result

            current_server['name'] = \
                request.json.get('name', current_server['name'])
            current_server['hostname'] = \
                request.json.get('hostname', current_server['hostname'])
            current_server['description'] = \
                request.json.get('description', current_server['description'])
            current_server['enabled'] = \
                request.json.get('enabled', current_server['enabled'])
            current_server['admin-listener'] = \
                request.json.get('admin-listener', current_server['admin-listener'])
            current_server['internal-listener'] = \
                request.json.get('internal-listener', current_server['internal-listener'])
            current_server['http-listener'] = \
                request.json.get('http-listener', current_server['http-listener'])
            current_server['zookeeper-listener'] = \
                request.json.get('zookeeper-listener', current_server['zookeeper-listener'])
            current_server['replication-listener'] = \
                request.json.get('replication-listener', current_server['replication-listener'])
            current_server['client-listener'] = \
                request.json.get('client-listener', current_server['client-listener'])
            current_server['internal-interface'] = \
                request.json.get('internal-interface', current_server['internal-interface'])
            current_server['external-interface'] = \
                request.json.get('external-interface', current_server['external-interface'])
            current_server['public-interface'] = \
                request.json.get('public-interface', current_server['public-interface'])
            current_server['placement-group'] = \
                str(request.json.get('placement-group', current_server['placement-group']))
            current_server['isAdded'] = current_server['isAdded']
            current_server['voltdbroot'] = \
                str(request.json.get('voltdbroot', current_server['voltdbroot']))
            current_server['snapshots'] = \
                str(request.json.get('snapshots', current_server['snapshots']))
            current_server['exportoverflow'] = \
                str(request.json.get('exportoverflow', current_server['exportoverflow']))
            current_server['commandlog'] = \
                str(request.json.get('commandlog', current_server['commandlog']))
            current_server['commandlogsnapshot'] = \
                str(request.json.get('commandlogsnapshot', current_server['commandlogsnapshot']))
            current_server['droverflow'] = \
                str(request.json.get('droverflow', current_server['droverflow']))
            sync_configuration()
            Configuration.write_configuration_file()
            return jsonify({'status': 200, 'statusString': 'OK', 'server': current_server})
        else:
            return jsonify({'statusString': 'Given server with id %u doesn\'t belong to database with id %u.' % (
                server_id, database_id)})


class DatabaseAPI(MethodView):
    """
    Class to handle requests related to database
    """

    @staticmethod
    def get(database_id):
        """
        Gets the information of the database with specified database_id. If the database_id is
        not specified, then it returns the information of all databases.
        Args:
            database_id (int): The first parameter. Defaults to None.
        Returns:
            database or list of databases.
        """

        if database_id is None:
            # return a list of users
            return jsonify({'status': 200, 'statusString': 'OK', 'databases': Global.DATABASES.values()})
        else:
            # expose a single user
            database = Global.DATABASES.get(database_id)
            if database is None:
                abort(404)
            return jsonify({'status': 200, 'statusString': 'OK', 'database': Global.DATABASES.get(database_id)})

    @staticmethod
    def post():
        """
        Saves the database.
        Returns:
            Information and the status of database if it is saved otherwise the error message.
        """
        if 'id' in request.json or 'members' in request.json:
            return make_response(
                jsonify({'error': 'You cannot specify \'Id\' or \'Members\' while creating database.'}), 404)

        db_type_error = ValidateDbFieldType(request.json)
        if 'status' in db_type_error and db_type_error['status'] == 'error':
            return jsonify(status=401, statusString=db_type_error['errors'])

        inputs = DatabaseInputs(request)
        if not inputs.validate():
            return jsonify(status=401, statusString=inputs.errors)

        databases = [v if type(v) is list else [v] for v in Global.DATABASES.values()]
        if request.json['name'] in [(d["name"]) for item in databases for d in item]:
            return make_response(jsonify({'status':400, 'statusString': 'database name already exists'}), 400)

        if not Global.DATABASES:
            database_id = 1
        else:
            database_id = Global.DATABASES.keys()[-1] + 1

        Global.DATABASES[database_id] = {'id': database_id, 'name': request.json['name'], 'members': []}

        # Create new deployment
        app_root = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(app_root, "deployment.json")) as json_file:
            deployment = json.load(json_file)
            deployment['databaseid'] = database_id
            is_pro_version(deployment)
        Global.DEPLOYMENT[database_id] = deployment

        sync_configuration()

        Configuration.write_configuration_file()
        url = 'http://%s:%u/api/1.0/databases/%u' % \
              (__IP__, __PORT__, database_id)

        resp = make_response(
            jsonify({'status': 201, 'statusString': 'OK', 'database': Global.DATABASES.get(database_id)}), 201)
        resp.headers['Location'] = url

        return resp

    @staticmethod
    def put(database_id):
        """
        Updates the database with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            Information and the status of database if it is updated otherwise the error message.
        """
        if 'members' in request.json:
            return make_response(jsonify({'status':404, 'statusString': 'You cannot specify \'Members\' while updating database.'}), 404)
        if 'id' in request.json and database_id != request.json['id']:
            return make_response(jsonify({'status': 404, 'statusString': 'Database Id mentioned in the payload and url doesn\'t match.'}),
                                 404)
        db_type_error = ValidateDbFieldType(request.json)
        if 'status' in db_type_error and db_type_error['status'] == 'error':
            return jsonify(status=401, statusString=db_type_error['errors'])

        inputs = DatabaseInputs(request)
        if not inputs.validate():
            return jsonify(status=401, statusString=inputs.errors)

        database = Global.DATABASES.get(database_id)
        if database is None:
            abort(404)

        Global.DATABASES[database_id] = {'id': database_id, 'name': request.json['name'],
                                         'members': database['members']}

        sync_configuration()
        Configuration.write_configuration_file()
        return jsonify({'status': 200, 'statusString': 'OK', 'database': database})

    @staticmethod
    def delete(database_id):
        """
        Delete the database with specified database_id.
        Args:
        database_id (int): The first parameter.
        Returns:
        True if the server is deleted otherwise the error message.
        """
        members = []
        current_database = Global.DATABASES.get(database_id)
        if current_database is None:
            abort(404)
        else:
            members = current_database['members']

        for server_id in members:
            del Global.SERVERS[server_id]

        del Global.DATABASES[database_id]

        del Global.DEPLOYMENT[database_id]
        user_Id = []
        try:
            for key, value in Global.DEPLOYMENT_USERS.iteritems():
                if value["databaseid"] == database_id:
                    user_Id.append(int(value['userid']))

            for id in user_Id:
                del Global.DEPLOYMENT_USERS[id]
        except Exception, Err:
            print Err

        sync_configuration()
        Configuration.write_configuration_file()
        return '', 204


class DeploymentUserAPI(MethodView):
    """Class to handle request related to deployment."""

    @staticmethod
    def get(database_id):
        """
        Get the deployment with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            List of deployment information with specified database.
        """
        # deployment_user = Global.DEPLOYMENT_USERS.get(user_id)

        current_database = Global.DATABASES.get(database_id)
        if not current_database:
            return make_response(jsonify({'status': 404, 'statusString': 'No database found for id: %u' % database_id}), 404)

        deployment_user = []
        for key, value in Global.DEPLOYMENT_USERS.iteritems():
            if value["databaseid"] == database_id:
                deployment_user.append(value)

        return jsonify({'deployment': deployment_user})

    @staticmethod
    def post(database_id):
        """
        #     Add user information with specified username.
        #     Args:
        #         user (string): The first parameter.
        #     Returns:
        #         Deployment user object of added deployment user.
        #     """
        inputs = UserInputs(request)
        if not inputs.validate():
            return jsonify(status=401, statusString=inputs.errors)

        current_database = Global.DATABASES.get(database_id)
        if not current_database:
            return make_response(jsonify({'status': 404, 'statusString': 'No database found for id: %u' % database_id}), 404)

        is_invalid_roles = check_invalid_roles(request.json['roles'])
        if not is_invalid_roles:
            return make_response(jsonify({'status': 404, 'statusString': 'Invalid user roles.'}))

        user = [v if type(v) is list else [v] for v in Global.DEPLOYMENT_USERS.values()]
        if request.json['name'] in [(d["name"]) for item in user for d in item] and d["databaseid"] == database_id:
            return make_response(jsonify({'status': 404,  'statusString': 'user name already exists'}), 404)

        user_roles = ','.join(set(request.json['roles'].split(',')))
        if not Global.DEPLOYMENT_USERS:
            user_id = 1
        else:
            user_id = Global.DEPLOYMENT_USERS.keys()[-1] + 1

        try:

            Global.DEPLOYMENT_USERS[user_id] = {
                'userid': user_id,
                'databaseid': database_id,
                'name': request.json['name'],
                'password': urllib.unquote(str(request.json['password']).encode('ascii')).decode('utf-8'),
                'roles': user_roles,
                'plaintext': True
            }
        except Exception, err:
            print err

        sync_configuration()
        Configuration.write_configuration_file()

        return jsonify({'user': Global.DEPLOYMENT_USERS.get(user_id), 'status': 1, 'statusstring': 'User Created'})

    @staticmethod
    def put(database_id, user_id):
        #     """
        #     Add user information with specified username.
        #     Args:
        #         user (string): The first parameter.
        #     Returns:
        #         Deployment user object of added deployment user.
        #     """

        inputs = UserInputs(request)
        if not inputs.validate():
            return jsonify(status=401, statusString=inputs.errors)

        current_user = Global.DEPLOYMENT_USERS.get(user_id)
        if current_user is None:
            return make_response(jsonify({'status': 401, 'statusString': 'No user found for id: %u' % user_id}), 404)

        is_invalid_roles = check_invalid_roles(request.json['roles'])
        if not is_invalid_roles:
            return make_response(jsonify({'status': 404, 'statusString': 'Invalid user roles.'}))

        user = [v if type(v) is list else [v] for v in Global.DEPLOYMENT_USERS.values()]
        if request.json['name'] in [(d["name"]) for item in user for d in item] and d["databaseid"] == database_id \
                and request.json["name"] != current_user["name"]:
            return make_response(jsonify({'status': 404, 'statusString': 'user name already exists'}), 404)
        user_roles = ','.join(set(request.json.get('roles', current_user['roles']).split(',')))
        current_user = Global.DEPLOYMENT_USERS.get(user_id)

        current_user['name'] = request.json.get('name', current_user['name'])
        current_user['password'] = urllib.unquote(
            str(request.json.get('password', current_user['password'])).encode('ascii')).decode('utf-8')
        current_user['roles'] = user_roles
        current_user['plaintext'] = request.json.get('plaintext', current_user['plaintext'])
        sync_configuration()
        Configuration.write_configuration_file()
        return jsonify({'user': current_user, 'status': 1, 'statusstring': "User Updated"})

    @staticmethod
    def delete(database_id, user_id):
        """
        Delete the user with specified user_id.
        Args:
            user_id (int): The first parameter.
        Returns:
            True if the user is deleted otherwise the error message.
        """
        current_user = Global.DEPLOYMENT_USERS.get(user_id)
        if current_user is None:
            return make_response(jsonify({'statusstring': 'No user found for id: %u' % user_id}), 404)

        current_database = Global.DATABASES.get(database_id)
        if not current_database:
            return make_response(jsonify({'status': 404, 'statusString': 'No database found for id: %u' % database_id}), 404)

        del Global.DEPLOYMENT_USERS[user_id]

        sync_configuration()
        Configuration.write_configuration_file()

        return jsonify({'status': 1, 'statusstring': "User Deleted"})


class StartDatabaseAPI(MethodView):
    """Class to handle request to start servers on all nodes of a database."""

    @staticmethod
    def put(database_id):
        """
        Starts VoltDB database servers on all nodes for the specified database
        Args:
            database_id (int): The id of the database that should be started
        Returns:
            Status string indicating if the database start requesst was sent successfully
        """

        try:

            if 'pause' in request.args:
                is_pause = request.args.get('pause').lower()
            else:
                is_pause = "false"

            if 'force' in request.args:
                is_force = request.args.get('force')
            else:
                is_force = "false"
            database = voltdbserver.VoltDatabase(database_id)
            response = database.start_database(is_pause, is_force)
            return response
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                 500)


class RecoverDatabaseAPI(MethodView):
    """Class to handle request to start servers on all nodes of a database."""

    @staticmethod
    def put(database_id):
        """
        Starts VoltDB database servers on all nodes for the specified database
        Args:
            database_id (int): The id of the database that should be started
        Returns:
            Status string indicating if the database start request was sent successfully
        """

        try:
            if 'pause' in request.args:
                pause = request.args.get('pause')
            else:
                pause = "false"

            database = voltdbserver.VoltDatabase(database_id)
            response = database.start_database(pause, False)
            return response
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status':500, 'statusString': str(err)}),
                                 500)


class StopDatabaseAPI(MethodView):
    """Class to handle request to stop a database."""

    @staticmethod
    def put(database_id):
        """
        Stops the specified VoltDB
        Args:
            database_id (int): The id of the database that should be stopped
        Returns:
            Status string indicating if the stop request was sent successfully
        """
        if 'force' in request.args:
            is_force = request.args.get('force').lower()
        else:
            is_force = "false"

        if is_force == "true":
            server = voltdbserver.VoltDatabase(database_id)
            response = server.stop_database(database_id)
            resp_json = json.loads(response.data)
            return make_response(jsonify({'status': 200, 'statusString': resp_json['statusString']}))

        else:
            try:
                server = voltdbserver.VoltDatabase(database_id)
                response = server.kill_database()
                # Don't use the response in the json we send back
                # because voltadmin shutdown gives 'Connection broken' output
                resp_json = json.loads(json.loads(response.data)['statusString'])
                resp_status = {}

                for value in resp_json:
                    resp_status[value] = {'status': json.loads(resp_json[value])['statusString']}
                return make_response(jsonify({'status': 200, 'statusString': str(resp_status)}),
                                     200)
            except Exception, err:
                print traceback.format_exc()
                return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                     500)


class StopServerAPI(MethodView):
    """Class to handle request to stop a server."""

    @staticmethod
    def put(database_id, server_id):
        """
        Stops VoltDB database server on the specified server
        Args:
            database_id (int): The id of the database that should be stopped
            server_id (int): The id of the server node that is to be stopped
        Returns:
            Status string indicating if the stop request was sent successfully
        """

        try:
            if 'force' in request.args:
                force = request.args.get('force').lower()
            else:
                force = "false"

            server = voltdbserver.VoltDatabase(database_id)
            response = server.stop_server(server_id, force)
            resp_json = json.loads(response.data)
            if response.status_code == 500:
                return make_response(jsonify({'status': 500, 'statusString': resp_json['statusString']}), 500)
            else:
                return make_response(jsonify({'status': 200, 'statusString': resp_json['statusString']}), 200)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                 500)


class StopLocalServerAPI(MethodView):
    """Class to handle request to stop a server."""

    @staticmethod
    def put(database_id):
        """
        Stops VoltDB database server on the local server
        Args:
            database_id (int): The id of the database that should be stopped
        Returns:
            Status string indicating if the stop request was sent successfully
        """

        if 'force' in request.args:
            is_force = request.args.get('force').lower()
        else:
            is_force = 'false'
        if 'id' in request.args:
            sid = int(request.args.get('id'))

        if is_force == "false":
            try:
                server = voltdbserver.VoltDatabase(database_id)
                response = server.kill_server(sid)
                if 'Connection broken' in response.data:
                    return make_response(jsonify({'status': 200, 'statusString': 'SUCCESS: Server shutdown '
                                                                                 'successfully.'}))
                else:
                    return make_response(jsonify({'status': 200, 'statusString': response.data}))
            except Exception, err:
                print traceback.format_exc()
                return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                     500)
        else:
            try:
                server = voltdbserver.VoltDatabase(database_id)
                response = server.stop_db_server(sid)
                if 'Connection broken' in response:
                    return make_response(jsonify({'status': 200, 'statusString': 'SUCCESS: Server shutdown successfully.'}))
                else:
                    return make_response(jsonify({'status': 200, 'statusString': response}))
            except Exception, err:
                print traceback.format_exc()
                return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                     500)


class StartServerAPI(MethodView):
    """Class to handle request to start a server for this database."""

    @staticmethod
    def put(database_id, server_id):
        """
        Starts VoltDB database server on the specified server
        Args:
            database_id (int): The id of the database that should be started
            server_id (int): The id of the server node that is to be started
        Returns:
            Status string indicating if the server node start request was sent successfully
        """

        try:
            if 'pause' in request.args:
                pause = request.args.get('pause')
            else:
                pause = "false"

            if 'force' in request.args:
                is_force = request.args.get('force')
            else:
                is_force = "false"

            server = voltdbserver.VoltDatabase(database_id)
            response = server.start_server(server_id, pause, is_force)
            resp_json = json.loads(response.data)
            if response.status_code == 500:
                return make_response(jsonify({'status': 500, 'statusString': resp_json['statusString']}), 500)
            else:
                return make_response(jsonify({'status': 200, 'statusString': resp_json['statusString']}), 200)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                 500)


class StartLocalServerAPI(MethodView):
    """Class to handle request to start local server for this database."""

    @staticmethod
    def put(database_id):
        """
        Starts VoltDB database server on this local machine
        Args:
            database_id (int): The id of the database that should be started
        Returns:
            Status string indicating if the server start request was sent successfully
        """

        try:
            sid = -1
            if 'pause' in request.args:
                pause = request.args.get('pause')
            else:
                pause = "false"

            if 'force' in request.args:
                force = request.args.get('force')
            else:
                force = "false"

            if 'id' in request.args:
                sid = int(request.args.get('id'))

            server = voltdbserver.VoltDatabase(database_id)
            return server.check_and_start_local_server(sid, pause, database_id, force, False)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                 500)


class RecoverServerAPI(MethodView):
    """Class to handle request to issue recover cmd on this local server."""

    @staticmethod
    def put(database_id):
        """
        Issues recover cmd on this local server
        Args:
            database_id (int): The id of the database that should be started
        Returns:
            Status string indicating if the request was sent successfully
        """

        try:
            sid = -1
            if 'pause' in request.args:
                is_pause = request.args.get('pause').lower()
            else:
                is_pause = "false"

            if 'id' in request.args:
                sid = int(request.args.get('id'))
            server = voltdbserver.VoltDatabase(database_id)
            response = server.check_and_start_local_server(sid, is_pause, database_id, False)
            return response
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                 500)


class AddServerAPI(MethodView):
    """Class to handle request to join the server to existing cluster."""

    @staticmethod
    def put(database_id, server_id):
        """
        Issues add cmd on this local server
        Args:
            database_id (int): The id of the database that should be started
        Returns:
            Status string indicating if the request was sent successfully
        """
        try:
            server = voltdbserver.VoltDatabase(database_id)
            response = server.start_server(server_id, 'false', 'false', True)
            resp_json = json.loads(response.data)
            if response.status_code == 500:
                return make_response(jsonify({'status': '500', 'statusString': resp_json['statusString']}), 500)
            else:
                return make_response(jsonify({'status': '200', 'statusString': resp_json['statusString']}), 200)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'statusString': str(err)}),
                                 500)


class AddLocalServerAPI(MethodView):
    """Class to handle request to start local server for this database."""

    @staticmethod
    def put(database_id):
        """
        Starts VoltDB database server on this local machine
        Args:
            database_id (int): The id of the database that should be started
        Returns:
            Status string indicating if the server start request was sent successfully
        """

        try:
            sid = -1
            if 'id' in request.args:
                sid = int(request.args.get('id'))
            server = voltdbserver.VoltDatabase(database_id)
            return server.check_and_start_local_server(sid, 'false', database_id, 'false', True)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'status': 500, 'statusString': str(err)}),
                                 500)


class VdmStatus(MethodView):
    """
    Class to get VDM status for peers to check.
    """

    @staticmethod
    def get():
        if request.args is not None and 'jsonp' in request.args and request.args['jsonp'] is not None:
            return str(request.args['jsonp']) + '(' + '{\'voltdeploy\': {"running": "true"}}' + ')'
        else:
            return jsonify({'voltdeploy': {"running": "true"}})


class SyncVdmConfiguration(MethodView):
    """
    Class to sync configuration between two servers.
    """

    @staticmethod
    def post():
        try:
            result = request.json

            databases = result['voltdeploy']['databases']
            databases = dict((int(key), value) for (key, value) in databases.items())

            servers = result['voltdeploy']['members']
            servers = dict((int(key), value) for (key, value) in servers.items())

            deployments = result['voltdeploy']['deployments']
            deployments = dict((int(key), value) for (key, value) in deployments.items())

            deployment_users = result['voltdeploy']['deployment_users']
            deployment_users = dict((int(key), value) for (key, value) in deployment_users.items())

        except Exception, errs:
            print traceback.format_exc()
            return jsonify({'status': 'success', 'statusString': str(errs)})

        try:
            Global.DATABASES = databases
            Global.SERVERS = servers
            Global.DEPLOYMENT = deployments
            Global.DEPLOYMENT_USERS = deployment_users
        except Exception, errs:
            print traceback.format_exc()
            return jsonify({'status': 'success', 'statusString': str(errs)})
        Configuration.write_configuration_file()
        return jsonify({'status': '201', 'statusString': 'success'})


class VdmConfiguration(MethodView):
    """
    Class related to the vdm configuration
    """

    @staticmethod
    def get():
        return jsonify(Configuration.get_configuration())

    @staticmethod
    def post():

        result = Configuration.get_configuration()
        d = result['voltdeploy']['members']
        for key, value in d.iteritems():
            try:
                headers = {'content-type': 'application/json'}
                url = 'http://%s:%u/api/1.0/voltdeploy/sync_configuration/' % (d[key]['hostname'], __PORT__)
                data = result
                response = requests.post(url, data=json.dumps(data), headers=headers)
            except Exception, errs:
                print traceback.format_exc()
                print str(errs)

        if Global.DELETED_HOSTNAME != '':
            try:
                headers = {'content-type': 'application/json'}
                url = 'http://%s:%u/api/1.0/voltdeploy/sync_configuration/' % (Global.DELETED_HOSTNAME, __PORT__)
                data = result
                response = requests.post(url, data=json.dumps(data), headers=headers)
                Global.DELETED_HOSTNAME = ''
            except Exception, errs:
                print traceback.format_exc()
                print str(errs)

        return jsonify({'deployment': response.status_code})


class DatabaseDeploymentAPI(MethodView):
    """
    Class related to the vdm configuration
    """

    @staticmethod
    def get(database_id):
        if 'Accept' in request.headers and 'application/json' in request.headers['Accept']:
            deployment = Global.DEPLOYMENT.get(database_id)

            new_deployment = deployment.copy()

            new_deployment['users'] = {}
            new_deployment['users']['user'] = []

            deployment_user = [v if type(v) is list else [v] for v in Global.DEPLOYMENT_USERS.values()]

            if deployment_user is not None:
                for user in deployment_user:
                    if user[0]['databaseid'] == database_id:
                        new_deployment['users']['user'].append({
                            'name': user[0]['name'],
                            'roles': user[0]['roles'],
                            'plaintext': user[0]['plaintext']

                        })

            del new_deployment['databaseid']

            return jsonify({'deployment': new_deployment})
        else:
            deployment_content = DeploymentConfig.DeploymentConfiguration.get_database_deployment(database_id)
            return Response(deployment_content, mimetype='text/xml')

    @staticmethod
    def put(database_id):
        if 'application/json' in request.headers['Content-Type']:
            inputs = JsonInputs(request)
            if not inputs.validate():
                return jsonify(status=401, statusString=inputs.errors)
            result = Configuration.check_validation_deployment(request)
            if 'status' in result and result['status'] == 401:
                return jsonify(result)
            if 'dr' in request.json and request.json['dr'] and 'id' not in request.json['dr']:
                return jsonify({'status': '401', 'statusString': 'DR id is required.'})
            deployment = map_deployment(request, database_id)
            sync_configuration()
            Configuration.write_configuration_file()
            return jsonify({'status': 200, 'statusString': 'Ok', 'deployment': deployment})
        else:
            result = Configuration.set_deployment_for_upload(database_id, request)
            if 'status' in result and result['status'] == 401:
                return jsonify({'status': 401, 'statusString': result['statusString']})
            else:
                return jsonify({'status': 201, 'statusString': 'success'})


class VdmAPI(MethodView):
    """
    Class to return vdm.xml file
    """

    @staticmethod
    def get():
        vdm_content = Configuration.make_configuration_file()
        return Response(vdm_content, mimetype='text/xml')


class StatusDatabaseAPI(MethodView):
    """Class to return status of database and its servers."""

    @staticmethod
    def get(database_id):
        serverDetails = []
        status = ''

        database = Global.DATABASES.get(database_id)
        has_stalled = False
        has_run = False
        is_server_unreachable = False
        if not database:
            return make_response(jsonify({"status": 404, 'statusString': 'Not found'}), 404)
        else:
            if len(database['members']) == 0:
                return jsonify({"status": 404, "statusString": "No Members"})
            for server_id in database['members']:
                server = Global.SERVERS.get(server_id)
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%u/status/') % \
                      (server['hostname'], __PORT__, database_id, server['id'])
                try:
                    response = requests.get(url)
                    if response.json()['serverStatus']['status'] == "stalled":
                        has_stalled = True
                    elif response.json()['serverStatus']['status'] == "running":
                        has_run = True
                    value = response.json()

                    if 'status' in response.json():
                        del value['status']
                        del value['statusString']
                    serverDetails.append({server['hostname']: value['serverStatus']})
                except Exception, err:
                    #return jsonify({"status": 404, "statusString": "error"})
                    serverDetails.append({server['hostname']: {'status': 'unreachable', 'details': 'Server is unreachable', 'isInitialized': False}})
                    is_server_unreachable = True
            if is_server_unreachable:
                status = 'unreachable'
            elif has_run:
                status = 'running'
            elif has_stalled:
                status = 'stalled'
            elif not has_run and not has_stalled:
                status = 'stopped'

            isFreshStart = voltdbserver.check_snapshot_folder(database_id)

            return jsonify({'status': 200, 'statusString': 'OK',
                            'dbStatus': {'status': status, 'serverStatus': serverDetails,
                                         'isFreshStart': isFreshStart}})


class StatusDatabaseServerAPI(MethodView):
    """Class to return status of servers"""

    @staticmethod
    def get(database_id, server_id):
        database = Global.DATABASES.get(database_id)
        if not database:
            return make_response(jsonify({"status": 404, "statusString": "Not found"}), 404)
        else:
            server = Global.SERVERS.get(server_id)
            if len(database['members']) == 0:
                return jsonify({"status": 200, "statusString": "OK", 'statusString': 'errorNoMembers'})
            if not server:
                return make_response(jsonify({"status": 404, "statusString": "Not found"}), 404)
            elif server_id not in database['members']:
                return make_response(jsonify({"status": 404, "statusString": "Not found"}), 404)
            else:

                try:
                    if not server['client-listener']:
                        client_port = 21212
                        client_host = str(server['hostname'])
                    else:
                        client_listener = server['client-listener']
                        if ":" in client_listener:
                            arr_client = client_listener.split(':', 2)
                            client_port = int(arr_client[1])
                            client_host = str(arr_client[0])
                        else:
                            client_port = int(client_listener)
                            client_host = str(server['hostname'])

                    client = voltdbclient.FastSerializer(client_host, client_port)
                    proc = voltdbclient.VoltProcedure(client, "@Ping")
                    response = proc.call()
                    success = ''
                    try:
                        success = Log.get_error_log_details()
                    except:
                        pass

                    return jsonify({'status': 200, 'statusString': 'OK', 'serverStatus': {'status': "running",
                                                                                          'details': success
                                                                                          }})
                    success = ''
                    try:
                        success = Log.get_error_log_details()
                    except:
                        pass
                    voltProcess = voltdbserver.VoltDatabase(database_id)
                    if voltProcess.Get_Voltdb_Process(database_id).isProcessRunning:
                        return jsonify({'status': 200, 'statusString': 'OK', 'serverStatus': {'status': "running",
                                                                                              'details': success
                                                                                              }})
                    else:
                        return jsonify({'status': 200, 'statusString': 'OK', 'serverStatus': {'status': "stopped",
                                                                                              'details': success
                                                                                              }})
                except:
                    voltProcess = voltdbserver.VoltDatabase(database_id)
                    error = ''
                    try:
                        error = Log.get_error_log_details()
                    except:
                        pass
                    if voltProcess.Get_Voltdb_Process(database_id).isProcessRunning:
                        return jsonify({'status': 200, 'statusString': 'OK', 'serverStatus': {'status': "stalled",
                                                                                              'details': error
                                                                                              }})
                    else:
                        return jsonify({'status': 200, 'statusString': 'OK', 'serverStatus': {'status': "stopped",
                                                                                              'details': error
                                                                                              }})


def main(runner, amodule, config_dir, data_dir, server):
    try:
        F_DEBUG = os.environ['DEBUG']
    except KeyError:
        F_DEBUG = 'False'

    if F_DEBUG == 'True':
        APP.config.update(DEBUG=True)

    path = os.path.dirname(amodule.__file__)
    Global.MODULE_PATH = path
    depjson = os.path.join(path, "deployment.json")
    json_data = open(depjson).read()
    deployment = json.loads(json_data)
    Global.CONFIG_PATH = config_dir
    Global.DATA_PATH = data_dir
    global __IP__
    global __PORT__


    config_path = os.path.join(config_dir, 'voltdeploy.xml')

    arrServer = {}
    bindIp = "0.0.0.0"
    if server is not None:
        arrServer = server.split(':', 2)
        __host_name__ = arrServer[0]
        __host_or_ip__ = arrServer[0]
        bindIp = __host_or_ip__
        __PORT__ = int(8000)
        __IP__ = arrServer[0]
        if len(arrServer) >= 2:
            __PORT__ = int(arrServer[1])
    else:
        __host_name__ = socket.gethostname()
        __host_or_ip__ = socket.gethostbyname(__host_name__)
        __IP__ = __host_or_ip__
        __PORT__ = int(8000)

    if os.path.exists(config_path):
        Configuration.convert_xml_to_json(config_path)
    else:
        is_pro_version(deployment)

        Global.DEPLOYMENT[deployment['databaseid']] = deployment

        Global.SERVERS[1] = {'id': 1, 'name': __host_name__, 'hostname': __host_or_ip__, 'description': "",
                             'enabled': True, 'external-interface': "", 'internal-interface': "",
                             'public-interface': "", 'client-listener': "", 'internal-listener': "",
                             'admin-listener': "", 'http-listener': "", 'replication-listener': "",
                             'zookeeper-listener': "", 'placement-group': "", 'isAdded': False, 'voltdbroot': "",
                             'snapshots': "", 'exportoverflow': "", 'commandlog': "",
                             'commandlogsnapshot': "", 'droverflow': ""}
        Global.DATABASES[1] = {'id': 1, 'name': "Database", "members": [1]}


    Configuration.write_configuration_file()

    SERVER_VIEW = ServerAPI.as_view('server_api')
    DATABASE_VIEW = DatabaseAPI.as_view('database_api')

    START_LOCAL_SERVER_VIEW = StartLocalServerAPI.as_view('start_local_server_api')
    RECOVER_DATABASE_SERVER_VIEW = RecoverServerAPI.as_view('recover_server_api')
    STOP_DATABASE_SERVER_VIEW = StopServerAPI.as_view('stop_server_api')
    START_DATABASE_VIEW = StartDatabaseAPI.as_view('start_database_api')
    START_DATABASE_SERVER_VIEW = StartServerAPI.as_view('start_server_api')
    STOP_DATABASE_VIEW = StopDatabaseAPI.as_view('stop_database_api')
    RECOVER_DATABASE_VIEW = RecoverDatabaseAPI.as_view('recover_database_api')
    DEPLOYMENT_USER_VIEW = DeploymentUserAPI.as_view('deployment_user_api')
    VDM_STATUS_VIEW = VdmStatus.as_view('vdm_status_api')
    VDM_CONFIGURATION_VIEW = VdmConfiguration.as_view('vdm_configuration_api')
    SYNC_VDM_CONFIGURATION_VIEW = SyncVdmConfiguration.as_view('sync_vdm_configuration_api')
    DATABASE_DEPLOYMENT_VIEW = DatabaseDeploymentAPI.as_view('database_deployment_api')
    STATUS_DATABASE_VIEW = StatusDatabaseAPI.as_view('status_database_api')
    STATUS_DATABASE_SERVER_VIEW = StatusDatabaseServerAPI.as_view('status_database_server_view')
    VDM_VIEW = VdmAPI.as_view('vdm_api')
    ADD_SERVER_VIEW = AddServerAPI.as_view('add_server_api')
    ADD_LOCAL_SERVER_VIEW = AddLocalServerAPI.as_view('add_local_server_api')
    STOP_LOCAL_SERVER_VIEW = StopLocalServerAPI.as_view('stop_local_server_api')

    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/', strict_slashes=False,
                     view_func=SERVER_VIEW, methods=['GET', 'POST'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/', strict_slashes=False,
                     view_func=SERVER_VIEW, methods=['GET', 'PUT', 'DELETE'])
    APP.add_url_rule('/api/1.0/databases/', strict_slashes=False, defaults={'database_id': None},
                     view_func=DATABASE_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>', strict_slashes=False, view_func=DATABASE_VIEW,
                     methods=['GET', 'PUT', 'DELETE'])
    APP.add_url_rule('/api/1.0/databases/', strict_slashes=False, view_func=DATABASE_VIEW, methods=['POST'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/start', strict_slashes=False,
                     view_func=START_DATABASE_SERVER_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/stop', strict_slashes=False,
                     view_func=STOP_DATABASE_SERVER_VIEW, methods=['PUT'])

    APP.add_url_rule('/api/1.0/databases/<int:database_id>/start', strict_slashes=False,
                     view_func=START_DATABASE_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/stop', strict_slashes=False,
                     view_func=STOP_DATABASE_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/recover', strict_slashes=False,
                     view_func=RECOVER_DATABASE_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/add', strict_slashes=False,
                     view_func=ADD_SERVER_VIEW, methods=['PUT'])
    # Internal API
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/start', strict_slashes=False,
                     view_func=START_LOCAL_SERVER_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/recover', strict_slashes=False,
                     view_func=RECOVER_DATABASE_SERVER_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/status/', strict_slashes=False,
                     view_func=STATUS_DATABASE_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/status/', strict_slashes=False,
                     view_func=STATUS_DATABASE_SERVER_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/users/<int:user_id>/', strict_slashes=False,
                     view_func=DEPLOYMENT_USER_VIEW, methods=['PUT', 'DELETE'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/users/', strict_slashes=False,
                     view_func=DEPLOYMENT_USER_VIEW, methods=['GET', 'POST'])
    APP.add_url_rule('/api/1.0/voltdeploy/status/', strict_slashes=False,
                     view_func=VDM_STATUS_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/voltdeploy/configuration/', strict_slashes=False,
                     view_func=VDM_CONFIGURATION_VIEW, methods=['GET', 'POST'])
    APP.add_url_rule('/api/1.0/voltdeploy/sync_configuration/', strict_slashes=False,
                     view_func=SYNC_VDM_CONFIGURATION_VIEW, methods=['POST'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/deployment/', strict_slashes=False,
                     view_func=DATABASE_DEPLOYMENT_VIEW,
                     methods=['GET', 'PUT'])
    APP.add_url_rule('/api/1.0/voltdeploy/', strict_slashes=False, view_func=VDM_VIEW,
                     methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/add', strict_slashes=False,
                     view_func=ADD_LOCAL_SERVER_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/stop', strict_slashes=False,
                     view_func=STOP_LOCAL_SERVER_VIEW, methods=['PUT'])

    log_file = os.path.join(Global.DATA_PATH, 'voltdeploy.log')
    if os.path.exists(log_file):
        open(log_file, 'w').close()
    handler = RotatingFileHandler(log_file)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s|%(levelname)s|%(message)s"))
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.NOTSET)
    log.addHandler(handler)

    APP.run(threaded=True, host=bindIp, port=__PORT__)
