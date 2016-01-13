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

from flask import Flask, render_template, jsonify, abort, make_response, request
from flask.views import MethodView
from Validation import ServerInputs, DatabaseInputs, JsonInputs, UserInputs
import socket
import os
import json
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
import sys


APP = Flask(__name__, template_folder="../templates", static_folder="../static")

SERVERS = []

DATABASES = []

DEPLOYMENT = []

DEPLOYMENT_USERS = []

__PATH__ = ""


@APP.errorhandler(400)
def not_found(error):
    """
    Gives error message when any bad requests are made.
    Args:
        error (string): The first parameter.
    Returns:
        Error message.
    """
    print error
    return make_response(jsonify({'error': 'Bad request'}), 400)


@APP.errorhandler(404)
def not_found(error):
    """
    Gives error message when any invalid url are requested.
    Args:
        error (string): The first parameter.
    Returns:
        Error message.
    """
    print error
    return make_response(jsonify({'error': 'Not found'}), 404)


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


def map_deployment_without_database_id(deployment):
    """
    Get the deployment information without database_id in required format.
    Args:
        deployment (deployment object): The first parameter.
    Returns:
        Deployment object in required format.
    """

    new_deployment = {}

    for field in deployment:
        if 'databaseid' not in field:
            new_deployment[field] = deployment[field]

    new_deployment['users'] = {}
    new_deployment['users']['user'] = []

    deployment_user = filter(lambda t: t['databaseid'] == deployment['databaseid'], DEPLOYMENT_USERS)

    for user in deployment_user:
        new_deployment['users']['user'].append({
            'name': user['name'],
            'roles': user['roles'],
            'plaintext': user['plaintext']
        })
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
    deployment = filter(lambda t: t['databaseid'] == database_id, DEPLOYMENT)

    if 'cluster' in request.json and 'elastic' in request.json['cluster']:
        deployment[0]['cluster']['elastic'] = request.json['cluster']['elastic']

    if 'cluster' in request.json and 'schema' in request.json['cluster']:
        deployment[0]['cluster']['schema'] = request.json['cluster']['schema']

    if 'cluster' in request.json and 'sitesperhost' in request.json['cluster']:
        deployment[0]['cluster']['sitesperhost'] = request.json['cluster']['sitesperhost']

    if 'cluster' in request.json and 'kfactor' in request.json['cluster']:
        deployment[0]['cluster']['kfactor'] = request.json['cluster']['kfactor']

    if 'admin-mode' in request.json and 'adminstartup' in request.json['admin-mode']:
        deployment[0]['admin-mode']['adminstartup'] = request.json['admin-mode']['adminstartup']

    if 'admin-mode' in request.json and 'port' in request.json['admin-mode']:
        deployment[0]['admin-mode']['port'] = request.json['admin-mode']['port']

    if 'commandlog' in request.json and 'adminstartup' in request.json['commandlog']:
        deployment[0]['commandlog']['adminstartup'] = request.json['commandlog']['adminstartup']

    if 'commandlog' in request.json and 'frequency' in \
            request.json['commandlog'] and 'time' in request.json['commandlog']['frequency']:
        deployment[0]['commandlog']['frequency']['time'] = request.json['commandlog']['frequency']['time']

    if 'commandlog' in request.json and 'frequency' in \
            request.json['commandlog'] and 'transactions' in request.json['commandlog']['frequency']:
        deployment[0]['commandlog']['frequency']['transactions'] = request.json['commandlog']['frequency'][
            'transactions']

    if 'commandlog' in request.json and 'enabled' in request.json['commandlog']:
        deployment[0]['commandlog']['enabled'] = request.json['commandlog']['enabled']

    if 'commandlog' in request.json and 'logsize' in request.json['commandlog']:
        deployment[0]['commandlog']['logsize'] = request.json['commandlog']['logsize']

    if 'commandlog' in request.json and 'synchronous' in request.json['commandlog']:
        deployment[0]['commandlog']['synchronous'] = request.json['commandlog']['synchronous']

    if 'heartbeat' in request.json and 'timeout' in request.json['heartbeat']:
        deployment[0]['heartbeat']['timeout'] = request.json['heartbeat']['timeout']

    if 'httpd' in request.json and 'enabled' in request.json['httpd']:
        deployment[0]['httpd']['enabled'] = request.json['httpd']['enabled']

    if 'httpd' in request.json and 'jsonapi' in request.json['httpd'] and 'enabled' in request.json['httpd'][
        'jsonapi']:
        deployment[0]['httpd']['jsonapi']['enabled'] = request.json['httpd']['jsonapi']['enabled']

    if 'httpd' in request.json and 'port' in request.json['httpd']:
        deployment[0]['httpd']['port'] = request.json['httpd']['port']

    if 'partition-detection' in request.json and 'enabled' in request.json['partition-detection']:
        deployment[0]['partition-detection']['enabled'] = request.json['partition-detection']['enabled']

    if 'partition-detection' in request.json and 'snapshot' in request.json['partition-detection'] \
            and 'prefix' in request.json['partition-detection']['snapshot']:
        deployment[0]['partition-detection']['snapshot']['prefix'] = \
            request.json['partition-detection']['snapshot']['prefix']

    if 'paths' in request.json and 'commandlog' in request.json['paths'] and \
                    'path' in request.json['paths']['commandlog']:
        deployment[0]['paths']['commandlog']['path'] = \
            request.json['paths']['commandlog']['path']

    if 'paths' in request.json and 'commandlogsnapshot' in request.json['paths'] and \
                    'path' in request.json['paths']['commandlogsnapshot']:
        deployment[0]['paths']['commandlogsnapshot']['path'] = \
            request.json['paths']['commandlogsnapshot']['path']

    if 'paths' in request.json and 'droverflow' in request.json['paths'] and \
                    'path' in request['paths']['droverflow']:
        deployment[0]['paths']['droverflow']['path'] = \
            request.json['paths']['droverflow']['path']

    if 'paths' in request.json and 'exportoverflow' in request.json['paths'] and \
                    'path' in request.json['paths']['exportoverflow']:
        deployment[0]['paths']['exportoverflow']['path'] = \
            request.json['paths']['exportoverflow']['path']

    if 'paths' in request.json and 'snapshots' in request.json['paths'] and \
                    'path' in request.json['paths']['snapshots']:
        deployment[0]['paths']['snapshots']['path'] = \
            request.json['paths']['snapshots']['path']

    if 'paths' in request.json and 'voltdbroot' in request.json['paths'] and \
                    'path' in request.json['paths']['voltdbroot']:
        deployment[0]['paths']['voltdbroot']['path'] = \
            request.json['paths']['voltdbroot']['path']

    if 'security' in request.json and 'enabled' in request.json['security']:
        deployment[0]['security']['enabled'] = request.json['security']['enabled']

    if 'security' in request.json and 'frequency' in request.json['security']:
        deployment[0]['security']['frequency'] = request.json['security']['frequency']

    if 'security' in request.json and 'provider' in request.json['security']:
        deployment[0]['security']['provider'] = request.json['security']['provider']

    if 'snapshot' in request.json and 'enabled' in request.json['snapshot']:
        deployment[0]['snapshot']['enabled'] = request.json['snapshot']['enabled']

    if 'snapshot' in request.json and 'frequency' in request.json['snapshot']:
        deployment[0]['snapshot']['frequency'] = request.json['snapshot']['frequency']

    if 'snapshot' in request.json and 'prefix' in request.json['snapshot']:
        deployment[0]['snapshot']['prefix'] = request.json['snapshot']['prefix']

    if 'snapshot' in request.json and 'retain' in request.json['snapshot']:
        deployment[0]['snapshot']['retain'] = request.json['snapshot']['retain']

    if 'systemsettings' in request.json and 'elastic' in request.json['systemsettings'] \
            and 'duration' in request.json['systemsettings']['elastic']:
        deployment[0]['systemsettings']['elastic']['duration'] = request.json['systemsettings']['elastic'][
            'duration']

    if 'systemsettings' in request.json and 'elastic' in request.json['systemsettings'] \
            and 'throughput' in request.json['systemsettings']['elastic']:
        deployment[0]['systemsettings']['elastic']['throughput'] = request.json['systemsettings']['elastic'][
            'throughput']

    if 'systemsettings' in request.json and 'query' in request.json['systemsettings'] \
            and 'timeout' in request.json['systemsettings']['query']:
        deployment[0]['systemsettings']['query']['timeout'] = request.json['systemsettings']['query']['timeout']

    if 'systemsettings' in request.json and 'temptables' in request.json['systemsettings'] \
            and 'maxsize' in request.json['systemsettings']['temptables']:
        deployment[0]['systemsettings']['temptables']['maxsize'] = request.json['systemsettings']['temptables'][
            'maxsize']

    if 'systemsettings' in request.json and 'snapshot' in request.json['systemsettings'] \
            and 'priority' in request.json['systemsettings']['snapshot']:
        deployment[0]['systemsettings']['snapshot']['priority'] = request.json['systemsettings']['snapshot']['priority']

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings'] \
            and 'disklimit' in request.json['systemsettings']['resourcemonitor'] \
            and 'feature' in request.json['systemsettings']['resourcemonitor']['disklimit'] \
            and 'name' in request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
        deployment[0]['systemsettings']['resourcemonitor']['disklimit']['feature']['name'] = \
            request.json['systemsettings']['resourcemonitor']['disklimit']['feature']['name']

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings'] \
            and 'disklimit' in request.json['systemsettings']['resourcemonitor'] \
            and 'feature' in request.json['systemsettings']['resourcemonitor']['disklimit'] \
            and 'size' in request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
        deployment[0]['systemsettings']['resourcemonitor']['disklimit']['feature']['size'] = \
            request.json['systemsettings']['resourcemonitor']['disklimit']['feature']['size']

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings'] \
            and 'memorylimit' in request.json['systemsettings']['resourcemonitor'] \
            and 'size' in request.json['systemsettings']['resourcemonitor']['memorylimit']:
        deployment[0]['systemsettings']['resourcemonitor']['memorylimit']['size'] = \
            request.json['systemsettings']['resourcemonitor']['memorylimit']['size']

    if 'import' in request.json and 'configuration' in request.json['import']:
        deployment[0]['import']['configuration'] = []
        i = 0
        for configuration in request.json['import']['configuration']:
            deployment[0]['import']['configuration'].append(
                {
                    'enabled': configuration['enabled'],
                    'module': configuration['module'],
                    'type': configuration['type'],
                    'property': []
                }
            )

            if 'property' in configuration:
                for property in configuration['property']:
                    deployment[0]['import']['configuration'][i]['property'].append(
                        {
                            'name': property['name'],
                            'value': property['value']
                        }
                    )
                i += 1

    if 'export' in request.json and 'configuration' in request.json['export']:
        deployment[0]['export']['configuration'] = []
        i = 0
        for configuration in request.json['export']['configuration']:
            deployment[0]['export']['configuration'].append(
                {
                    'enabled': configuration['enabled'],
                    'stream': configuration['stream'],
                    'type': configuration['type'],
                    'exportconnectorclass': configuration['exportconnectorclass'],
                    'property': []
                }
            )

            if 'property' in configuration:
                for property in configuration['property']:
                    deployment[0]['export']['configuration'][i]['property'].append(
                        {
                            'name': property['name'],
                            'value': property['value']
                        }
                    )
                i += 1

    if 'users' in request.json and 'user' in request.json['users']:
        deployment[0]['users'] = {}
        deployment[0]['users']['user'] = []
        for user in request.json['users']['user']:
            deployment[0]['users']['user'].append(
                {
                    'name': user['name'],
                    'roles': user['roles'],
                    'password': user['password'],
                    'plaintext': user['plaintext']
                }
            )

    if 'dr' in request.json:
        if deployment[0]['dr'] is None:
            deployment[0]['dr'] = {}

    if 'dr' in request.json and 'connection' in request.json['dr']:
        if not hasattr(deployment[0]['dr'], 'connection'):
            deployment[0]['dr']['connection'] = {}

    if 'dr' in request.json and 'id' in request.json['dr']:
        deployment[0]['dr']['id'] = request.json['dr']['id']

    if 'dr' in request.json and 'type' in request.json['dr']:
        deployment[0]['dr']['type'] = request.json['dr']['type']

    if 'dr' in request.json and 'enabled' in request.json['dr']:
        deployment[0]['dr']['enabled'] = request.json['dr']['enabled']

    if 'dr' in request.json and 'connection' in request.json['dr'] \
            and 'source' in request.json['dr']['connection']:
        deployment[0]['dr']['connection']['source'] = request.json['dr']['connection']['source']

    if 'dr' in request.json and 'connection' in request.json['dr'] \
            and 'servers' in request.json['dr']['connection']:
        deployment[0]['dr']['connection']['servers'] = request.json['dr']['connection']['servers']

    if 'dr' in request.json and not request.json['dr']:
        deployment[0]['dr'] = {}

    if 'dr' in request.json and 'connection' in request.json['dr'] and not request.json['dr']['connection']:
        deployment[0]['dr']['connection'] = {}

    return deployment[0]


def map_deployment_users(request, user):
    if 'name' not in DEPLOYMENT_USERS:
        DEPLOYMENT_USERS.append(
            {
                'databaseid': request.json['databaseid'],
                'name': request.json['name'],
                'password': request.json['password'],
                'roles': request.json['roles'],
                'plaintext': request.json['plaintext']
            }
        )
        deployment_user = filter(lambda t: t['name'] == user, DEPLOYMENT_USERS)
    else:
        deployment_user = filter(lambda t: t['name'] == user, DEPLOYMENT_USERS)

        if len(deployment_user) != 0:
            deployment_user[0]['name'] = request.json['name']
            deployment_user[0]['password'] = request.json['password']
            deployment_user[0]['plaintext'] = request.json['plaintext']
            deployment_user[0]['roles'] = request.json['roles']

    return deployment_user[0]


def get_database_deployment(key):
    deployment_top = Element('deployment')
    i = 0
    value = DEPLOYMENT[key-1]
    if type(value) is dict:
        handle_deployment_dict(deployment_top, key, value, True)
    else:
        if isinstance(value, bool):
            if value == False:
                deployment_top.attrib[key] = "false"
            else:
                deployment_top.attrib[key] = "true"
        else:
            deployment_top.attrib[key] = str(value)

    return tostring(deployment_top,encoding='UTF-8')

def make_configuration_file():
    main_header = Element('vdm')
    db_top = SubElement(main_header, 'databases')
    server_top = SubElement(main_header, 'members')
    deployment_top = SubElement(main_header, 'deployments')
    #db1 = get_database_deployment(1)
    i = 0
    while i < len(DATABASES):
        db_elem = SubElement(db_top, 'database')
        for key, value in DATABASES[i].iteritems():
            if isinstance(value, bool):
                if value == False:
                    db_elem.attrib[key] = "false"
                else:
                    db_elem.attrib[key] = "true"
            else:
                db_elem.attrib[key] = str(value)
        i += 1

    i = 0
    while i < len(SERVERS):
        server_elem = SubElement(server_top, 'member')
        for key, value in SERVERS[i].iteritems():
            if isinstance(value, bool):
                if value == False:
                    server_elem.attrib[key] = "false"
                else:
                    server_elem.attrib[key] = "true"
            else:
                server_elem.attrib[key] = str(value)
        i += 1

    i = 0
    while i < len(DEPLOYMENT):
        deployment_elem = SubElement(deployment_top, 'deployment')
        for key, value in DEPLOYMENT[i].iteritems():
            if type(value) is dict:
                handle_deployment_dict(deployment_elem, key, value, False)
            else:
                if value is not None:
                    deployment_elem.attrib[key] = str(value)
        i += 1

    try:
        f = open(PATH + 'vdm.xml' if PATH.endswith('/') else PATH + '/' + 'vdm.xml','w')
        f.write(tostring(main_header,encoding='UTF-8'))
        f.close()
    except Exception, err:
        print str(err)

IS_CURRENT_NODE_ADDED = False
IS_CURRENT_DATABASE_ADDED = False
IGNORETOP = { "databaseid" : True, "users" : True, "dr" : True}


def handle_deployment_dict(deployment_elem, key, value, istop):

    if istop == True:
        deployment_sub_element = deployment_elem
    else:
        deployment_sub_element = SubElement(deployment_elem, str(key))
    for key1, value1 in value.iteritems():
        if type(value1) is dict:
            handle_deployment_dict(deployment_sub_element, key1, value1, False)
        elif type(value1) is list:
            handle_deployment_list(deployment_sub_element, key1, value1)
        else:
            if isinstance(value1, bool):
                if value1 == False:
                    deployment_sub_element.attrib[key1] = "false"
                else:
                    deployment_sub_element.attrib[key1] = "true"
            else:
                if key == "property":
                    deployment_sub_element.attrib["name"] = value["name"];
                    deployment_sub_element.text = str(value1)
                else:
                    if istop == False:
                        deployment_sub_element.attrib[key1] = str(value1)
                    elif IGNORETOP[key1] != True:
                        deployment_sub_element.attrib[key1] = str(value1)



def handle_deployment_list(deployment_elem, key, value):
    for items in value:
        handle_deployment_dict(deployment_elem, key, items, False)



class ServerAPI(MethodView):
    """Class to handle requests related to server"""

    @staticmethod
    def get(server_id):
        """
        Gets the information of the server with specified server_id. If the server_id is
        not specified, then it returns the information of all the servers.
        Args:
            server_id (int): The first parameter. Defaults to None.
        Returns:
            server or list of servers.
        """
        if server_id is None:
            return jsonify({'servers': [make_public_server(x) for x in SERVERS]})
        else:
            server = [server for server in SERVERS if server.id == server_id]
            if len(server) == 0:
                abort(404)
            return jsonify({'server': make_public_server(server[0])})

    @staticmethod
    def post(database_id):
        """
        Saves the server and associate it to database with given database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            Information and the status of server if it is saved otherwise the error message.
        """
        inputs = ServerInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        server = [server for server in SERVERS if server['name'] == request.json['name']]
        if len(server) > 0:
            return make_response(jsonify({'error': 'Server name already exists'}), 404)

        server = [server for server in SERVERS if server['hostname'] == request.json['hostname']]
        if len(server) > 0:
            return make_response(jsonify({'error': 'Host name already exists'}), 404)

        if not SERVERS:
            server_id = 1
        else:
            server_id = SERVERS[-1]['id'] + 1
        server = {
            'id': server_id,
            'name': request.json['name'].strip(),
            'description': request.json.get('description', "").strip(),
            'hostname': request.json.get('hostname', "").strip(),
            'enabled': True,
            'admin-listener': request.json.get('admin-listener', "").strip(),
            'zookeeper-listener': request.json.get('zookeeper-listener', "").strip(),
            'replication-listener': request.json.get('replication-listener', "").strip(),
            'client-listener': request.json.get('client-listener', "").strip(),
            'internal-interface': request.json.get('internal-interface', "").strip(),
            'external-interface': request.json.get('external-interface', "").strip(),
            'public-interface': request.json.get('public-interface', "").strip(),
            'internal-listener': request.json.get('internal-listener', "").strip(),
            'http-listener': request.json.get('http-listener', "").strip(),
            'placement-group': request.json.get('placement-group', "").strip(),

        }
        SERVERS.append(server)

        # Add server to the current database
        current_database = [database for database in DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)
        if not request.json:
            abort(400)
        current_database[0]['members'].append(server_id)
        make_configuration_file()
        return jsonify({'server': server, 'status': 1,
                        'members': current_database[0]['members']}), 201

    @staticmethod
    def delete(server_id):
        """
        Delete the server with specified server_id.
        Args:
            server_id (int): The first parameter.
        Returns:
            True if the server is deleted otherwise the error message.
        """
        if not request.json or not 'dbId' in request.json:
            abort(400)
        database_id = request.json['dbId']
        # delete a single server
        server = [server for server in SERVERS if server['id'] == server_id]
        if len(server) == 0:
            abort(404)
        # remove the server from given database member list
        current_database = [database for database in DATABASES if database['id'] == database_id]
        current_database[0]['members'].remove(server_id)
        # Check if server is referenced by database
        for database in DATABASES:
            if database["id"] == database_id:
                continue
            if server_id in database["members"]:
                return jsonify({'success': "Server deleted from given member list only. "
                                           "Server cannot be deleted completely since"
                                           " it is referred by database."})

        SERVERS.remove(server[0])
        make_configuration_file()
        return jsonify({'result': True})

    @staticmethod
    def put(server_id):
        """
        Update the server with specified server_id.
        Args:
            server_id (int): The first parameter.
        Returns:
            Information of server with specified server_id after being updated
            otherwise the error message.
        """

        inputs = ServerInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)
        current_server = [server for server in SERVERS if server['id'] == server_id]
        if len(current_server) == 0:
            abort(404)

        current_server[0]['name'] = \
            request.json.get('name', current_server[0]['name'])
        current_server[0]['hostname'] = \
            request.json.get('hostname', current_server[0]['hostname'])
        current_server[0]['description'] = \
            request.json.get('description', current_server[0]['description'])
        current_server[0]['enabled'] = \
            request.json.get('enabled', current_server[0]['enabled'])
        current_server[0]['admin-listener'] = \
            request.json.get('admin-listener', current_server[0]['admin-listener'])
        current_server[0]['internal-listener'] = \
            request.json.get('internal-listener', current_server[0]['internal-listener'])
        current_server[0]['http-listener'] = \
            request.json.get('http-listener', current_server[0]['http-listener'])
        current_server[0]['zookeeper-listener'] = \
            request.json.get('zookeeper-listener', current_server[0]['zookeeper-listener'])
        current_server[0]['replication-listener'] = \
            request.json.get('replication-listener', current_server[0]['replication-listener'])
        current_server[0]['client-listener'] = \
            request.json.get('client-listener', current_server[0]['client-listener'])
        current_server[0]['internal-interface'] = \
            request.json.get('internal-interface', current_server[0]['internal-interface'])
        current_server[0]['external-interface'] = \
            request.json.get('external-interface', current_server[0]['external-interface'])
        current_server[0]['public-interface'] = \
            request.json.get('public-interface', current_server[0]['public-interface'])
        current_server[0]['placement-group'] = \
            request.json.get('placement-group', current_server[0]['placement-group'])
        make_configuration_file()
        return jsonify({'server': current_server[0], 'status': 1})


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
            return jsonify({'databases': [make_public_database(x) for x in DATABASES]})
        else:
            # expose a single user
            database = [database for database in DATABASES if database['id'] == database_id]
            if len(database) == 0:
                abort(404)
            return jsonify({'database': make_public_database(database[0])})

    @staticmethod
    def post():
        """
        Saves the database.
        Returns:
            Information and the status of database if it is saved otherwise the error message.
        """
        make_configuration_file()
        inputs = DatabaseInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        database = [database for database in DATABASES if database['name'] == request.json['name']]
        if len(database) != 0:
            return make_response(jsonify({'error': 'database name already exists'}), 404)

        if not DATABASES:
            database_id = 1
        else:
            database_id = DATABASES[-1]['id'] + 1
        database = {
            'id': database_id,
            'name': request.json['name'],
            'deployment': request.json.get('deployment', ""),
            'members': []
        }
        DATABASES.append(database)

        # Create new deployment
        app_root = os.path.dirname(os.path.abspath(__file__))

        with open(app_root +"/deployment.json") as json_file:
            deployment = json.load(json_file)
            deployment['databaseid'] = database_id

        DEPLOYMENT.append(deployment)
        make_configuration_file()
        return jsonify({'database': database, 'status': 1}), 201

    @staticmethod
    def put(database_id):
        """
        Updates the database with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            Information and the status of database if it is updated otherwise the error message.
        """
        inputs = DatabaseInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        current_database = [database for database in DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)

        current_database[0]['name'] = request.json.get('name', current_database[0]['name'])
        current_database[0]['deployment'] = \
            request.json.get('deployment', current_database[0]['deployment'])
        make_configuration_file()
        return jsonify({'database': current_database[0], 'status': 1})

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
        current_database = [database for database in DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)
        else:
            members = current_database[0]['members']

        for server_id in members:
            is_server_associated = False
            # Check if server is referenced by database
            for database in DATABASES:
                if database["id"] == database_id:
                    continue
                if server_id in database["members"]:
                    is_server_associated = True
            # if server is not referenced by other database then delete it
            if not is_server_associated:
                server = [server for server in SERVERS if server['id'] == server_id]
                if len(server) == 0:
                    continue
                SERVERS.remove(server[0])

        DATABASES.remove(current_database[0])

        deployment = [deployment for deployment in DEPLOYMENT if deployment['databaseid'] == database_id]

        DEPLOYMENT.remove(deployment[0])
        make_configuration_file()
        return jsonify({'result': True})


class DatabaseMemberAPI(MethodView):
    """
    Class to handle request related to database member.
    """

    @staticmethod
    def get(database_id):
        """
        Get the members of the database with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            List of member ids related to specified database.
        """
        database = [database for database in DATABASES if database['id'] == database_id]
        if len(database) == 0:
            abort(404)

        return jsonify({'members': database[0]['members']})

    @staticmethod
    def put(database_id):
        """
        Add members to the database with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            List of member ids related to specified database.
        """
        current_database = [database for database in DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)
        if not request.json:
            abort(400)

        # if 'members' not in request.json:
        for member_id in request.json['members']:
            current_server = [server for server in SERVERS if server['id'] == member_id]
            if len(current_server) == 0:
                return jsonify({'error': 'Server id %d does not exists' % member_id})

            if member_id not in current_database[0]['members']:
                current_database[0]['members'].append(member_id)
        make_configuration_file()
        return jsonify({'members': current_database[0]['members'], 'status': 1})


class deploymentAPI(MethodView):
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
        if database_id is None:
            # return a list of users
            return jsonify({'deployment': [make_public_deployment(x) for x in DEPLOYMENT]})
        else:
            deployment = [deployment for deployment in DEPLOYMENT if deployment['databaseid'] == database_id]
            return jsonify({'deployment': map_deployment_without_database_id(deployment[0])})

    @staticmethod
    def put(database_id):
        """
        Add deployment information to specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            Deployment object of added deployment.
        """
        inputs = JsonInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        if 'dr' in request.json and 'type' in request.json['dr']:
            if request.json['dr']['type'] != 'Master':
                if 'connection' in request.json['dr'] \
                        and 'source' in request.json['dr']['connection'] \
                        and 'servers' in request.json['dr']['connection']:
                    database_selected = [database for database in DATABASES if
                                         database['name'] == str(request.json['dr']['connection']['source'])]
                    if len(database_selected) == 0:
                        make_response(jsonify({'error': 'The selected database must have database enabled.'}), 404)
                    deployment_selected = [deployment1 for deployment1 in DEPLOYMENT
                                           if deployment1['databaseid'] == database_selected[0]['id']]

                    if request.json['dr']['type'] == 'Replica':
                        if len(deployment_selected) == 0:
                            make_response(jsonify({'error': 'The selected database must have database enabled.'}), 404)
                        if deployment_selected[0]['dr'] is not None and \
                                        deployment_selected[0]['dr']['enabled'] is True and \
                                        deployment_selected[0]['dr']['type'] is not None:
                            deployment_type = deployment_selected[0]['dr']['type']
                            if deployment_type != 'Master':
                                return make_response(jsonify({'error': 'The selected database '
                                                                       'must be of type Master.'}), 404)
                        else:
                            return make_response(jsonify({'error': 'The selected database '
                                                                   'must have database enabled.'}), 404)
                    if request.json['dr']['type'] == 'XDCR':
                        if len(deployment_selected) != 0 and deployment_selected[0]['dr'] is not None and \
                                        len(deployment_selected[0]['dr']) != 0 and \
                                        deployment_selected[0]['dr']['enabled'] is True and \
                                        deployment_selected[0]['dr']['type'] is not None:
                            database_selected = [database for database in DATABASES if database['id'] == database_id]
                            if len(database_selected) == 0:
                                return make_response(jsonify({'error': 'Database not found.'}))
                            if database_selected[0]['name'] != deployment_selected[0]['dr']['connection']['source']:
                                if deployment_selected[0]['dr']['type'] == 'Master' or \
                                                deployment_selected[0]['dr']['type'] == 'Replica':
                                    return make_response(jsonify({'error': 'The selected database should be '
                                                                           'of DR type "XDCR".'}), 404)
                                elif deployment_selected[0]['dr']['type'] == 'XDCR':
                                    return make_response(jsonify({'error': 'The selected database is configured '
                                                                           'to use XDCR with another database. '
                                                                           'Please use another database.'}), 404)

                else:
                    return make_response(jsonify({'error': 'Connection source '
                                                           'not defined properly.'}), 404)

        # if 'users' in request.json:
        #     if 'user' in request.json['users']:
        #         prev_username = ''
        #         for user in request.json['users']['user']:
        #             if user['name'] == prev_username:
        #                 return make_response(jsonify({'error': 'Duplicate Username'
        #                                                  , 'success': False}), 404)
        #             prev_username = user['name']

        deployment = map_deployment(request, database_id)
        make_configuration_file()
        return jsonify({'deployment': deployment, 'status': 1})


class deploymentUserAPI(MethodView):
    """Class to handle request related to deployment."""

    @staticmethod
    def get(username):
        """
        Get the deployment with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            List of deployment information with specified database.
        """
        deployment_user = [deployment_user for deployment_user in DEPLOYMENT_USERS
                           if deployment_user['name'] == username]

        return jsonify({'deployment': deployment_user})

    @staticmethod
    def put(username, database_id):
        """
        Add user information with specified username.
        Args:
            user (string): The first parameter.
        Returns:
            Deployment user object of added deployment user.
        """

        inputs = UserInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        current_user = [user for user in DEPLOYMENT_USERS if
                        user['name'] == username and user['databaseid'] == database_id]

        if len(current_user) != 0:
            return make_response(jsonify({'error': 'Duplicate Username'
                                             , 'success': False}), 404)

        deployment_user = map_deployment_users(request, username)
        make_configuration_file()
        return jsonify({'user': deployment_user, 'status': 1, 'statusstring': 'User Created'})

    @staticmethod
    def post(username, database_id):
        """
        Add user information with specified username.
        Args:
            user (string): The first parameter.
        Returns:
            Deployment user object of added deployment user.
        """

        inputs = UserInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        current_user = [user for user in DEPLOYMENT_USERS if
                        user['name'] == username and user['databaseid'] == database_id]

        current_user[0]['name'] = request.json.get('name', current_user[0]['name'])
        current_user[0]['password'] = request.json.get('password', current_user[0]['password'])
        current_user[0]['roles'] = request.json.get('roles', current_user[0]['roles'])
        current_user[0]['plaintext'] = request.json.get('plaintext', current_user[0]['plaintext'])
        make_configuration_file()
        return jsonify({'user': current_user[0], 'status': 1, 'statusstring': "User Updated"})

    @staticmethod
    def delete(username, database_id):
        current_user = [user for user in DEPLOYMENT_USERS if
                        user['name'] == username and user['databaseid'] == database_id]

        DEPLOYMENT_USERS.remove(current_user[0])
        return jsonify({'status': 1, 'statusstring': "User Deleted"})


class VdmStatus(MethodView):
    """
    Class to get VDM status for peers to check.
    """
    @staticmethod
    def get():
        return jsonify({'vdm': {"running": "true"}})

def main(runner, amodule, aport, apath):
    try:
        F_DEBUG = os.environ['DEBUG']
    except KeyError:
        F_DEBUG = 'False'

    if F_DEBUG == 'True':
        APP.config.update(DEBUG=True)

    path = os.path.dirname(amodule.__file__)
    depjson = path + "/deployment.json"
    json_data= open(depjson).read()
    deployment = json.loads(json_data)
    global PATH
    PATH = apath
    DEPLOYMENT.append(deployment)

    __host_name__ = socket.gethostname()
    __host_or_ip__ = socket.gethostbyname(__host_name__)
    SERVERS.append({'id': 1, 'name': __host_name__, 'hostname': __host_or_ip__, 'description': "",
                    'enabled': True, 'external-interface': "", 'internal-interface': "",
                    'public-interface': "", 'client-listener': "", 'internal-listener': "",
                    'admin-listener': "", 'http-listener': "", 'replication-listener': "",
                    'zookeeper-listener': "", 'placement-group': ""})
    DATABASES.append({'id': 1, 'name': "local", 'deployment': "default", "members": [1]})
    make_configuration_file()

    SERVER_VIEW = ServerAPI.as_view('server_api')
    DATABASE_VIEW = DatabaseAPI.as_view('database_api')
    DATABASE_MEMBER_VIEW = DatabaseMemberAPI.as_view('database_member_api')
    DEPLOYMENT_VIEW = deploymentAPI.as_view('deployment_api')
    DEPLOYMENT_USER_VIEW = deploymentUserAPI.as_view('deployment_user_api')
    VDM_STATUS_VIEW = VdmStatus.as_view('vdm_status_api')
    APP.add_url_rule('/api/1.0/servers/', defaults={'server_id': None},
                     view_func=SERVER_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/servers/<int:database_id>', view_func=SERVER_VIEW, methods=['POST'])
    APP.add_url_rule('/api/1.0/servers/<int:server_id>', view_func=SERVER_VIEW,
                     methods=['GET', 'PUT', 'DELETE'])

    APP.add_url_rule('/api/1.0/databases/', defaults={'database_id': None},
                     view_func=DATABASE_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>', view_func=DATABASE_VIEW,
                     methods=['GET', 'PUT', 'DELETE'])
    APP.add_url_rule('/api/1.0/databases/', view_func=DATABASE_VIEW, methods=['POST'])
    APP.add_url_rule('/api/1.0/databases/member/<int:database_id>',
                     view_func=DATABASE_MEMBER_VIEW, methods=['GET', 'PUT', 'DELETE'])

    APP.add_url_rule('/api/1.0/deployment/', defaults={'database_id': None},
                     view_func=DEPLOYMENT_VIEW, methods=['GET'])

    APP.add_url_rule('/api/1.0/deployment/<int:database_id>', view_func=DEPLOYMENT_VIEW, methods=['GET', 'PUT'])
    APP.add_url_rule('/api/1.0/deployment/users/<string:username>', view_func=DEPLOYMENT_USER_VIEW,
                     methods=['GET', 'PUT', 'POST', 'DELETE'])
    APP.add_url_rule('/api/1.0/deployment/users/<int:database_id>/<string:username>', view_func=DEPLOYMENT_USER_VIEW,
                     methods=['PUT', 'POST', 'DELETE'])
    APP.add_url_rule('/api/1.0/vdm/status',
                     view_func=VDM_STATUS_VIEW, methods=['GET'])
    APP.run(threaded=True, host='0.0.0.0', port=aport)
