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

import ast
from collections import defaultdict
from flask import Flask, render_template, jsonify, abort, make_response, request, Response
from flask.ext.cors import CORS
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
from Validation import ServerInputs, DatabaseInputs, JsonInputs, UserInputs, ConfigValidation
import DeploymentConfig
import voltdbserver
import glob
import psutil
import Log

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../../voltcli'))
from voltcli import utility
import voltdbclient
import logging
from logging.handlers import RotatingFileHandler
from flask_logging import Filter

filter_log = Filter('/api/1.0/', 'GET')


APP = Flask(__name__, template_folder="../templates", static_folder="../static")
CORS(APP)

__PATH__ = ""

__IP__ = "localhost"

__PORT__ = 8000

ALLOWED_EXTENSIONS = ['xml']


@APP.errorhandler(400)
def not_found(error):
    """
    Gives error message when any bad requests are made.
    Args:
        error (string): The first parameter.
    Returns:
        Error message.
    """
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

    deployment_user = filter(lambda t: t['databaseid'] == deployment['databaseid'], Global.DEPLOYMENT_USERS)
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

    deployment = filter(lambda t: t['databaseid'] == database_id, Global.DEPLOYMENT)

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
                    'path' in request.json['paths']['droverflow']:
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

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings']:
        if 'resourcemonitor' not in deployment[0]['systemsettings'] or deployment[0]['systemsettings'][
            'resourcemonitor'] is None:
            deployment[0]['systemsettings']['resourcemonitor'] = {}

        if 'memorylimit' in request.json['systemsettings']['resourcemonitor']:
            deployment[0]['systemsettings']['resourcemonitor']['memorylimit'] = {}
            if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings'] \
                    and 'memorylimit' in request.json['systemsettings']['resourcemonitor'] \
                    and 'size' in request.json['systemsettings']['resourcemonitor']['memorylimit']:
                if request.json['systemsettings']['resourcemonitor']['memorylimit']['size'] != '':
                    deployment[0]['systemsettings']['resourcemonitor']['memorylimit']['size'] = \
                        request.json['systemsettings']['resourcemonitor']['memorylimit']['size']
                else:
                    deployment[0]['systemsettings']['resourcemonitor']['memorylimit'] = {}

    if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings']:
        if 'resourcemonitor' not in deployment[0]['systemsettings'] or deployment[0]['systemsettings'][
            'resourcemonitor'] is None:
            deployment[0]['systemsettings']['resourcemonitor'] = {}

        if 'disklimit' in request.json['systemsettings']['resourcemonitor']:
            deployment[0]['systemsettings']['resourcemonitor']['disklimit'] = {}
            if 'feature' in request.json['systemsettings']['resourcemonitor']['disklimit']:
                deployment[0]['systemsettings']['resourcemonitor']['disklimit']['feature'] = []
                if request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                    for feature in request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                        deployment[0]['systemsettings']['resourcemonitor']['disklimit']['feature'].append(
                            {
                                'name': feature['name'],
                                'size': feature['size']
                            }
                        )
                else:
                    deployment[0]['systemsettings']['resourcemonitor']['disklimit'] = {}

    if 'systemsettings' in deployment[0] and 'resourcemonitor' in deployment[0]['systemsettings']:
        result = False
        if 'memorylimit' in deployment[0]['systemsettings']['resourcemonitor'] and \
                deployment[0]['systemsettings']['resourcemonitor']['memorylimit']:
            result = True
        if 'disklimit' in deployment[0]['systemsettings']['resourcemonitor'] and \
                deployment[0]['systemsettings']['resourcemonitor']['disklimit']:
            result = True
        if result == False:
            deployment[0]['systemsettings']['resourcemonitor'] = {}

    if 'import' in request.json:
        if 'import' not in deployment[0] or deployment[0]['import'] is None:
            deployment[0]['import'] = {}

    if 'import' in request.json and 'configuration' in request.json['import']:
        deployment[0]['import']['configuration'] = []
        i = 0
        for configuration in request.json['import']['configuration']:
            deployment[0]['import']['configuration'].append(
                {
                    'enabled': configuration['enabled'],
                    'module': configuration['module'],
                    'type': configuration['type'],
                    'format': configuration['format'],
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
    if 'export' in request.json:
        if 'export' not in deployment[0] or deployment[0]['export'] is None:
            deployment[0]['export'] = {}

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
        if 'dr' not in deployment[0] or deployment[0]['dr'] is None:
            deployment[0]['dr'] = {}

    if 'dr' in request.json and 'connection' in request.json['dr']:
        if not hasattr(deployment[0]['dr'], 'connection'):
            deployment[0]['dr']['connection'] = {}

    if 'dr' in request.json and 'connection' in request.json['dr'] and 'source' not in request.json['dr']['connection']:
        deployment[0]['dr']['connection'] = None

    if 'dr' in request.json and 'id' in request.json['dr']:
        deployment[0]['dr']['id'] = request.json['dr']['id']

    if 'dr' in request.json and 'listen' in request.json['dr']:
        deployment[0]['dr']['listen'] = request.json['dr']['listen']

    if 'dr' in request.json and request.json['dr']:
        if 'port' in request.json['dr']:
            deployment[0]['dr']['port'] = request.json['dr']['port']
        else:
            deployment[0]['dr']['port'] = None

    if 'dr' in request.json and 'connection' in request.json['dr'] \
            and 'source' in request.json['dr']['connection']:
        deployment[0]['dr']['connection']['source'] = request.json['dr']['connection']['source']

    if 'dr' in request.json and not request.json['dr']:
        deployment[0]['dr'] = {}

    if 'dr' in request.json and 'connection' in request.json['dr'] and not request.json['dr']['connection']:
        deployment[0]['dr']['connection'] = {}

    return deployment[0]


def map_deployment_users(request, user):
    if 'name' not in Global.DEPLOYMENT_USERS:
        Global.DEPLOYMENT_USERS.append(
            {
                'databaseid': request.json['databaseid'],
                'name': request.json['name'],
                'password': urllib.unquote(str(request.json['password']).encode('ascii')).decode('utf-8'),
                'roles': request.json['roles'],
                'plaintext': request.json['plaintext']
            }
        )
        deployment_user = filter(lambda t: t['name'] == user, Global.DEPLOYMENT_USERS)
    else:
        deployment_user = filter(lambda t: t['name'] == user, Global.DEPLOYMENT_USERS)

        if len(deployment_user) != 0:
            deployment_user[0]['name'] = request.json['name']
            deployment_user[0]['password'] = request.json['password']
            deployment_user[0]['plaintext'] = request.json['plaintext']
            deployment_user[0]['roles'] = request.json['roles']

    return deployment_user[0]


def get_volt_jar_dir():
    return os.path.realpath(os.path.join(Global.MODULE_PATH, '../../../..', 'voltdb'))


def get_configuration():
    deployment_json = {
        'voltdeploy': {
            'databases': Global.DATABASES,
            'members': Global.SERVERS,
            'deployments': Global.DEPLOYMENT,
            'deployment_users': Global.DEPLOYMENT_USERS
        }
    }
    return deployment_json


def write_configuration_file():
    main_header = make_configuration_file()

    try:
        path = os.path.join(Global.PATH, 'voltdeploy.xml')
        f = open(path, 'w')
        f.write(main_header)
        f.close()

    except Exception, err:
        print str(err)


def make_configuration_file():
    main_header = Element('voltdeploy')
    db_top = SubElement(main_header, 'databases')
    server_top = SubElement(main_header, 'members')
    deployment_top = SubElement(main_header, 'deployments')
    i = 0
    while i < len(Global.DATABASES):
        db_elem = SubElement(db_top, 'database')
        for key, value in Global.DATABASES[i].iteritems():
            if isinstance(value, bool):
                if value == False:
                    db_elem.attrib[key] = "false"
                else:
                    db_elem.attrib[key] = "true"
            else:
                db_elem.attrib[key] = str(value)
        i += 1

    i = 0
    while i < len(Global.SERVERS):
        server_elem = SubElement(server_top, 'member')
        for key, value in Global.SERVERS[i].iteritems():
            if isinstance(value, bool):
                if value == False:
                    server_elem.attrib[key] = "false"
                else:
                    server_elem.attrib[key] = "true"
            else:
                server_elem.attrib[key] = str(value)
        i += 1

    i = 0
    while i < len(Global.DEPLOYMENT):
        Global.DEPLOYMENT[i]['users'] = {}
        Global.DEPLOYMENT[i]['users']['user'] = []
        deployment_user = filter(lambda t: t['databaseid'] == Global.DEPLOYMENT[i]['databaseid'],
                                 Global.DEPLOYMENT_USERS)
        if len(deployment_user) == 0:
            Global.DEPLOYMENT[i]['users'] = None
        for user in deployment_user:
            Global.DEPLOYMENT[i]['users']['user'].append({
                'name': user['name'],
                'roles': user['roles'],
                'plaintext': user['plaintext'],
                'password': user['password'],
                'databaseid': user['databaseid']
            })

        deployment_elem = SubElement(deployment_top, 'deployment')
        for key, value in Global.DEPLOYMENT[i].iteritems():
            if type(value) is dict:
                DeploymentConfig.handle_deployment_dict(deployment_elem, key, value, False)
            elif type(value) is list:
                DeploymentConfig.handle_deployment_list(deployment_elem, key, value)
            else:
                if value is not None:
                    deployment_elem.attrib[key] = str(value)
        i += 1
    return tostring(main_header, encoding='UTF-8')


def sync_configuration():
    headers = {'content-type': 'application/json'}
    url = 'http://%s:%u/api/1.0/voltdeploy/configuration/' % \
          (__IP__,__PORT__)
    response = requests.post(url, headers=headers)
    return response


def convert_xml_to_json(config_path):
    with open(config_path) as f:
        xml = f.read()
    o = XML(xml)
    xml_final = json.loads(json.dumps(etree_to_dict(o)))
    if type(xml_final['voltdeploy']['members']['member']) is dict:
        member_json = get_member_from_xml(xml_final['voltdeploy']['members']['member'], 'dict')
    else:
        member_json = get_member_from_xml(xml_final['voltdeploy']['members']['member'], 'list')

    if type(xml_final['voltdeploy']['databases']['database']) is dict:
        db_json = get_db_from_xml(xml_final['voltdeploy']['databases']['database'], 'dict')
    else:
        db_json = get_db_from_xml(xml_final['voltdeploy']['databases']['database'], 'list')

    if type(xml_final['voltdeploy']['deployments']['deployment']) is dict:
        deployment_json = get_deployment_from_xml(xml_final['voltdeploy']['deployments']['deployment'], 'dict')
    else:
        deployment_json = get_deployment_from_xml(xml_final['voltdeploy']['deployments']['deployment'], 'list')
    if type(xml_final['voltdeploy']['deployments']['deployment']) is dict:
        user_json = get_users_from_xml(xml_final['voltdeploy']['deployments']['deployment'], 'dict')
    else:
        user_json = get_users_from_xml(xml_final['voltdeploy']['deployments']['deployment'], 'list')

    Global.DATABASES = db_json

    Global.SERVERS = member_json

    Global.DEPLOYMENT = deployment_json

    Global.DEPLOYMENT_USERS = user_json


def get_db_from_xml(db_xml, is_list):
    new_database = {}
    db = []
    if is_list is 'list':
        for database in db_xml:
            new_database = {}
            for field in database:
                new_database[field] = convert_db_field_required_format(database, field)
            db.append(new_database)
    else:
        for field in db_xml:
            new_database[field] = convert_db_field_required_format(db_xml, field)
        db.append(new_database)
    return db


def convert_db_field_required_format(database, field):
    if field == 'id':
        modified_field = int(database[field])
    elif field == 'members':
        modified_field = ast.literal_eval(database[field])
    else:
        modified_field = database[field]
    return modified_field


def get_member_from_xml(member_xml, is_list):
    new_member = {}
    members = []
    if is_list is 'list':
        for member in member_xml:
            new_member = {}
            for field in member:
                new_member[field] = convert_server_field_required_format(member, field)
            members.append(new_member)
    else:
        for field in member_xml:
            new_member[field] = convert_server_field_required_format(member_xml, field)
        members.append(new_member)
    return members


def convert_server_field_required_format(server, field):
    if field == 'id':
        modified_field = int(server[field])
    else:
        modified_field = server[field]
    return modified_field


def get_deployment_from_xml(deployment_xml, is_list):
    new_deployment = {}
    deployments = []
    if is_list is 'list':
        for deployment in deployment_xml:
            new_deployment = {}
            for field in deployment:
                if field == 'export':
                    if deployment[field] is not None:
                        if type(deployment[field]['configuration']) is list:
                            new_deployment[field] = get_deployment_export_field(deployment[field]['configuration'],
                                                                                'list')
                        else:
                            new_deployment[field] = get_deployment_export_field(deployment[field]['configuration'],
                                                                                'dict')
                    else:
                        new_deployment[field] = deployment[field]
                elif field == 'import':
                    if deployment[field] is not None:
                        if type(deployment[field]['configuration']) is list:
                            new_deployment[field] = get_deployment_export_field(deployment[field]['configuration'],
                                                                                'list')
                        else:
                            new_deployment[field] = get_deployment_export_field(deployment[field]['configuration'],
                                                                                'dict')
                    else:
                        new_deployment[field] = deployment[field]
                elif field == 'admin-mode':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['adminstartup'] = parse_bool_string(deployment[field]['adminstartup'])
                        new_deployment[field]['port'] = int(deployment[field]['port'])
                    except Exception, err:
                        print 'Failed to get deployment: ' % str(err)
                        print traceback.format_exc()
                elif field == 'cluster':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['hostcount'] = int(deployment[field]['hostcount'])
                        new_deployment[field]['kfactor'] = int(deployment[field]['kfactor'])
                        new_deployment[field]['sitesperhost'] = int(deployment[field]['sitesperhost'])
                        new_deployment[field]['elastic'] = str(deployment[field]['elastic'])
                        new_deployment[field]['schema'] = str(deployment[field]['schema'])
                    except Exception, err:
                        print str(err)
                        print traceback.format_exc()
                elif field == 'commandlog':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]['enabled'])
                        new_deployment[field]['synchronous'] = parse_bool_string(deployment[field]['synchronous'])
                        new_deployment[field]['logsize'] = int(deployment[field]['logsize'])
                        new_deployment[field]['frequency'] = {}
                        new_deployment[field]['frequency']['transactions'] = int(
                            deployment[field]['frequency']['transactions'])
                        new_deployment[field]['frequency']['time'] = int(deployment[field]['frequency']['time'])
                    except Exception, err:
                        print str(err)
                        print traceback.format_exc()
                elif field == 'heartbeat':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['timeout'] = int(deployment[field]['timeout'])
                    except Exception, err:
                        print str(err)
                        print traceback.format_exc()
                elif field == 'httpd':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['port'] = int(deployment[field]['port'])
                        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]['enabled'])
                        new_deployment[field]['jsonapi'] = {}
                        new_deployment[field]['jsonapi']['enabled'] = parse_bool_string(
                            deployment[field]['jsonapi']['enabled'])
                    except Exception, err:
                        print str(err)
                elif field == 'partition-detection':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]['enabled'])
                        new_deployment[field]['snapshot'] = {}
                        new_deployment[field]['snapshot']['prefix'] = deployment[field]['snapshot']['prefix']
                    except Exception, err:
                        print str(err)
                elif field == 'security':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]['enabled'])
                        new_deployment[field]['provider'] = str(deployment[field]['provider'])
                    except Exception, err:
                        print str(err)
                elif field == 'snapshot':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]['enabled'])
                        new_deployment[field]['frequency'] = str(deployment[field]['frequency'])
                        new_deployment[field]['prefix'] = str(deployment[field]['prefix'])
                        new_deployment[field]['retain'] = int(deployment[field]['retain'])
                    except Exception, err:
                        print str(err)
                elif field == 'systemsettings':
                    try:
                        new_deployment[field] = {}
                        new_deployment[field]['elastic'] = {}
                        new_deployment[field]['elastic']['duration'] = int(deployment[field]['elastic']['duration'])
                        new_deployment[field]['elastic']['throughput'] = int(deployment[field]['elastic']['throughput'])
                        new_deployment[field]['query'] = {}
                        new_deployment[field]['query']['timeout'] = int(deployment[field]['query']['timeout'])
                        new_deployment[field]['snapshot'] = {}
                        new_deployment[field]['snapshot']['priority'] = int(deployment[field]['snapshot']['priority'])
                        new_deployment[field]['temptables'] = {}
                        new_deployment[field]['temptables']['maxsize'] = int(deployment[field]['temptables']['maxsize'])
                        if 'resourcemonitor' not in deployment[field] or deployment[field]['resourcemonitor'] is None:
                            if 'resourcemonitor' in deployment[field]:
                                new_deployment[field]['resourcemonitor'] = None
                        else:
                            new_deployment[field]['resourcemonitor'] = {}
                            if 'memorylimit' in deployment[field]['resourcemonitor']:
                                new_deployment[field]['resourcemonitor']['memorylimit'] = \
                                deployment[field]['resourcemonitor']['memorylimit']

                            if 'disklimit' in deployment[field]['resourcemonitor'] and 'feature' in \
                                    deployment[field]['resourcemonitor']['disklimit']:
                                if type(deployment[field]['resourcemonitor']['disklimit']['feature']) is list:
                                    new_deployment[field]['resourcemonitor']['disklimit'] = {}
                                    new_deployment[field]['resourcemonitor']['disklimit'][
                                        'feature'] = get_deployment_properties(
                                        deployment[field]['resourcemonitor']['disklimit']['feature'], 'list')
                                else:
                                    new_deployment[field]['resourcemonitor']['disklimit'] = {}
                                    new_deployment[field]['resourcemonitor']['disklimit'][
                                        'feature'] = get_deployment_properties(
                                        deployment[field]['resourcemonitor']['disklimit']['feature'], 'dict')

                    except Exception, err:
                        print str(err)
                        print traceback.format_exc()
                elif field == 'dr':
                    try:
                        if deployment[field] is not None:
                            new_deployment[field] = {}
                            new_deployment[field]['id'] = int(deployment[field]['id'])
                            new_deployment[field]['listen'] = parse_bool_string(deployment[field]['listen'])
                            if 'port' in deployment[field]:
                                new_deployment[field]['port'] = int(deployment[field]['port'])
                            if 'connection' in deployment[field] and deployment[field][
                                'connection'] is not None and 'source' in deployment[field]['connection']:
                                new_deployment[field]['connection'] = {}
                                new_deployment[field]['connection']['source'] = str(
                                    deployment[field]['connection']['source'])

                    except Exception, err:
                        print 'dr:' + str(err)
                        print traceback.format_exc()
                elif field == 'users':
                    if deployment[field] is not None:
                        new_deployment[field] = {}
                        if type(deployment[field]['user']) is list:
                            new_deployment[field]['user'] = []
                            new_deployment[field]['user'] = get_deployment_properties(deployment[field]['user'], 'list')
                        else:
                            new_deployment[field]['user'] = []
                            new_deployment[field]['user'] = get_deployment_properties(deployment[field]['user'], 'dict')
                else:
                    new_deployment[field] = convert_deployment_field_required_format(deployment, field)

            deployments.append(new_deployment)
    else:
        for field in deployment_xml:
            if field == 'export':
                if deployment_xml[field] is not None:
                    if type(deployment_xml[field]['configuration']) is list:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'],
                                                                            'list')
                    else:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'],
                                                                            'dict')
                else:
                    new_deployment[field] = deployment_xml[field]
            elif field == 'import':
                if deployment_xml[field] is not None:
                    if type(deployment_xml[field]['configuration']) is list:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'],
                                                                            'list')
                    else:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'],
                                                                            'dict')
                else:
                    new_deployment[field] = deployment_xml[field]
            elif field == 'admin-mode':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['adminstartup'] = parse_bool_string(deployment_xml[field]['adminstartup'])
                    new_deployment[field]['port'] = int(deployment_xml[field]['port'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'cluster':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['hostcount'] = int(deployment_xml[field]['hostcount'])
                    new_deployment[field]['kfactor'] = int(deployment_xml[field]['kfactor'])
                    new_deployment[field]['sitesperhost'] = int(deployment_xml[field]['sitesperhost'])
                    new_deployment[field]['elastic'] = str(deployment_xml[field]['elastic'])
                    new_deployment[field]['schema'] = str(deployment_xml[field]['schema'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'commandlog':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                    new_deployment[field]['synchronous'] = parse_bool_string(deployment_xml[field]['synchronous'])
                    new_deployment[field]['logsize'] = int(deployment_xml[field]['logsize'])
                    new_deployment[field]['frequency'] = {}
                    new_deployment[field]['frequency']['transactions'] = int(
                        deployment_xml[field]['frequency']['transactions'])
                    new_deployment[field]['frequency']['time'] = int(deployment_xml[field]['frequency']['time'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'heartbeat':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['timeout'] = int(deployment_xml[field]['timeout'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'httpd':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['port'] = int(deployment_xml[field]['port'])
                    new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                    new_deployment[field]['jsonapi'] = {}
                    new_deployment[field]['jsonapi']['enabled'] = parse_bool_string(
                        deployment_xml[field]['jsonapi']['enabled'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'partition-detection':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                    new_deployment[field]['snapshot'] = {}
                    new_deployment[field]['snapshot']['prefix'] = deployment_xml[field]['snapshot']['prefix']
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'security':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                    new_deployment[field]['provider'] = str(deployment_xml[field]['provider'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'snapshot':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                    new_deployment[field]['frequency'] = str(deployment_xml[field]['frequency'])
                    new_deployment[field]['prefix'] = str(deployment_xml[field]['prefix'])
                    new_deployment[field]['retain'] = int(deployment_xml[field]['retain'])
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'systemsettings':
                try:
                    new_deployment[field] = {}
                    new_deployment[field]['elastic'] = {}
                    new_deployment[field]['elastic']['duration'] = int(deployment_xml[field]['elastic']['duration'])
                    new_deployment[field]['elastic']['throughput'] = int(deployment_xml[field]['elastic']['throughput'])
                    new_deployment[field]['query'] = {}
                    new_deployment[field]['query']['timeout'] = int(deployment_xml[field]['query']['timeout'])
                    new_deployment[field]['snapshot'] = {}
                    new_deployment[field]['snapshot']['priority'] = int(deployment_xml[field]['snapshot']['priority'])
                    new_deployment[field]['temptables'] = {}
                    new_deployment[field]['temptables']['maxsize'] = int(deployment_xml[field]['temptables']['maxsize'])

                    if 'resourcemonitor' not in deployment_xml[field] or deployment_xml[field][
                        'resourcemonitor'] is None:
                        if 'resourcemonitor' in deployment_xml[field]:
                            new_deployment[field]['resourcemonitor'] = None
                    else:
                        new_deployment[field]['resourcemonitor'] = {}
                        if 'memorylimit' in deployment_xml[field]['resourcemonitor']:
                            new_deployment[field]['resourcemonitor']['memorylimit'] = \
                            deployment_xml[field]['resourcemonitor']['memorylimit']

                        if 'disklimit' in deployment_xml[field]['resourcemonitor'] and 'feature' in \
                                deployment_xml[field]['resourcemonitor']['disklimit']:
                            if type(deployment_xml[field]['resourcemonitor']['disklimit']['feature']) is list:
                                new_deployment[field]['resourcemonitor']['disklimit'] = {}
                                new_deployment[field]['resourcemonitor']['disklimit'][
                                    'feature'] = get_deployment_properties(
                                    deployment_xml[field]['resourcemonitor']['disklimit']['feature'], 'list')
                            else:
                                new_deployment[field]['resourcemonitor']['disklimit'] = {}
                                new_deployment[field]['resourcemonitor']['disklimit'][
                                    'feature'] = get_deployment_properties(
                                    deployment_xml[field]['resourcemonitor']['disklimit']['feature'], 'dict')
                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'dr':
                try:
                    if deployment_xml[field] is not None:
                        new_deployment[field] = {}
                        new_deployment[field]['id'] = int(deployment_xml[field]['id'])
                        new_deployment[field]['listen'] = parse_bool_string(deployment_xml[field]['listen'])
                        if 'port' in deployment_xml[field]:
                            new_deployment[field]['port'] = int(deployment_xml[field]['port'])
                        if 'connection' in deployment_xml[field] and deployment_xml[field][
                            'connection'] is not None and 'source' in deployment_xml[field]['connection']:
                            new_deployment[field]['connection'] = {}
                            new_deployment[field]['connection']['source'] = str(
                                deployment_xml[field]['connection']['source'])

                except Exception, err:
                    print str(err)
                    print traceback.format_exc()
            elif field == 'users':
                if deployment_xml[field] is not None:
                    new_deployment[field] = {}
                    if type(deployment_xml[field]['user']) is list:
                        new_deployment[field]['user'] = []
                        new_deployment[field]['user'] = get_deployment_properties(deployment_xml[field]['user'], 'list')
                    else:
                        new_deployment[field]['user'] = []
                        new_deployment[field]['user'] = get_deployment_properties(deployment_xml[field]['user'], 'dict')
            else:
                new_deployment[field] = convert_deployment_field_required_format(deployment_xml, field)

        deployments.append(new_deployment)
    return deployments


def get_deployment_for_upload(deployment_xml, is_list):
    new_deployment = {}
    deployments = []
    for field in deployment_xml:
        if field == 'export':
            try:
                if deployment_xml[field] is not None:
                    if type(deployment_xml[field]['configuration']) is list:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'], 'list')
                    else:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'], 'dict')
                else:
                    new_deployment[field] = deployment_xml[field]
            except Exception, exp:
                return {'error': 'Export: ' + str(exp)}
        elif field == 'import':
            try:
                if deployment_xml[field] is not None:
                    if type(deployment_xml[field]['configuration']) is list:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'], 'list')
                    else:
                        new_deployment[field] = get_deployment_export_field(deployment_xml[field]['configuration'], 'dict')
                else:
                    new_deployment[field] = deployment_xml[field]
            except Exception, exp:
                return {'error': 'Import: ' + str(exp)}
        elif field == 'admin-mode':
            try:
                new_deployment[field] = {}
                new_deployment[field]['adminstartup'] = parse_bool_string(deployment_xml[field]['adminstartup'])
                new_deployment[field]['port'] = int(deployment_xml[field]['port'])
            except Exception, err:
                return {'Error': 'Admin-mode: ' + str(err)}
        elif field == 'cluster':
            try:
                new_deployment[field] = {}
                new_deployment[field]['hostcount'] = int(deployment_xml[field]['hostcount'])
                new_deployment[field]['kfactor'] = int(deployment_xml[field]['kfactor'])
                new_deployment[field]['sitesperhost'] = int(deployment_xml[field]['sitesperhost'])
                new_deployment[field]['elastic'] = str(deployment_xml[field]['elastic'])
                new_deployment[field]['schema'] = str(deployment_xml[field]['schema'])
            except Exception, err:
                return {'error': 'Cluster: ' + str(err)}
        elif field == 'commandlog':
            try:
                new_deployment[field] = {}
                new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                new_deployment[field]['synchronous'] = parse_bool_string(deployment_xml[field]['synchronous'])
                new_deployment[field]['logsize'] = int(deployment_xml[field]['logsize'])
                new_deployment[field]['frequency'] = {}
                new_deployment[field]['frequency']['transactions'] = int(deployment_xml[field]['frequency']['transactions'])
                new_deployment[field]['frequency']['time'] = int(deployment_xml[field]['frequency']['time'])
            except Exception, err:
                return {'error': 'Commandlog: ' + str(err)}
        elif field == 'heartbeat':
            try:
                new_deployment[field] = {}
                new_deployment[field]['timeout'] = int(deployment_xml[field]['timeout'])
            except Exception, err:
                return {'error': 'Heartbeat: ' + str(err)}
        elif field == 'httpd':
            try:
                new_deployment[field] = {}
                new_deployment[field]['port'] = int(deployment_xml[field]['port'])
                new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                new_deployment[field]['jsonapi'] = {}
                new_deployment[field]['jsonapi']['enabled'] = parse_bool_string(deployment_xml[field]['jsonapi']['enabled'])
            except Exception, err:
                return {'error': 'HTTPD: ' + str(err)}
        elif field == 'partition-detection':
            try:
                new_deployment[field] = {}
                new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                new_deployment[field]['snapshot'] = {}
                new_deployment[field]['snapshot']['prefix'] = deployment_xml[field]['snapshot']['prefix']
            except Exception, err:
                return {'error': 'Partition-Detection: ' + str(err)}
        elif field == 'security':
            try:
                new_deployment[field] = {}
                new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                new_deployment[field]['provider'] = str(deployment_xml[field]['provider'])
            except Exception, err:
                return {'error': 'Security: ' + str(err)}
        elif field == 'snapshot':
            try:
                new_deployment[field] = {}
                new_deployment[field]['enabled'] = parse_bool_string(deployment_xml[field]['enabled'])
                new_deployment[field]['frequency'] = str(deployment_xml[field]['frequency'])
                new_deployment[field]['prefix'] = str(deployment_xml[field]['prefix'])
                new_deployment[field]['retain'] = int(deployment_xml[field]['retain'])
            except Exception, err:
                return {'error': 'Snapshot: ' + str(err)}
        elif field == 'systemsettings':
            try:
                new_deployment[field] = {}
                new_deployment[field]['elastic'] = {}
                new_deployment[field]['elastic']['duration'] = int(deployment_xml[field]['elastic']['duration'])
                new_deployment[field]['elastic']['throughput'] = int(deployment_xml[field]['elastic']['throughput'])
                new_deployment[field]['query'] = {}
                new_deployment[field]['query']['timeout'] = int(deployment_xml[field]['query']['timeout'])
                new_deployment[field]['snapshot'] = {}
                new_deployment[field]['snapshot']['priority'] = int(deployment_xml[field]['snapshot']['priority'])
                new_deployment[field]['temptables'] = {}
                new_deployment[field]['temptables']['maxsize'] = int(deployment_xml[field]['temptables']['maxsize'])

                if 'resourcemonitor' not in deployment_xml[field] or deployment_xml[field]['resourcemonitor'] is None:
                    if 'resourcemonitor'  in deployment_xml[field]:
                        new_deployment[field]['resourcemonitor'] = None
                else:
                    new_deployment[field]['resourcemonitor'] = {}
                    if 'memorylimit' in deployment_xml[field]['resourcemonitor']:
                        new_deployment[field]['resourcemonitor']['memorylimit'] = deployment_xml[field]['resourcemonitor']['memorylimit']

                    if 'disklimit' in deployment_xml[field]['resourcemonitor'] and 'feature' in deployment_xml[field]['resourcemonitor']['disklimit']:
                        if type(deployment_xml[field]['resourcemonitor']['disklimit']['feature']) is list:
                            new_deployment[field]['resourcemonitor']['disklimit'] = {}
                            new_deployment[field]['resourcemonitor']['disklimit']['feature'] = get_deployment_properties(deployment_xml[field]['resourcemonitor']['disklimit']['feature'], 'list')
                        else:
                            new_deployment[field]['resourcemonitor']['disklimit'] = {}
                            new_deployment[field]['resourcemonitor']['disklimit']['feature'] = get_deployment_properties(deployment_xml[field]['resourcemonitor']['disklimit']['feature'], 'dict')
            except Exception, err:
                return {'error': 'SystemSettings: ' + str(err)}
        elif field == 'dr':
            try:
                if deployment_xml[field] is not None:
                    new_deployment[field] = {}
                    new_deployment[field]['id'] = int(deployment_xml[field]['id'])
                    new_deployment[field]['listen'] = parse_bool_string(deployment_xml[field]['listen'])
                    if 'port' in deployment_xml[field]:
                        new_deployment[field]['port'] = int(deployment_xml[field]['port'])
                    if 'connection' in deployment_xml[field] and deployment_xml[field]['connection'] is not None and 'source' in deployment_xml[field]['connection']:
                        new_deployment[field]['connection'] = {}
                        new_deployment[field]['connection']['source'] = str(deployment_xml[field]['connection']['source'])

            except Exception, err:
                return {'error': 'DR: ' + str(err)}
        elif field == 'users':
            try:
                if deployment_xml[field] is not None:
                    new_deployment[field] = {}
                    if type(deployment_xml[field]['user']) is list:
                        new_deployment[field]['user'] = []
                        new_deployment[field]['user'] = get_deployment_properties(deployment_xml[field]['user'], 'list')
                    else:
                        new_deployment[field]['user'] = []
                        new_deployment[field]['user'] = get_deployment_properties(deployment_xml[field]['user'], 'dict')
            except Exception, err:
                return {'error': 'Users: ' + str(err)}
        else:
            new_deployment[field] = convert_deployment_field_required_format(deployment_xml, field)

    deployments.append(new_deployment)
    return deployments


def get_deployment_export_field(export_xml, is_list):
    new_export = {}
    exports = []
    if is_list is 'list':
        for export in export_xml:
            new_export = {}
            for field in export:
                if field == 'property':
                    if type(export['property']) is list:
                        new_export['property'] = get_deployment_properties(export['property'], 'list')
                    else:
                        new_export['property'] = get_deployment_properties(export['property'], 'dict')
                elif field == 'enabled':
                    new_export[field] = parse_bool_string(export[field])
                else:
                    new_export[field] = export[field]
            exports.append(new_export)
    else:
        for field in export_xml:
            if field == 'property':
                if type(export_xml['property']) is list:
                    new_export['property'] = get_deployment_properties(export_xml['property'], 'list')
                else:
                    new_export['property'] = get_deployment_properties(export_xml['property'], 'dict')
            elif field == 'enabled':
                new_export[field] = parse_bool_string(export_xml[field])
            else:
                new_export[field] = export_xml[field]
        exports.append(new_export)
    return {'configuration': exports}


def get_deployment_import_field(export_xml, is_list):
    new_export = {}
    exports = []
    if is_list is 'list':
        for export in export_xml:
            new_export = {}
            for field in export:
                new_export[field] = export[field]
            exports.append(new_export)
    else:
        for field in export_xml:
            if field == 'property':
                if type(export_xml['property']) is list:
                    new_export['property'] = get_deployment_properties(export_xml['property'], 'list')
                else:
                    new_export['property'] = get_deployment_properties(export_xml['property'], 'dict')
            else:
                new_export[field] = export_xml[field]
        exports.append(new_export)
    return {'configuration': exports}


def get_deployment_properties(export_xml, is_list):
    new_export = {}
    exports = []
    if is_list is 'list':
        for export in export_xml:
            new_export = {}
            for field in export:
                if field == 'plaintext':
                    new_export[field] = parse_bool_string(export[field])
                else:
                    new_export[field] = export[field]
            exports.append(new_export)
    else:
        for field in export_xml:
            if field == 'plaintext':
                new_export[field] = parse_bool_string(export_xml[field])
            else:
                new_export[field] = export_xml[field]
        exports.append(new_export)
    return exports


def get_users_from_xml(deployment_xml, is_list):
    users = []
    if is_list is 'list':
        for deployment in deployment_xml:
            for field in deployment:
                if field == 'users':
                    if deployment[field] is not None:
                        if type(deployment[field]['user']) is list:
                            for user in deployment[field]['user']:
                                users.append(convert_user_required_format(user))
                        else:
                            users.append(convert_user_required_format(deployment[field]['user']))
    else:
        for field in deployment_xml:
            if field == 'users':
                if deployment_xml[field] is not None:
                    if type(deployment_xml[field]['user']) is list:
                        for user in deployment_xml[field]['user']:
                            users.append(convert_user_required_format(user))
                    else:
                        users.append(convert_user_required_format(deployment_xml[field]['user']))
    return users


def convert_user_required_format(user):
    for field in user:
        if field == 'databaseid':
            user[field] = int(user[field])
    return user


def convert_deployment_field_required_format(deployment, field):
    if field == 'databaseid':
        modified_field = int(deployment[field])
    else:
        modified_field = deployment[field]
    return modified_field


def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.iteritems():
                dd[k].append(v)
        # d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.iteritems()}}
        aa = {}
        for k, v in dd.iteritems():
            aa[k] = v[0] if len(v) == 1 else v
        d = {t.tag: aa}
    if t.attrib:
        d[t.tag].update((k, v) for k, v in t.attrib.iteritems())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]['value'] = text
        else:
            d[t.tag] = text
    return d


def replace_last(source_string, replace_what, replace_with):
    head, sep, tail = source_string.rpartition(replace_what)
    return head + replace_with + tail


def check_size_value(value, key):
    per_idx = value.find('%')
    if per_idx != -1:
        try:
            str_value = replace_last(value, '%', '')
            int_value = int(str_value)
            if int_value < 0 or int_value > 100:
                return jsonify({'error': key + ' percent value must be between 0 and 100.'})
            return jsonify({'status':'success'})
        except Exception, exp:
            return jsonify({'error': str(exp)})
    else:
        try:
            int_value = int(value)
            if int_value < 0 or int_value > 2147483647:
                return jsonify({'error': key + ' value must be between 0 and 2147483647.'})
            return jsonify({'status':'success'})
        except Exception, exp:
            return jsonify({'error': str(exp)})


def parse_bool_string(bool_string):
    return bool_string.upper() == 'TRUE'


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

    SERVERS = []
    DATABASES = []
    DEPLOYMENT = []
    DEPLOYMENT_USERS = []
    PATH = ''
    MODULE_PATH = ''


class ServerAPI(MethodView):
    """Class to handle requests related to server"""

    @staticmethod
    def get(database_id, server_id = None):
        """
        Get the members of the database with specified database_id.
        Args:
            database_id (int): The first parameter.
        Returns:
            List of member ids related to specified database.
        """
        if server_id is None:
            servers = []
            database = [database for database in Global.DATABASES if database['id'] == database_id]
            if len(database) == 0:
                return make_response(jsonify( { 'statusstring': 'No database found for id: %u' % database_id } ), 404)
            else:
                members = database[0]['members']

            for servers_id in members:
                server = [server for server in Global.SERVERS if server['id'] == servers_id]
                if not server:
                    return make_response(jsonify( { 'statusstring': 'Server details not found for id: %u' % server_id } ), 404)
                servers.append(server[0])

            return jsonify({'members': servers})
        else:
            database = [database for database in Global.DATABASES if database['id'] == database_id]
            if len(database) == 0:
                return make_response(jsonify( { 'statusstring': 'No database found for id: %u' % database_id } ), 404)
            else:
                members = database[0]['members']
            if server_id in members:
                server = [server for server in Global.SERVERS if server['id'] == server_id]
                if not server:
                    abort(404)
                return jsonify({'server': make_public_server(server[0])})
            else:
                return jsonify({'statusstring': 'Given server with id %u doesn\'t belong to database with id %u.' %(server_id,database_id)})

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

        if not Global.SERVERS:
            server_id = 1
        else:
            server_id = Global.SERVERS[-1]['id'] + 1
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
        Global.SERVERS.append(server)

        # Add server to the current database
        current_database = [database for database in Global.DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)
        if not request.json:
            abort(400)
        current_database[0]['members'].append(server_id)

        sync_configuration()
        write_configuration_file()
        return jsonify({'server': server, 'status': 1,
                        'members': current_database[0]['members']}), 201

    @staticmethod
    def delete(database_id, server_id):
        """
        Delete the server with specified server_id.
        Args:
            server_id (int): The first parameter.
        Returns:
            True if the server is deleted otherwise the error message.
        """
        database = [database for database in Global.DATABASES if database['id'] == database_id]
        if len(database) == 0:
            return make_response(jsonify( { 'statusstring': 'No database found for id: %u' % database_id } ), 404)
        else:
            members = database[0]['members']
        if server_id in members:
            # delete a single server
            server = [server for server in Global.SERVERS if server['id'] == server_id]
            if len(server) == 0:
                return make_response(jsonify( { 'statusstring': 'No server found for id: %u in database %u' % (server_id, database_id) } ), 404)
            # remove the server from given database member list
            current_database = [database for database in Global.DATABASES if database['id'] == database_id]
            current_database[0]['members'].remove(server_id)

            Global.SERVERS.remove(server[0])
            sync_configuration()
            write_configuration_file()
            return jsonify({'result': True})
        else:
            return make_response(jsonify( { 'statusstring': 'No server found for id: %u in database %u' % (server_id, database_id) } ), 404)

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

        database = [database for database in Global.DATABASES if database['id'] == database_id]
        if len(database) == 0:
            return make_response(jsonify( { 'statusstring': 'No database found for id: %u' % database_id } ), 404)
        else:
            members = database[0]['members']
        if server_id in members:
            inputs = ServerInputs(request)
            if not inputs.validate():
                return jsonify(success=False, errors=inputs.errors)
            current_server = [server for server in Global.SERVERS if server['id'] == server_id]
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
            sync_configuration()
            write_configuration_file()
            return jsonify({'server': current_server[0], 'status': 1})
        else:
            return jsonify({'statusstring': 'Given server with id %u doesn\'t belong to database with id %u.' %(server_id,database_id) })


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
            return jsonify({'databases': [make_public_database(x) for x in Global.DATABASES]})
        else:
            # expose a single user
            database = [database for database in Global.DATABASES if database['id'] == database_id]
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
        sync_configuration()

        write_configuration_file()
        inputs = DatabaseInputs(request)
        if not inputs.validate():
            return jsonify(success=False, errors=inputs.errors)

        database = [database for database in Global.DATABASES if database['name'] == request.json['name']]
        if len(database) != 0:
            return make_response(jsonify({'error': 'database name already exists'}), 404)

        if not Global.DATABASES:
            database_id = 1
        else:
            database_id = Global.DATABASES[-1]['id'] + 1
        database = {
            'id': database_id,
            'name': request.json['name'],
            'members': []
        }
        Global.DATABASES.append(database)

        # Create new deployment
        app_root = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(app_root, "deployment.json")) as json_file:
            deployment = json.load(json_file)
            deployment['databaseid'] = database_id
            is_pro_version(deployment)
        Global.DEPLOYMENT.append(deployment)

        sync_configuration()

        write_configuration_file()
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

        current_database = [database for database in Global.DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)

        current_database[0]['name'] = request.json.get('name', current_database[0]['name'])
        sync_configuration()
        write_configuration_file()
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
        current_database = [database for database in Global.DATABASES if database['id'] == database_id]
        if len(current_database) == 0:
            abort(404)
        else:
            members = current_database[0]['members']

        for server_id in members:
            is_server_associated = False
            # Check if server is referenced by database
            for database in Global.DATABASES:
                if database["id"] == database_id:
                    continue
                if server_id in database["members"]:
                    is_server_associated = True
            # if server is not referenced by other database then delete it
            if not is_server_associated:
                server = [server for server in Global.SERVERS if server['id'] == server_id]
                if len(server) == 0:
                    continue
                Global.SERVERS.remove(server[0])

        Global.DATABASES.remove(current_database[0])

        deployment = [deployment for deployment in Global.DEPLOYMENT if deployment['databaseid'] == database_id]

        Global.DEPLOYMENT.remove(deployment[0])
        sync_configuration()
        write_configuration_file()
        return jsonify({'result': True})


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
            return jsonify({'deployment': [make_public_deployment(x) for x in Global.DEPLOYMENT]})
        else:
            deployment = [deployment for deployment in Global.DEPLOYMENT if deployment['databaseid'] == database_id]
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

        if 'systemsettings' in request.json and 'resourcemonitor' in request.json['systemsettings']:
            if 'memorylimit' in request.json['systemsettings']['resourcemonitor'] and \
                            'size' in request.json['systemsettings']['resourcemonitor']['memorylimit']:
                size = str(request.json['systemsettings']['resourcemonitor']['memorylimit']['size'])
                response = json.loads(check_size_value(size, 'memorylimit').data)
                if 'error' in response:
                    return jsonify({'error': response['error']})
            disk_limit_arr = []
            if 'disklimit' in request.json['systemsettings']['resourcemonitor'] and \
                            'feature' in request.json['systemsettings']['resourcemonitor']['disklimit']:
                for feature in request.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                    size = feature['size']
                    if feature['name'] in disk_limit_arr:
                        return jsonify({'error': 'Duplicate items are not allowed.'})
                    disk_limit_arr.append(feature['name'])
                    response = json.loads(check_size_value(size, 'disklimit').data)
                    if 'error' in response:
                        return jsonify({'error': response['error']})

        # if 'users' in request.json:
        #     if 'user' in request.json['users']:
        #         prev_username = ''
        #         for user in request.json['users']['user']:
        #             if user['name'] == prev_username:
        #                 return make_response(jsonify({'error': 'Duplicate Username'
        #                                                  , 'success': False}), 404)
        #             prev_username = user['name']

        deployment = map_deployment(request, database_id)
        sync_configuration()
        write_configuration_file()
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
        deployment_user = [deployment_user for deployment_user in Global.DEPLOYMENT_USERS
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

        current_user = [user for user in Global.DEPLOYMENT_USERS if
                        user['name'] == username and user['databaseid'] == database_id]

        if len(current_user) != 0:
            return make_response(jsonify({'error': 'Duplicate Username'
                                             , 'success': False}), 404)

        deployment_user = map_deployment_users(request, username)

        if Global.DEPLOYMENT[0]['users'] is None:
            Global.DEPLOYMENT[0]['users'] = {}
            Global.DEPLOYMENT[0]['users']['user'] = []

        Global.DEPLOYMENT[0]['users']['user'].append({
            'name': deployment_user['name'],
            'roles': deployment_user['roles'],
            'plaintext': deployment_user['plaintext']
        })

        sync_configuration()
        write_configuration_file()
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

        current_user = [user for user in Global.DEPLOYMENT_USERS if
                        user['name'] == username and user['databaseid'] == database_id]
        current_user[0]['name'] = request.json.get('name', current_user[0]['name'])
        current_user[0]['password'] = urllib.unquote(
            str(request.json.get('password', current_user[0]['password'])).encode('ascii')).decode('utf-8')
        current_user[0]['roles'] = request.json.get('roles', current_user[0]['roles'])
        current_user[0]['plaintext'] = request.json.get('plaintext', current_user[0]['plaintext'])
        sync_configuration()
        write_configuration_file()
        return jsonify({'user': current_user[0], 'status': 1, 'statusstring': "User Updated"})

    @staticmethod
    def delete(username, database_id):
        current_user = [user for user in Global.DEPLOYMENT_USERS if
                        user['name'] == username and user['databaseid'] == database_id]

        Global.DEPLOYMENT_USERS.remove(current_user[0])
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
            database = voltdbserver.VoltDatabase(database_id)
            response = database.start_database()
            return response
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'statusstring': str(err)}),
                                 200)


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
            database = voltdbserver.VoltDatabase(database_id)
            return database.start_database(True)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'statusstring': str(err)}),
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
            return server.kill_database(database_id)

        else:
            try:
                server = voltdbserver.VoltDatabase(database_id)
                response = server.stop_database()
                # Don't use the response in the json we send back
                # because voltadmin shutdown gives 'Connection broken' output
                return response
            except Exception, err:
                print traceback.format_exc()
                return make_response(jsonify({'statusstring': str(err)}),
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

        if 'force' in request.args:
            is_force = request.args.get('force').lower()
        else:
            is_force = "false"

        if is_force == "true":
            try:
                server = voltdbserver.VoltDatabase(database_id)
                response = server.kill_server(server_id)
                return response
            except Exception, err:
                print traceback.format_exc()
                return make_response(jsonify({'statusstring': str(err)}),
                                     500)
        else:
            try:
                server = voltdbserver.VoltDatabase(database_id)
                response = server.stop_server(server_id)
                return response
            except Exception, err:
                print traceback.format_exc()
                return make_response(jsonify({'statusstring': str(err)}),
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
            server = voltdbserver.VoltDatabase(database_id)
            return server.start_server(server_id)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'statusstring': str(err)}),
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
            if 'id' in request.args:
                sid = int(request.args.get('id'))
            server = voltdbserver.VoltDatabase(database_id)
            return server.check_and_start_local_server(sid)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'statusstring': str(err)}),
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
            server = voltdbserver.VoltDatabase(database_id)
            return server.check_and_start_local_server(True)
        except Exception, err:
            print traceback.format_exc()
            return make_response(jsonify({'statusstring': str(err)}),
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
            servers = result['voltdeploy']['members']
            deployments = result['voltdeploy']['deployments']
            deployment_users = result['voltdeploy']['deployment_users']

        except Exception, errs:
            print traceback.format_exc()
            return jsonify({'status': 'success', 'error': str(errs)})

        Global.DATABASES = databases
        Global.SERVERS = servers
        Global.DEPLOYMENT = deployments
        Global.DEPLOYMENT_USERS = deployment_users

        return jsonify({'status': 'success'})


class VdmConfiguration(MethodView):
    """
    Class related to the vdm configuration
    """

    @staticmethod
    def get():
        return jsonify(get_configuration())

    @staticmethod
    def post():

        result = get_configuration()

        for member in result['voltdeploy']['members']:
            try:
                headers = {'content-type': 'application/json'}
                url = 'http://%s:%u/api/1.0/voltdeploy/sync_configuration/' % (member['hostname'], __PORT__)
                data = result
                response = requests.post(url, data=json.dumps(data), headers=headers)
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
        deployment_content = DeploymentConfig.DeploymentConfiguration.get_database_deployment(database_id)
        return Response(deployment_content, mimetype='text/xml')

    @staticmethod
    def put(database_id):
        file = request.files['file']
        if file and allowed_file(file.filename):
            try:
                content = file.read()
                o = XML(content)
                xml_final = json.loads(json.dumps(etree_to_dict(o)))
                if 'deployment' not in xml_final:
                    return jsonify({'status': 'failure', 'error': 'Invalid file content.'})
                else:
                    if type(xml_final['deployment']) is dict:
                        deployment_data = get_deployment_for_upload(xml_final['deployment'], 'dict')
                        if type(deployment_data) is dict:
                            if 'error' in deployment_data:
                                return jsonify({'error': deployment_data['error']})
                        else:
                            deployment_json = deployment_data[0]
                        req = DictClass()
                        req.json = {}
                        req.json = deployment_json
                        inputs = JsonInputs(req)
                        if not inputs.validate():
                            return jsonify(success=False, errors=inputs.errors)
                        if 'systemsettings' in req.json and 'resourcemonitor' in req.json['systemsettings']:
                            if 'memorylimit' in req.json['systemsettings']['resourcemonitor'] and \
                                            'size' in req.json['systemsettings']['resourcemonitor']['memorylimit']:
                                size = str(req.json['systemsettings']['resourcemonitor']['memorylimit']['size'])
                                response = json.loads(check_size_value(size, 'memorylimit').data)
                                if 'error' in response:
                                    return jsonify({'error': response['error']})
                            disk_limit_arr = []
                            if 'disklimit' in req.json['systemsettings']['resourcemonitor'] and \
                                            'feature' in req.json['systemsettings']['resourcemonitor']['disklimit']:
                                for feature in req.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                                    size = feature['size']
                                    if feature['name'] in disk_limit_arr:
                                        return jsonify({'error': 'Duplicate items are not allowed.'})
                                    disk_limit_arr.append(feature['name'])
                                    response = json.loads(check_size_value(size, 'disklimit').data)
                                    if 'error' in response:
                                        return jsonify({'error': response['error']})
                        if 'snapshot' in req.json and 'frequency' in req.json['snapshot']:
                            frequency_unit = ['h', 'm', 's']
                            frequency = str(req.json['snapshot']['frequency'])
                            last_char =  frequency[len(frequency)-1]
                            if last_char not in frequency_unit:
                                return jsonify({'error': 'Snapshot: Invalid frequency value.'})
                            frequency = frequency[:-1]
                            try:
                                int_frequency = int(frequency)
                            except Exception, exp:
                                return jsonify({'error': 'Snapshot: ' + str(exp)})

                        map_deployment(req, database_id)
                        Global.DEPLOYMENT_USERS = []
                        if 'users' in req.json and 'user' in req.json['users']:
                            for user in req.json['users']['user']:
                                Global.DEPLOYMENT_USERS.append(
                                    {
                                        'name': user['name'],
                                        'roles': user['roles'],
                                        'password': user['password'],
                                        'plaintext': user['plaintext'],
                                        'databaseid': database_id
                                    }
                                )
                        sync_configuration()
                        write_configuration_file()
                        return jsonify({'status': 'success'})
                    else:
                        return jsonify({'status': 'failure', 'error': 'Invalid file content.'})

            except Exception as err:
                return jsonify({'status': 'failure', 'error': 'Invalid file content.'})
        else:
            return jsonify({'status': 'failure', 'error': 'Invalid file type.'})


class VdmAPI(MethodView):
    """
    Class to return vdm.xml file
    """

    @staticmethod
    def get():
        vdm_content = make_configuration_file()
        return Response(vdm_content, mimetype='text/xml')


class StatusDatabaseAPI(MethodView):
    """Class to return status of database and its servers."""

    @staticmethod
    def get(database_id):
        serverDetails = []
        status = []

        database = [database for database in Global.DATABASES if database['id'] == database_id]
        has_stalled = False
        has_stopped = False
        has_run = False
        if not database:
            return make_response(jsonify({'error': 'Not found'}), 404)
        else:
            if len(database[0]['members']) == 0:
                return jsonify({'status':'errorNoMembers'})
            for server_id in database[0]['members']:
                server = [server for server in Global.SERVERS if server['id'] == server_id]
                url = ('http://%s:%u/api/1.0/databases/%u/servers/%u/status/') % \
                      (server[0]['hostname'], __PORT__, database_id, server[0]['id'])
                try:
                    response = requests.get(url)
                except Exception, err:
                    return jsonify({'status': 'error', 'errorDetails': err, 'hostname': server[0]['hostname']})

                if response.json()['status'] == "stalled":
                    has_stalled = True
                elif response.json()['status'] == "running":
                    has_run = True
                elif response.json()['status'] == "stopped":
                    has_stopped = True
                serverDetails.append({server[0]['hostname']: response.json()})

            # if has_stalled:
            #     status.append({'status': 'stalled'})
            # elif has_run == True and has_stopped:
            #     status.append({'status': 'stalled'})
            # elif not has_stalled and not has_stopped and has_run:
            #     status.append({'status': 'running'})
            # elif has_stopped and not has_stalled and not has_run:
            #     status.append({'status': 'stopped'})

            if has_run == True:
                status.append({'status': 'running'})
            elif has_stalled == True and not has_run:
                status.append({'status': 'stalled'})
            elif has_stopped == True and not has_run and not has_stalled:
                status.append({'status': 'stopped'})

            isFreshStart = voltdbserver.check_snapshot_folder(database_id)

            return jsonify({'status':status, 'serverDetails': serverDetails, 'isFreshStart': isFreshStart})


class StatusDatabaseServerAPI(MethodView):
    """Class to return status of servers"""

    @staticmethod
    def get(database_id, server_id):
        database = [database for database in Global.DATABASES if database['id'] == database_id]
        if not database:
            return make_response(jsonify({'error': 'Not found'}), 404)
        else:
            server = [server for server in Global.SERVERS if server['id'] == server_id]
            if len(database[0]['members']) == 0:
                return jsonify({'error':'errorNoMembers'})
            if not server:
                return make_response(jsonify({'error': 'Not found'}), 404)
            elif server_id not in database[0]['members']:
                return make_response(jsonify({'error': 'Not found'}), 404)
            else:

                try:
                    client = voltdbclient.FastSerializer(str(server[0]['hostname']), 21212)
                    proc = voltdbclient.VoltProcedure(client, "@Ping")
                    response = proc.call()
                    return jsonify({'status': "running"})
                except:
                    voltProcess = voltdbserver.VoltDatabase(database_id)
                    error = ''
                    try:
                        error = Log.get_error_log_details()
                    except:
                        pass

                    if voltProcess.Get_Voltdb_Process().isProcessRunning:
                        return jsonify({'status': "stalled", "details": error})
                    else:
                        return jsonify({'status': "stopped", "details": error})



def main(runner, amodule, config_dir, server):
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
    Global.PATH = config_dir
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
        convert_xml_to_json(config_path)
    else:
        is_pro_version(deployment)

        Global.DEPLOYMENT.append(deployment)

        Global.SERVERS.append({'id': 1, 'name': __host_name__, 'hostname': __host_or_ip__, 'description': "",
                               'enabled': True, 'external-interface': "", 'internal-interface': "",
                               'public-interface': "", 'client-listener': "", 'internal-listener': "",
                               'admin-listener': "", 'http-listener': "", 'replication-listener': "",
                               'zookeeper-listener': "", 'placement-group': ""})
        Global.DATABASES.append({'id': 1, 'name': "Database", "members": [1]})

    write_configuration_file()

    SERVER_VIEW = ServerAPI.as_view('server_api')
    DATABASE_VIEW = DatabaseAPI.as_view('database_api')


    START_LOCAL_SERVER_VIEW = StartLocalServerAPI.as_view('start_local_server_api')
    RECOVER_DATABASE_SERVER_VIEW = RecoverServerAPI.as_view('recover_server_api')
    STOP_DATABASE_SERVER_VIEW = StopServerAPI.as_view('stop_server_api')
    START_DATABASE_VIEW = StartDatabaseAPI.as_view('start_database_api')
    START_DATABASE_SERVER_VIEW = StartServerAPI.as_view('start_server_api')
    STOP_DATABASE_VIEW = StopDatabaseAPI.as_view('stop_database_api')
    RECOVER_DATABASE_VIEW = RecoverDatabaseAPI.as_view('recover_database_api')
    DEPLOYMENT_VIEW = deploymentAPI.as_view('deployment_api')
    DEPLOYMENT_USER_VIEW = deploymentUserAPI.as_view('deployment_user_api')
    VDM_STATUS_VIEW = VdmStatus.as_view('vdm_status_api')
    VDM_CONFIGURATION_VIEW = VdmConfiguration.as_view('vdm_configuration_api')
    SYNC_VDM_CONFIGURATION_VIEW = SyncVdmConfiguration.as_view('sync_vdm_configuration_api')
    DATABASE_DEPLOYMENT_VIEW = DatabaseDeploymentAPI.as_view('database_deployment_api')
    STATUS_DATABASE_VIEW = StatusDatabaseAPI.as_view('status_database_api')
    STATUS_DATABASE_SERVER_VIEW = StatusDatabaseServerAPI.as_view('status_database_server_view')
    VDM_VIEW = VdmAPI.as_view('vdm_api')

    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/', view_func=SERVER_VIEW, methods=['GET', 'POST'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/', view_func=SERVER_VIEW,
                     methods=['GET', 'PUT', 'DELETE'])
    APP.add_url_rule('/api/1.0/databases/', defaults={'database_id': None},
                     view_func=DATABASE_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>', view_func=DATABASE_VIEW,
                     methods=['GET', 'PUT', 'DELETE'])
    APP.add_url_rule('/api/1.0/databases/', view_func=DATABASE_VIEW, methods=['POST'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/start',
                     view_func=START_DATABASE_SERVER_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/stop',
                     view_func=STOP_DATABASE_SERVER_VIEW, methods=['PUT'])

    APP.add_url_rule('/api/1.0/databases/<int:database_id>/start',
                     view_func=START_DATABASE_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/stop',
                     view_func=STOP_DATABASE_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/recover',
                     view_func=RECOVER_DATABASE_VIEW, methods=['PUT'])

    # Internal API
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/start',
                     view_func=START_LOCAL_SERVER_VIEW, methods=['PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/recover',
                     view_func=RECOVER_DATABASE_SERVER_VIEW, methods=['PUT'])

    APP.add_url_rule('/api/1.0/deployment/', defaults={'database_id': None},
                     view_func=DEPLOYMENT_VIEW, methods=['GET'])

    APP.add_url_rule('/api/1.0/deployment/<int:database_id>', view_func=DEPLOYMENT_VIEW, methods=['GET', 'PUT'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/status/', view_func=STATUS_DATABASE_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/servers/<int:server_id>/status/',
                     view_func=STATUS_DATABASE_SERVER_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/deployment/users/<string:username>', view_func=DEPLOYMENT_USER_VIEW,
                     methods=['GET', 'PUT', 'POST', 'DELETE'])
    APP.add_url_rule('/api/1.0/deployment/users/<int:database_id>/<string:username>', view_func=DEPLOYMENT_USER_VIEW,
                     methods=['PUT', 'POST', 'DELETE'])
    APP.add_url_rule('/api/1.0/voltdeploy/status/',
                     view_func=VDM_STATUS_VIEW, methods=['GET'])
    APP.add_url_rule('/api/1.0/voltdeploy/configuration/',
                     view_func=VDM_CONFIGURATION_VIEW, methods=['GET', 'POST'])
    APP.add_url_rule('/api/1.0/voltdeploy/sync_configuration/',
                     view_func=SYNC_VDM_CONFIGURATION_VIEW, methods=['POST'])
    APP.add_url_rule('/api/1.0/databases/<int:database_id>/deployment/', view_func=DATABASE_DEPLOYMENT_VIEW,
                     methods=['GET', 'PUT'])
    APP.add_url_rule('/api/1.0/voltdeploy/', view_func=VDM_VIEW,
                     methods=['GET'])

    if os.path.exists('voltdeploy.log'):
        open('voltdeploy.log', 'w').close()
    handler = RotatingFileHandler('voltdeploy.log')
    handler.setFormatter(logging.Formatter(
         "%(asctime)s|%(levelname)s|%(message)s"))
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.NOTSET)
    log.addHandler(handler)


    APP.run(threaded=True, host=bindIp, port=__PORT__)
