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

import os
from collections import defaultdict
import json
import traceback
from xml.etree.ElementTree import Element, SubElement, tostring, XML
import sys
from flask import jsonify
import HTTPListener
import DeploymentConfig
from Validation import ServerInputs, DatabaseInputs, JsonInputs, UserInputs, ConfigValidation
from logging.handlers import RotatingFileHandler
import logging
import ast
import itertools


def convert_xml_to_json(config_path):
    """
    Method to get the json content from xml file
    :param config_path (string): path of xml file
    """
    with open(config_path) as config_file:
        xml = config_file.read()
    config_content = XML(xml)
    xml_final = etree_to_dict(config_content)

    D2 = {}
    for (k, v) in zip(xml_final.keys(), xml_final.values()):
        D2[k] = v

    # To get the list of servers in case of old members[] (for backward compatible)
    if 'members' in D2[k] and 'member' in D2[k]['members'] and D2[k]['members']['member']:
        if type(D2[k]['members']['member']) is dict:
            member_json = get_field_from_xml(D2[k]['members']['member'], 'dict')
            HTTPListener.Global.SERVERS[member_json[0]['id']] = member_json[0]
        else:
            member_json = get_field_from_xml(D2[k]['members']['member'], 'list')
            for member in member_json:
                HTTPListener.Global.SERVERS[member['id']] = member

    if type(D2[k]['databases']['database']) is dict:
        db_json = get_field_from_xml(D2[k]['databases']['database'],
                                     'dict', 'database')
        HTTPListener.Global.DATABASES[db_json[0]['id']] = db_json[0]
    else:
        db_json = get_field_from_xml(D2[k]['databases']['database'],
                                     'list', 'database')
        for database in db_json:
            HTTPListener.Global.DATABASES[database['id']] = database

    if type(D2[k]['deployments']['deployment']) is dict:
        deployment_json = get_deployment_from_xml(D2[k]['deployments']
                                                  ['deployment'], 'dict')
        HTTPListener.Global.DEPLOYMENT[deployment_json[0]['databaseid']] = deployment_json[0]
    else:
        deployment_json = get_deployment_from_xml(D2[k]['deployments']
                                                  ['deployment'], 'list')
        for deployment in deployment_json:
            HTTPListener.Global.DEPLOYMENT[deployment['databaseid']] = deployment


    if D2[k]['deployments'] and 'deployment' in D2[k]['deployments']:
        if type(D2[k]['deployments']['deployment']) is dict:
            set_deployment_users(D2[k]['deployments']['deployment'])
        else:
            for deployment in D2[k]['deployments']['deployment']:
                set_deployment_users(deployment)


def set_deployment_users(deployment):
    if 'users' in deployment and deployment['users'] is not None\
            and 'user' in deployment['users']:
        if type(deployment) is dict:
            user_json = get_users_from_xml(deployment,
                                           'dict')
            for user in user_json:
                HTTPListener.Global.DEPLOYMENT_USERS[int(user['userid'])] = user
        else:
            user_json = get_users_from_xml(deployment,
                                           'list')
            for deployment_user in user_json:
                HTTPListener.Global.DEPLOYMENT_USERS[int(deployment_user['userid'])] = deployment_user


def validate_and_convert_xml_to_json(config_path):
    """
    Method to get the json content from xml file
    :param config_path (string): path of xml file
    """
    log_file = os.path.join(HTTPListener.Global.DATA_PATH, 'voltdeploy.log')

    handler = RotatingFileHandler(log_file)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s|%(levelname)s|%(message)s"))
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.NOTSET)
    log.addHandler(handler)

    try:
        with open(config_path) as config_file:
            xml = config_file.read()
        config_content = XML(xml)
        xml_final = etree_to_dict(config_content)

        D2 = {}
        for (k, v) in zip(xml_final.keys(), xml_final.values()):
            D2[k] = v

        populate_database(D2[k]['databases']['database'], log)

        if 'members' in D2[k] and 'member' in D2[k]['members'] and D2[k]['members']['member']:
            if type(D2[k]['members']['member']) is dict:
                populate_server(D2[k]['members']['member'], D2[k]['databases']['database'], log)

        populate_deployment(D2[k]['deployments']['deployment'], log)

    except Exception as err:
        log.error("Error while reloading configuration: %s", "Invalid file content.")


def populate_database(databases, log):
    success = True
    if type(databases) is dict:
        db_json = get_database_from_xml(databases,
                                     'dict', log, 'database')
        req = HTTPListener.DictClass()
        req.json = {}
        req.json = db_json[0]
        inputs = DatabaseInputs(req)
        if not inputs.validate():
            success = False
            sys.stdout.write(str(inputs.errors))
            log.error("Error while reloading configuration: %s", str(inputs.errors))

        if success is True:
            HTTPListener.Global.DATABASES = {db_json[0]['id']: db_json[0]}
    else:
        db_json = get_database_from_xml(databases,
                                     'list', log, 'database')
        success = True
        result = check_duplicate_database(db_json)
        if result != "":
            success = False
            log.error("Error while reloading configuration: %s", result)
        else:
            for database in db_json:
                req = HTTPListener.DictClass()
                req.json = {}
                req.json = database
                inputs = DatabaseInputs(req)
                if not inputs.validate():
                    success = False
                    sys.stdout.write(str(inputs.errors))
                    log.error("Error while reloading configuration: %s", str(inputs.errors))

        if success is True:
            HTTPListener.Global.DATABASES = {}
            for database in db_json:
                HTTPListener.Global.DATABASES[database['id']] = database


def populate_server(servers, databases, log):
    success = True
    if type(servers) is dict:
        member_json = get_field_from_xml(servers, 'dict')
        req = HTTPListener.DictClass()
        req.json = {}
        req.json = member_json[0]
        inputs = ServerInputs(req)
        if not inputs.validate():
            success = False
            sys.stdout.write(str(inputs.errors))
            log.error("Error while reloading configuration: %s", str(inputs.errors))
        else:
            result = validate_server_ports_dict(member_json[0], databases, True)
            if result is not None:
                success = False
                log.error("Error while reloading configuration: %s", result)

        if success is True:
            HTTPListener.Global.SERVERS = {member_json[0]['id']: member_json[0]}
    else:
        member_json = get_field_from_xml(servers, 'list')
        for member in member_json:
            req = HTTPListener.DictClass()
            req.json = {}
            req.json = member
            inputs = ServerInputs(req)
            if not inputs.validate():
                success = False
                sys.stdout.write(str(inputs.errors))
                log.error("Error while reloading configuration: %s", str(inputs.errors))
            result = validate_server_ports_list(member_json, databases, False)
            if result is not None:
                success = False
                log.error("Error while reloading configuration: %s", result)

        if success is True:
            HTTPListener.Global.SERVERS = {}
            for member in member_json:
                HTTPListener.Global.SERVERS[member['id']] = member


def populate_deployment(deployments, log):
    success = True
    if type(deployments) is dict:
        deployment_json = get_deployment_from_xml(deployments, 'dict')
        req = HTTPListener.DictClass()
        req.json = {}
        req.json = deployment_json[0]
        inputs = JsonInputs(req)
        if not inputs.validate():
            success = False
            sys.stdout.write(str(inputs.errors))
            log.error("Error while reloading configuration: %s", str(inputs.errors))

        if success is True:
            HTTPListener.Global.DEPLOYMENT = {deployment_json[0]['databaseid']: deployment_json[0]}
    else:
        deployment_json = get_deployment_from_xml(deployments, 'list')
        for deployment in deployment_json:
            req = HTTPListener.DictClass()
            req.json = {}
            req.json = deployment
            inputs = JsonInputs(req)
            if not inputs.validate():
                success = False
                sys.stdout.write(str(inputs.errors))
                log.error("Error while reloading configuration: %s", str(inputs.errors))

        if success is True:
            HTTPListener.Global.DEPLOYMENT = {}
            for deployment in deployment_json:
                HTTPListener.Global.DEPLOYMENT[deployment['databaseid']] = deployment
    success = True
    if type(deployments) is list:
        users = []
        for deployment in deployments:
            if 'users' in deployment and deployment['users'] is not None \
                    and 'user' in deployment['users']:
                user_json = get_users_from_xml(deployment,
                                               'dict')
                if type(user_json) is dict:
                    req = HTTPListener.DictClass()
                    req.json = {}
                    user_json['plaintext'] = bool(user_json['plaintext'])
                    req.json = user_json
                    inputs = UserInputs(req)
                    if not inputs.validate():
                        success = False
                        sys.stdout.write(str(inputs.errors))
                        log.error("Error while reloading configuration: %s", str(inputs.errors))

                    if success is True:
                        users.append(user_json)

                elif type(user_json) is list:
                    for user in user_json:
                        req = HTTPListener.DictClass()
                        req.json = {}
                        user['plaintext'] = bool(user['plaintext'])
                        req.json = user
                        inputs = UserInputs(req)
                        if not inputs.validate():
                            success = False
                            sys.stdout.write(str(inputs.errors))
                            log.error("Error while reloading configuration: %s", str(inputs.errors))

                    if len(user_json)> 1:
                        result = check_duplicate_user(user_json)
                        if result != "":
                            success = False
                            log.error("Error while reloading configuration: %s", result)

                    if success is True:
                        for user in user_json:
                            users.append(user)
        if len(users) > 0:
            HTTPListener.Global.DEPLOYMENT_USERS = {}
            for user in users:
                HTTPListener.Global.DEPLOYMENT_USERS[int(user['userid'])] = user
    else:
        user_json = get_users_from_xml(deployments,
                                       'dict')
        if type(user_json) is dict:
            req = HTTPListener.DictClass()
            req.json = {}
            user_json['plaintext'] = bool(user_json['plaintext'])
            req.json = user_json
            inputs = UserInputs(req)
            if not inputs.validate():
                success = False
                sys.stdout.write(str(inputs.errors))
                log.error("Error while reloading configuration: %s", str(inputs.errors))

            if success is True:
                HTTPListener.Global.DEPLOYMENT_USERS = {int(user_json['userid']): user_json}
        elif type(user_json) is list:
            for user in user_json:
                req = HTTPListener.DictClass()
                req.json = {}
                user['plaintext'] = bool(user['plaintext'])
                req.json = user
                inputs = UserInputs(req)
                if not inputs.validate():
                    success = False
                    sys.stdout.write(str(inputs.errors))
                    log.error("Error while reloading configuration: %s", str(inputs.errors))

            if len(user_json)> 1:
                result = check_duplicate_user(user_json)
                if result != "":
                    success = False
                    log.error("Error while reloading configuration: %s", result)

            if success is True:
                HTTPListener.Global.DEPLOYMENT_USERS = {}
                for user in user_json:
                    HTTPListener.Global.DEPLOYMENT_USERS[int(user['userid'])] = user


def validate_server_ports_dict(member, databases, isDict):
    arr = ["http-listener", "admin-listener", "internal-listener", "replication-listener", "zookeeper-listener",
           "client-listener"]

    specified_port_values = {
        "http-listener": HTTPListener.get_port(member['http-listener']),
        "admin-listener": HTTPListener.get_port(member['admin-listener']),
        "replication-listener": HTTPListener.get_port(member['replication-listener']),
        "client-listener": HTTPListener.get_port(member['client-listener']),
        "zookeeper-listener": HTTPListener.get_port(member['zookeeper-listener']),
        "internal-listener": HTTPListener.get_port(member['internal-listener'])
    }

    for option in arr:
        value = specified_port_values[option]
        for port_key in specified_port_values.keys():
            if option != port_key and value is not None and specified_port_values[port_key] == value:
                return "Duplicate port"


def validate_server_ports_list(members, databases, isDict):
    arr = ["http-listener", "admin-listener", "internal-listener", "replication-listener", "zookeeper-listener",
           "client-listener"]

    for i in range(len(members)):
        specified_port_values = {
            "http-listener": HTTPListener.get_port(members[i]['http-listener']),
            "admin-listener": HTTPListener.get_port(members[i]['admin-listener']),
            "replication-listener": HTTPListener.get_port(members[i]['replication-listener']),
            "client-listener": HTTPListener.get_port(members[i]['client-listener']),
            "zookeeper-listener": HTTPListener.get_port(members[i]['zookeeper-listener']),
            "internal-listener": HTTPListener.get_port(members[i]['internal-listener'])
        }

        for option in arr:
            value = specified_port_values[option]
            for port_key in specified_port_values.keys():
                if option != port_key and value is not None and specified_port_values[port_key] == value:
                    return "Duplicate port"

    if type(databases) is dict:
        for option in arr:
            result = check_port_valid(option, databases['members']['member'])
            if result is not None:
                return result

    elif type(databases) is list:
        for database in databases:
            for option in arr:
                result = check_port_valid(option, database['members']['member'])
                if result is not None:
                    return result


def check_port_valid(port_option, servers):
    result = None
    for i in range(len(servers)):
        for j in range(i + 1, len(servers)):
            if servers[i]['hostname'] == servers[j]['hostname']:
                result = compare(port_option, servers[i], servers[j])
    if result is not None:
        return result


def check_duplicate_database(databases):
    for i in range(len(databases)):
        for j in range(i + 1, len(databases)):
            result = compare_database(databases[i], databases[j])
    if result is not None:
        return result


def check_duplicate_user(users):
    for i in range(len(users)):
        for j in range(i + 1, len(users)):
            result = compare_user(users[i], users[j])
    if result is not None:
        return result


def get_servers_from_database_id(database_id):
    servers = []
    database = HTTPListener.Global.DATABASES.get(int(database_id))
    if database is None:
        return 'No database found for id: %u' % int(database_id)
    else:
        members = database['members']

    for server_id in members:
        server = HTTPListener.Global.SERVERS.get(server_id)
        servers.append(server)
    return servers


def compare(port_option, first, second):
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

    first_port = HTTPListener.get_port(first[port_option])
    if first_port is None or first_port == "":
        first_port = default_port

    second_port = HTTPListener.get_port(second[port_option])
    if second_port is None or second_port == "":
        second_port = default_port

    if first_port == second_port:
        return "Port %s for the same host is already used by server %s for %s" % \
                              (first_port, first['hostname'], port_option)


def compare_database(first, second):
    if first['name'] == second['name']:
        return 'Duplicate database name: %s' % first['name']
    else:
        return ""


def compare_user(first, second):
    if first['name'] == second['name']:
        return 'Duplicate user name: %s' % first['name']
    else:
        return ""


def get_deployment_from_xml(deployment_xml, is_list):
    """
    Function to get deployment json from the xml content
    :param deployment_xml: raw deployment object
    :param is_list: flag to know if it is list or dict
    :return: deployment object in required format
    """
    deployments = []
    if is_list is 'list':
        for deployment in deployment_xml:
            deployments.append(get_deployment(deployment))
    else:
        deployments.append(get_deployment(deployment_xml))
    return deployments


def get_deployment(deployment, is_upload=False):
    """
    Gets the required deployment object
    :param deployment: raw deployment object
    :param is_upload: flag to know if it is list or dict
    :return: deployment object in required format
    """
    new_deployment = {}
    for field in deployment:
        if field == 'export' or field == 'import':
            result = set_export_import_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'admin-mode':
            result = set_admin_mode_filed(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'cluster':
            result = set_cluster_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'commandlog':
            result = set_command_log_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'heartbeat':
            result = set_heartbeat_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'httpd':
            result = set_httpd_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'partition-detection':
            result = set_partition_detection_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'security':
            result = set_security_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'snapshot':
            result = set_snapshot_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'systemsettings':
            result = set_system_setting_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'dr':
            result = set_dr_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        elif field == 'users':
            result = set_users_field(deployment, field, new_deployment)
            if is_upload and 'success' not in result:
                return handle_errors(field, result)
        else:
            new_deployment[field] = convert_field_required_format(deployment, field)
    return new_deployment


def set_export_import_field(deployment, field, new_deployment):
    result = 'success'
    try:
        if deployment[field] is not None:
            new_deployment[field] = {}
            new_deployment[field]['configuration'] = {}
            if type(deployment[field]['configuration']) is list:
                new_deployment[field]['configuration'] = get_field_from_xml(
                    deployment[field]['configuration'], 'list', 'export')
            else:
                new_deployment[field]['configuration'] = get_field_from_xml(
                    deployment[field]['configuration'], 'dict', 'export')
        else:
            new_deployment[field] = None
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_admin_mode_filed(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['adminstartup'] = parse_bool_string(deployment[field]
                                                                  ['adminstartup'])
        new_deployment[field]['port'] = int(deployment[field]['port'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_cluster_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['hostcount'] = int(deployment[field]['hostcount'])
        new_deployment[field]['kfactor'] = int(deployment[field]['kfactor'])
        new_deployment[field]['sitesperhost'] = int(deployment[field]
                                                    ['sitesperhost'])
        new_deployment[field]['elastic'] = str(deployment[field]['elastic'])
        new_deployment[field]['schema'] = str(deployment[field]['schema'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_command_log_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]
                                                             ['enabled'])
        new_deployment[field]['synchronous'] = parse_bool_string(deployment[field]
                                                                 ['synchronous'])
        new_deployment[field]['logsize'] = int(deployment[field]['logsize'])
        new_deployment[field]['frequency'] = {}
        new_deployment[field]['frequency']['transactions'] = int(
            deployment[field]['frequency']['transactions'])
        new_deployment[field]['frequency']['time'] = int(deployment[field]
                                                         ['frequency']
                                                         ['time'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_heartbeat_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['timeout'] = int(deployment[field]['timeout'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_partition_detection_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]
                                                             ['enabled'])
        new_deployment[field]['snapshot'] = {}
        new_deployment[field]['snapshot']['prefix'] = deployment[field]['snapshot']['prefix']
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_httpd_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['port'] = int(deployment[field]['port'])
        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]
                                                             ['enabled'])
        new_deployment[field]['jsonapi'] = {}
        new_deployment[field]['jsonapi']['enabled'] = parse_bool_string(
            deployment[field]['jsonapi']['enabled'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_security_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]
                                                             ['enabled'])
        new_deployment[field]['provider'] = str(deployment[field]['provider'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_snapshot_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['enabled'] = parse_bool_string(deployment[field]
                                                             ['enabled'])
        new_deployment[field]['frequency'] = str(deployment[field]['frequency'])
        new_deployment[field]['prefix'] = str(deployment[field]['prefix'])
        new_deployment[field]['retain'] = int(deployment[field]['retain'])
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_system_setting_field(deployment, field, new_deployment):
    result = 'success'
    try:
        new_deployment[field] = {}
        new_deployment[field]['elastic'] = {}
        new_deployment[field]['elastic']['duration'] = int(deployment[field]
                                                           ['elastic']
                                                           ['duration'])
        new_deployment[field]['elastic']['throughput'] = int(deployment[field]
                                                             ['elastic']['throughput'])
        new_deployment[field]['query'] = {}
        new_deployment[field]['query']['timeout'] = int(deployment[field]['query']
                                                        ['timeout'])
        new_deployment[field]['snapshot'] = {}
        new_deployment[field]['snapshot']['priority'] = int(deployment[field]
                                                            ['snapshot']['priority'])
        new_deployment[field]['temptables'] = {}
        new_deployment[field]['temptables']['maxsize'] = int(deployment[field]
                                                             ['temptables']['maxsize'])
        if 'resourcemonitor' not in deployment[field] or \
                        deployment[field]['resourcemonitor'] is None:
            if 'resourcemonitor' in deployment[field]:
                new_deployment[field]['resourcemonitor'] = None
        else:
            new_deployment[field]['resourcemonitor'] = {}
            if 'memorylimit' in deployment[field]['resourcemonitor']:
                new_deployment[field]['resourcemonitor']['memorylimit'] = \
                    deployment[field]['resourcemonitor']['memorylimit']

            if 'disklimit' in deployment[field]['resourcemonitor'] and 'feature' in \
                    deployment[field]['resourcemonitor']['disklimit']:
                if type(deployment[field]['resourcemonitor']['disklimit']['feature']) is \
                        list:
                    new_deployment[field]['resourcemonitor']['disklimit'] = {}
                    new_deployment[field]['resourcemonitor']['disklimit']['feature'] = \
                        get_field_from_xml(deployment[field]
                                           ['resourcemonitor']['disklimit']['feature'],
                                           'list', 'disklimit')
                else:
                    new_deployment[field]['resourcemonitor']['disklimit'] = {}
                    new_deployment[field]['resourcemonitor']['disklimit']['feature'] = \
                        get_field_from_xml(deployment[field]['resourcemonitor']
                                           ['disklimit']['feature'], 'dict', 'disklimit')

    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_dr_field(deployment, field, new_deployment):
    result = 'success'
    try:
        if deployment[field] != 'None':
            new_deployment[field] = {}
            new_deployment[field]['id'] = int(deployment[field]['id'])
            new_deployment[field]['listen'] = parse_bool_string(deployment[field]
                                                                ['listen'])
            if 'port' in deployment[field]:
                new_deployment[field]['port'] = int(deployment[field]['port'])
            if 'connection' in deployment[field] and deployment[field]['connection'] \
                    is not None and 'source' in deployment[field]['connection']:
                new_deployment[field]['connection'] = {}
                new_deployment[field]['connection']['source'] = str(
                    deployment[field]['connection']['source'])

    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def set_users_field(deployment, field, new_deployment):
    result = 'success'
    try:
        if deployment[field] is not None:
            new_deployment[field] = {}
            if type(deployment[field]['user']) is list:
                new_deployment[field]['user'] = []
                new_deployment[field]['user'] = get_field_from_xml(deployment[field]
                                                                   ['user'], 'list', 'user')
            else:
                new_deployment[field]['user'] = []
                new_deployment[field]['user'] = get_field_from_xml(deployment[field]
                                                                   ['user'], 'dict', 'user')
    except Exception, err:
        result = str(err)
        print_errors(field, result)
    finally:
        return result


def get_field_from_xml(xml_content, is_list, type_content=''):
    """
    Gets the deployment attribute value in required format
    :param content: deployment attribute value in raw format
    :param is_list: check if it is a list or dict
    :param type_content: attribute type
    :return: deployment attribute object
    """
    final_property = []
    if is_list is 'list':
        for content in xml_content:
            final_property.append(get_fields(content, type_content))
    else:
        final_property.append(get_fields(xml_content, type_content))
    return final_property


def get_database_from_xml(xml_content, is_list, log, type_content=''):
    """
    Gets the deployment attribute value in required format
    :param content: deployment attribute value in raw format
    :param is_list: check if it is a list or dict
    :param type_content: attribute type
    :return: deployment attribute object
    """
    final_property = []
    if is_list is 'list':
        for content in xml_content:
            final_property.append(get_database_fields(content, type_content, log))
    else:
        final_property.append(get_database_fields(xml_content, type_content, log))
    return final_property


def get_fields(content, type_content):
    """
    Converts the deployment attribute value in required format
    :param content: deployment attribute value in raw format
    :param type_content: attribute type
    :return: deployment attribute object
    """
    new_property = {}
    for field in content:
        if field == 'plaintext' and type_content == 'user':
            new_property[field] = parse_bool_string(content[field])
        elif field == 'property' and type_content == 'export':
            if type(content['property']) is list:
                new_property['property'] = get_field_from_xml(content['property'],
                                                              'list', 'export')
            else:
                new_property['property'] = get_field_from_xml(content['property'],
                                                              'dict', 'export')
        elif field == 'enabled' and type_content == 'export':
            new_property[field] = parse_bool_string(content[field])
        elif field == 'members':
            members = []
            if type(content[field]) is dict:
                members = set_members_field(content[field])
            # To get the database members in case of old members[] (for backward compatible)
            elif type(content[field]) is str:
                members = convert_field_required_format(content, field)
            new_property[field] = members
        else:
            new_property[field] = convert_field_required_format(content, field)
    return new_property


def get_database_fields(content, type_content, log):
    """
    Converts the deployment attribute value in required format
    :param content: deployment attribute value in raw format
    :param type_content: attribute type
    :return: deployment attribute object
    """
    new_property = {}
    for field in content:
        if field == 'plaintext' and type_content == 'user':
            new_property[field] = parse_bool_string(content[field])
        elif field == 'property' and type_content == 'export':
            if type(content['property']) is list:
                new_property['property'] = get_field_from_xml(content['property'],
                                                              'list', 'export')
            else:
                new_property['property'] = get_field_from_xml(content['property'],
                                                              'dict', 'export')
        elif field == 'enabled' and type_content == 'export':
            new_property[field] = parse_bool_string(content[field])
        elif field == 'members':
            members = []
            if type(content[field]) is dict:
                members = populate_server(content[field]['member'], content, log)
            # To get the database members in case of old members[] (for backward compatible)
            elif type(content[field]) is str:
                members = convert_field_required_format(content, field)
            new_property[field] = members
        else:
            new_property[field] = convert_field_required_format(content, field)
    return new_property


def set_members_field(content):
    members = []
    if content and 'member' in content and content['member']:
        if type(content['member']) is dict:
            member_json = get_field_from_xml(content['member'], 'dict')
            HTTPListener.Global.SERVERS[member_json[0]['id']] = member_json[0]
        else:
            member_json = get_field_from_xml(content['member'], 'list')
            for member in member_json:
                HTTPListener.Global.SERVERS[member['id']] = member
        for mem in member_json:
            members.append(mem['id'])
    return members


def populate_server(servers, databases, log):
    members = []
    success = True
    if type(servers) is dict:
        member_json = get_field_from_xml(servers, 'dict')
        req = HTTPListener.DictClass()
        req.json = {}
        req.json = member_json[0]
        inputs = ServerInputs(req)
        if not inputs.validate():
            success = False
            sys.stdout.write(str(inputs.errors))
            log.error("Error while reloading configuration: %s", str(inputs.errors))
        else:
            result = validate_server_ports_dict(member_json[0], databases, True)
            if result is not None:
                success = False
                log.error("Error while reloading configuration: %s", result)

        if success is True:
            HTTPListener.Global.SERVERS = {member_json[0]['id']: member_json[0]}
    else:
        member_json = get_field_from_xml(servers, 'list')
        for member in member_json:
            req = HTTPListener.DictClass()
            req.json = {}
            req.json = member
            inputs = ServerInputs(req)
            if not inputs.validate():
                success = False
                sys.stdout.write(str(inputs.errors))
                log.error("Error while reloading configuration: %s", str(inputs.errors))
            result = validate_server_ports_list(member_json, databases, False)
            if result is not None:
                success = False
                log.error("Error while reloading configuration: %s", result)

        if success is True:
            HTTPListener.Global.SERVERS = {}
            for member in member_json:
                HTTPListener.Global.SERVERS[member['id']] = member

    for mem in member_json:
            members.append(mem['id'])
    return members

def get_users_from_xml(deployment_xml, is_list):
    """
    Gets the users from the json obtained from xml file
    :param deployment_xml:
    :param is_list:
    :return: users object
    """
    users = []
    if is_list is 'list':
        for deployment in deployment_xml:
            get_users(deployment, users)
    else:
        get_users(deployment_xml, users)
    return users


def get_users(deployment, users):
    """
    Creates the users object
    :param deployment:
    :param users:
    :return: user object
    """
    if 'users' in deployment and deployment['users'] is not None:
        if type(deployment['users']['user']) is list:
            for user in deployment['users']['user']:
                users.append(convert_user_required_format(user))
        else:
            users.append(convert_user_required_format(deployment['users']
                                                      ['user']))
    return users


def convert_user_required_format(user):
    """
    Convert the fields in user to required format
    :param user:
    :return: user object
    """
    for field in user:
        if field == 'databaseid':
            user[field] = int(user[field])
    return user


def convert_field_required_format(type, field):
    """
    Convert the fields to required format
    :param type: attribute name
    :param field: field name
    :return: field value in required format
    """
    if field == 'databaseid':
        modified_field = int(type[field])
    elif field == 'id':
        modified_field = int(type[field])
    elif field == 'members':
        modified_field = ast.literal_eval(type[field])
    else:
        modified_field = type[field]
    return modified_field


def etree_to_dict(t):
    """
    Gets the json object from the xml content
    :param t: xml content
    :return: object
    """
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


def parse_bool_string(bool_string):
    """
    Get the boolean value of bool_string
    :param bool_string:
    :return: boolean value
    """
    return bool_string.upper() == 'TRUE'


def get_deployment_for_upload(deployment_xml):
    """
    Gets the deployment json in required format for upload process
    :param deployment_xml: deployment object in raw format
    :return: deployment object in required format
    """
    deployments = []
    deployments.append(get_deployment(deployment_xml, True))
    return deployments


def get_configuration():
    """
    Gets the voltdeploy json object
    :return: voltdeploy json object
    """
    deployment_json = {
        'voltdeploy': {
            'databases': HTTPListener.Global.DATABASES,
            'members': HTTPListener.Global.SERVERS,
            'deployments': HTTPListener.Global.DEPLOYMENT,
            'deployment_users': HTTPListener.Global.DEPLOYMENT_USERS
        }
    }
    return deployment_json


def write_configuration_file():
    """
    Write the xml content to voltdeploy.xml
    """
    main_header = make_configuration_file()

    try:
        path = os.path.join(HTTPListener.Global.CONFIG_PATH, 'voltdeploy.xml')
        f = open(path, 'w')
        f.write(main_header)
        f.close()

    except Exception, err:
        print str(err)


def make_configuration_file():
    """
    Prepare the xml content
    :return: xml content
    """
    main_header = Element('voltdeploy')
    db_top = SubElement(main_header, 'databases')
    deployment_top = SubElement(main_header, 'deployments')

    for key, value in HTTPListener.Global.DATABASES.items():
        db_elem = SubElement(db_top, 'database')
        for k, val in value.items():
            if isinstance(val, bool):
                if not value:
                    db_elem.attrib[k] = "false"
                else:
                    db_elem.attrib[k] = "true"
            elif k == 'members':
                mem_elem = SubElement(db_elem, 'members')
                for mem_id in val:
                    server_info = HTTPListener.Global.SERVERS.get(mem_id)
                    if server_info:
                        mem_item = SubElement(mem_elem, 'member')
                        for field in server_info:
                            if isinstance(server_info[field], bool):
                                if not value:
                                    mem_item.attrib[field] = "false"
                                else:
                                    mem_item.attrib[field] = "true"
                            else:
                                mem_item.attrib[field] = str(server_info[field])
            else:
                db_elem.attrib[k] = str(val)

    for key, value in HTTPListener.Global.DEPLOYMENT.items():

        HTTPListener.Global.DEPLOYMENT[key]['users'] = {}
        HTTPListener.Global.DEPLOYMENT[key]['users']['user'] = []

        d = HTTPListener.Global.DEPLOYMENT_USERS
        for user_key, user_value in d.iteritems():
            HTTPListener.Global.DEPLOYMENT[user_value['databaseid']]['users']['user'].append({
                'name': d[user_key]['name'],
                'roles': d[user_key]['roles'],
                'plaintext': d[user_key]['plaintext'],
                'password': d[user_key]['password'],
                'databaseid': d[user_key]['databaseid'],
                'userid': d[user_key]['userid']
            })

        deployment_elem = SubElement(deployment_top, 'deployment')
        for k, val in value.items():
            if k == 'users' and not val['user']:
                pass
            elif type(val) is dict:
                DeploymentConfig.handle_deployment_dict(deployment_elem, k, val, False)
            elif type(val) is list:
                DeploymentConfig.handle_deployment_list(deployment_elem, k, val)
            else:
                if val is not None:
                    deployment_elem.attrib[k] = str(val)
    return tostring(main_header, encoding='UTF-8')


def handle_errors(config_type, error):
    """
    Print the error message while preparing required deployment object
    :param config_type: Attribute name like (export, import etc)
    :param error:  Error obtained while converting the deployment attribute
    :return: error object
    """
    return {'error': config_type + ': ' + str(error)}


def print_errors(config_type, error):
    """
    Print the error message while preparing required deployment object
    :param config_type: Attribute name like (export, import etc)
    :param error:  Error obtained while converting the deployment attribute
    """
    print 'error (' + config_type + '): ' + str(error)
    print traceback.format_exc()


def set_deployment_for_upload(database_id, request):
    if 'text/xml' in request.headers['Content-Type'] or 'application/xml' in request.headers['Content-Type']:
        content = request.data
        return read_content(content, database_id)
    else:
        dep_file = request.files['file']
        if dep_file and HTTPListener.allowed_file(dep_file.filename):
            content = dep_file.read()
            return read_content(content, database_id)
        else:
            return {'status': 401, 'statusString': 'Invalid file type.'}
        return {'status': 201, 'statusString': 'success'}


def read_content(content, database_id):
    try:
        o = XML(content)
        xml_final = json.loads(json.dumps(etree_to_dict(o)))
        if 'deployment' in xml_final and type(xml_final['deployment']) is dict:
            deployment_data = get_deployment_for_upload(xml_final['deployment'])
            if type(deployment_data) is dict:
                if 'error' in deployment_data:
                    return {'status': 'failure', 'error': deployment_data['error']}
            else:
                deployment_json = deployment_data[0]
            req = HTTPListener.DictClass()
            req.json = {}
            req.json = deployment_json
            inputs = JsonInputs(req)
            if not inputs.validate():
                return {'status': 401, 'statusString': inputs.errors}

            result = check_validation_deployment(req)
            if 'status' in result and result['status'] == 401:
                return {'status': 401, 'statusString': result['statusString']}

            is_duplicate_user = check_duplicate_users(req)
            if not is_duplicate_user:
                return {'status': 401, 'statusString': 'Duplicate users not allowed.'}

            is_invalid_roles = check_invalid_roles(req)
            if not is_invalid_roles:
                return {'status': 401, 'statusString': 'Invalid user roles.'}

            HTTPListener.map_deployment(req, database_id)

            deployment_user = [v if type(v) is list else [v] for v in HTTPListener.Global.DEPLOYMENT_USERS.values()]
            if deployment_user is not None:
                for user in deployment_user:
                    if user[0]['databaseid'] == database_id:
                        del HTTPListener.Global.DEPLOYMENT_USERS[int(user[0]['userid'])]
            if 'users' in req.json and 'user' in req.json['users']:
                for user in req.json['users']['user']:
                    if not HTTPListener.Global.DEPLOYMENT_USERS:
                        user_id = 1
                    else:
                        user_id = HTTPListener.Global.DEPLOYMENT_USERS.keys()[-1] + 1
                    user_roles = ','.join(set(user['roles'].split(',')))

                    HTTPListener.Global.DEPLOYMENT_USERS[user_id] = {
                            'name': user['name'],
                            'roles': user_roles,
                            'password': user['password'],
                            'plaintext': user['plaintext'],
                            'databaseid': database_id,
                            'userid': user_id
                        }
            HTTPListener.sync_configuration()
            write_configuration_file()
            return {'status': 200, 'statusString': 'success'}
        else:
            return {'status': 401, 'statusString': 'Invalid file content.'}
    except Exception as err:
        return {'status': 401, 'statusString': 'Invalid file content.'}


def check_duplicate_users(req):
    if 'users' in req.json and 'user' in req.json['users']:
        user_name_list = []
        for user in req.json['users']['user']:
            if user['name'] in user_name_list:
                return False
            user_name_list.append(user['name'])
    return True


def check_invalid_roles(req):
    if 'users' in req.json and 'user' in req.json['users']:
        for user in req.json['users']['user']:
            roles = str(user['roles']).split(',')
            for role in roles:
                if role.strip() == '':
                    return False
    return True


def check_validation_deployment(req):
    if 'systemsettings' in req.json and 'resourcemonitor' in req.json['systemsettings']:
        if 'memorylimit' in req.json['systemsettings']['resourcemonitor'] and \
                        'size' in req.json['systemsettings']['resourcemonitor']['memorylimit']:
            size = str(req.json['systemsettings']['resourcemonitor']['memorylimit']['size'])
            response = json.loads(HTTPListener.check_size_value(size, 'memorylimit').data)
            if 'error' in response:
                return {'status': 401, 'statusString': response['error']}
        disk_limit_arr = []
        if 'disklimit' in req.json['systemsettings']['resourcemonitor'] and \
           'feature' in req.json['systemsettings']['resourcemonitor']['disklimit']:
            for feature in req.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                size = feature['size']
                if feature['name'] in disk_limit_arr:
                    return {'status': 401, 'statusString': 'Duplicate items are not allowed.'}
                disk_limit_arr.append(feature['name'])
                response = json.loads(HTTPListener.check_size_value(size, 'disklimit').data)
                if 'error' in response:
                    return {'status': 401, 'statusString': response['error']}
        if 'snapshot' in req.json and 'frequency' in req.json['snapshot']:
            frequency_unit = ['h', 'm', 's']
            frequency = str(req.json['snapshot']['frequency'])
            if ' ' in frequency:
                return {'status': 401, 'statusString': 'Snapshot: White spaces not allowed in frequency.'}
            last_char = frequency[len(frequency) - 1]
            if last_char not in frequency_unit:
                return {'status': 401, 'statusString': 'Snapshot: Invalid frequency value.'}
            frequency = frequency[:-1]
            try:
                int_frequency = int(frequency)
            except Exception, exp:
                return {'status': 401, 'statusString': 'Snapshot: ' + str(exp)}
    if 'export' in req.json and 'configuration' in req.json['export']:
        for configuration in req.json['export']['configuration']:
            result = check_export_property(configuration['type'], configuration['property'])
            if 'status' in result and result['status'] == 401:
                return {'status': 401, 'statusString': 'Export: ' + result['statusString']}

    if 'import' in req.json and 'configuration' in req.json['import']:
        for configuration in req.json['import']['configuration']:
            result = check_export_property(configuration['type'], configuration['property'])
            if 'status' in result and result['status'] == 401:
                return {'status': 401, 'statusString': 'Import: ' + result['statusString']}

    return {'status': 200, 'statusString': 'success'}


def check_export_property(type, properties):
    property_list = []
    for property in properties:
        if 'name' in property and 'value' in property:
            if str(property['name']).strip() == '' or str(property['value']).strip() == '':
                return {'status': 401, 'statusString': 'Invalid property.'}
            if property['name'] in property_list:
                return {'status': 401, 'statusString': 'Duplicate properties are not allowed.'}
            property_list.append(property['name'])
        else:
            return {'status': 401, 'statusString': 'Invalid property.'}

    if str(type).lower() == 'kafka':
        if 'metadata.broker.list' not in property_list:
            return {'status': 401, 'statusString': 'Default property(metadata.broker.list) of kafka not present.'}
    if str(type).lower() == 'elasticsearch':
        if 'endpoint' not in property_list:
            return {'status': 401, 'statusString': 'Default property(endpoint) of elasticsearch not present.'}
    if str(type).lower() == 'file':
        if 'type' not in property_list or 'nonce' not in property_list \
                or 'outdir' not in property_list:
            return {'status': 401, 'statusString': 'Default properties(type, nonce, outdir) of file not present.'}
    if str(type).lower() == 'http':
        if 'endpoint' not in property_list:
            return {'status': 401, 'statusString': 'Default property(endpoint) of  http not present.'}
    if str(type).lower() == 'jdbc':
        if 'jdbcdriver' not in property_list or 'jdbcurl' not in property_list:
            return {'status': 401, 'statusString': 'Default properties(jdbcdriver, jdbcurl) of jdbc not present.'}
    if str(type).lower() == 'rabbitmq':
        if 'broker.host' not in property_list and 'amqp.uri' not in property_list:
            return {'status': 401, 'statusString': 'Default property(either amqp.uri or broker.host) of '
                                                'rabbitmq not present.'}
        elif 'broker.host' in property_list and 'amqp.uri' in property_list:
            return {'status': 401, 'statusString': 'Both broker.host and amqp.uri cannot be included as rabbibmq property.'}
    return {'status': 200, 'statusString': 'success'}
