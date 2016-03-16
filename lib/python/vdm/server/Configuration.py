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
import os
from collections import defaultdict
import json
import traceback
from xml.etree.ElementTree import Element, SubElement, tostring, XML
from Validation import JsonInputs
import HTTPListener
import DeploymentConfig


def convert_xml_to_json(config_path):
    """
    Method to get the json content from xml file
    :param config_path (string): path of xml file
    """
    with open(config_path) as config_file:
        xml = config_file.read()
    config_content = XML(xml)
    xml_final = json.loads(json.dumps(etree_to_dict(config_content)))
    if type(xml_final['voltdeploy']['members']['member']) is dict:
        member_json = get_field_from_xml(xml_final['voltdeploy']['members']['member'], 'dict')
    else:
        member_json = get_field_from_xml(xml_final['voltdeploy']['members']['member'], 'list')

    if type(xml_final['voltdeploy']['databases']['database']) is dict:
        db_json = get_field_from_xml(xml_final['voltdeploy']['databases']['database'],
                                     'dict', 'database')
    else:
        db_json = get_field_from_xml(xml_final['voltdeploy']['databases']['database'],
                                     'list', 'database')

    if type(xml_final['voltdeploy']['deployments']['deployment']) is dict:
        deployment_json = get_deployment_from_xml(xml_final['voltdeploy']['deployments']
                                                  ['deployment'], 'dict')
    else:
        deployment_json = get_deployment_from_xml(xml_final['voltdeploy']['deployments']
                                                  ['deployment'], 'list')
    if type(xml_final['voltdeploy']['deployments']['deployment']) is dict:
        user_json = get_users_from_xml(xml_final['voltdeploy']['deployments']['deployment'],
                                       'dict')
    else:
        user_json = get_users_from_xml(xml_final['voltdeploy']['deployments']['deployment'],
                                       'list')

    HTTPListener.Global.DATABASES = db_json

    HTTPListener.Global.SERVERS = member_json

    HTTPListener.Global.DEPLOYMENT = deployment_json

    HTTPListener.Global.DEPLOYMENT_USERS = user_json


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
            new_deployment[field] = deployment[field]
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
        if deployment[field] is not None:
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
        else:
            new_property[field] = convert_field_required_format(content, field)
    return new_property


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
        path = os.path.join(HTTPListener.Global.PATH, 'voltdeploy.xml')
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
    server_top = SubElement(main_header, 'members')
    deployment_top = SubElement(main_header, 'deployments')
    i = 0
    while i < len(HTTPListener.Global.DATABASES):
        db_elem = SubElement(db_top, 'database')
        for key, value in HTTPListener.Global.DATABASES[i].iteritems():
            if isinstance(value, bool):
                if value == False:
                    db_elem.attrib[key] = "false"
                else:
                    db_elem.attrib[key] = "true"
            else:
                db_elem.attrib[key] = str(value)
        i += 1

    i = 0
    while i < len(HTTPListener.Global.SERVERS):
        server_elem = SubElement(server_top, 'member')
        for key, value in HTTPListener.Global.SERVERS[i].iteritems():
            if isinstance(value, bool):
                if value == False:
                    server_elem.attrib[key] = "false"
                else:
                    server_elem.attrib[key] = "true"
            else:
                server_elem.attrib[key] = str(value)
        i += 1

    i = 0
    while i < len(HTTPListener.Global.DEPLOYMENT):
        HTTPListener.Global.DEPLOYMENT[i]['users'] = {}
        HTTPListener.Global.DEPLOYMENT[i]['users']['user'] = []
        deployment_user = filter(lambda t: t['databaseid'] == HTTPListener.Global.DEPLOYMENT[i]['databaseid']
                                 , HTTPListener.Global.DEPLOYMENT_USERS)
        if len(deployment_user) == 0:
            HTTPListener.Global.DEPLOYMENT[i]['users'] = None
        for user in deployment_user:
            HTTPListener.Global.DEPLOYMENT[i]['users']['user'].append({
                'name': user['name'],
                'roles': user['roles'],
                'plaintext': user['plaintext'],
                'password': user['password'],
                'databaseid': user['databaseid']
            })

        deployment_elem = SubElement(deployment_top, 'deployment')
        for key, value in HTTPListener.Global.DEPLOYMENT[i].iteritems():
            if type(value) is dict:
                DeploymentConfig.handle_deployment_dict(deployment_elem, key, value, False)
            elif type(value) is list:
                DeploymentConfig.handle_deployment_list(deployment_elem, key, value)
            else:
                if value is not None:
                    deployment_elem.attrib[key] = str(value)
        i += 1
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
    dep_file = request.files['file']
    if dep_file and HTTPListener.allowed_file(dep_file.filename):
        try:
            content = dep_file.read()
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
                    return {'status': 'failure', 'errors': inputs.errors}

                result = check_validation_deployment(req)
                if 'status' in result and result['status'] == 'error':
                    return {'status': 'failure', 'error': result['error']}

                HTTPListener.map_deployment(req, database_id)
                HTTPListener.Global.DEPLOYMENT_USERS = []
                if 'users' in req.json and 'user' in req.json['users']:
                    for user in req.json['users']['user']:
                        HTTPListener.Global.DEPLOYMENT_USERS.append(
                            {
                                'name': user['name'],
                                'roles': user['roles'],
                                'password': user['password'],
                                'plaintext': user['plaintext'],
                                'databaseid': database_id
                            }
                        )
                HTTPListener.sync_configuration()
                write_configuration_file()
            else:
                return {'status': 'failure', 'error': 'Invalid file content.'}

        except Exception as err:
            return {'status': 'failure', 'error': 'Invalid file content.'}
    else:
        return {'status': 'failure', 'error': 'Invalid file type.'}
    return {'status': 'success'}


def check_validation_deployment(req):
    if 'systemsettings' in req.json and 'resourcemonitor' in req.json['systemsettings']:
        if 'memorylimit' in req.json['systemsettings']['resourcemonitor'] and \
                            'size' in req.json['systemsettings']['resourcemonitor']['memorylimit']:
            size = str(req.json['systemsettings']['resourcemonitor']['memorylimit']['size'])
            response = json.loads(HTTPListener.check_size_value(size, 'memorylimit').data)
            if 'error' in response:
                return {'status': 'error', 'error': response['error']}
            disk_limit_arr = []
            if 'disklimit' in req.json['systemsettings']['resourcemonitor'] and \
                            'feature' in req.json['systemsettings']['resourcemonitor']['disklimit']:
                for feature in req.json['systemsettings']['resourcemonitor']['disklimit']['feature']:
                    size = feature['size']
                    if feature['name'] in disk_limit_arr:
                        return {'status': 'error', 'error': 'Duplicate items are not allowed.'}
                    disk_limit_arr.append(feature['name'])
                    response = json.loads(HTTPListener.check_size_value(size, 'disklimit').data)
                    if 'error' in response:
                        return {'status': 'error', 'error': response['error']}
        if 'snapshot' in req.json and 'frequency' in req.json['snapshot']:
            frequency_unit = ['h', 'm', 's']
            frequency = str(req.json['snapshot']['frequency'])
            last_char =  frequency[len(frequency)-1]
            if last_char not in frequency_unit:
                return {'status': 'error', 'error': 'Snapshot: Invalid frequency value.'}
            frequency = frequency[:-1]
            try:
                int_frequency = int(frequency)
            except Exception, exp:
                return {'status': 'error', 'error': 'Snapshot: ' + str(exp)}
    return {'status': 'success'}