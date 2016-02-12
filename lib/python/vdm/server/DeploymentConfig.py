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

import HTTPListener
from xml.etree.ElementTree import Element, SubElement, tostring


def handle_deployment_dict(deployment_elem, key, value, istop):
        if value:
            if istop == True:
                deployment_sub_element = deployment_elem
            else:
                deployment_sub_element = SubElement(deployment_elem, str(key))
            for key1, value1 in value.iteritems():
                if type(value1) is dict:
                    if istop == True:
                        if key1 not in HTTPListener.IGNORETOP:
                            handle_deployment_dict(deployment_sub_element, key1, value1, False)
                    else:
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
                            deployment_sub_element.attrib["name"] = value["name"]
                            deployment_sub_element.text = str(value1)
                        else:
                            if istop == False:
                                if value1 != None:
                                    deployment_sub_element.attrib[key1] = str(value1)
                            elif key1 not in HTTPListener.IGNORETOP:
                                if value1 != None:
                                    deployment_sub_element.attrib[key1] = str(value1)


def handle_deployment_list(deployment_elem, key, value):
    if key == 'servers':
        deployment_elem.attrib[key] = str(value)
    else:
        for items in value:
            handle_deployment_dict(deployment_elem, key, items, False)


class DeploymentConfiguration():
    """
    Handles the functionality related to the deployment file.
    """

    def __init__(self):
        pass

    @staticmethod
    def get_database_deployment(dbid):
        deployment_top = Element('deployment')
        value = HTTPListener.Global.DEPLOYMENT[dbid-1]
        db = HTTPListener.Global.DATABASES[dbid-1]
        host_count = len(db['members'])
        value['cluster']['hostcount'] = host_count
        # Add users
        addTop = False
        for duser in HTTPListener.Global.DEPLOYMENT_USERS:
            if duser['databaseid'] == dbid:
                # Only create subelement if users have anything in this database.
                if addTop != True:
                    users_top = SubElement(deployment_top, 'users')
                    addTop = True
                uelem = SubElement(users_top, "user")
                uelem.attrib["name"] = duser["name"]
                uelem.attrib["password"] = duser["password"]
                uelem.attrib["roles"] = duser["roles"]
                plaintext = str(duser["plaintext"])
                if isinstance(duser["plaintext"], bool):
                    if duser["plaintext"] == False:
                        plaintext = "false"
                    else:
                        plaintext = "true"
                uelem.attrib["plaintext"] = plaintext

        handle_deployment_dict(deployment_top, dbid, value, True)

        xmlstr = tostring(deployment_top,encoding='UTF-8')
        return xmlstr

