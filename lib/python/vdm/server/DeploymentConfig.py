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
        value = HTTPListener.Global.DEPLOYMENT[dbid]
        db = HTTPListener.Global.DATABASES[dbid]
        host_count = len(db['members'])
        value['cluster']['hostcount'] = host_count
        # Add users
        addTop = False
        for key, duser in HTTPListener.Global.DEPLOYMENT_USERS.items():
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

