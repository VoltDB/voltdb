/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

#include "connectordestinationinfo.h"
#include "catalog.h"

using namespace catalog;
using namespace std;

ConnectorDestinationInfo::ConnectorDestinationInfo(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["ipaddr"] = value;
    m_fields["username"] = value;
    m_fields["password"] = value;
}

void ConnectorDestinationInfo::update() {
    m_ipaddr = m_fields["ipaddr"].strValue.c_str();
    m_username = m_fields["username"].strValue.c_str();
    m_password = m_fields["password"].strValue.c_str();
}

CatalogType * ConnectorDestinationInfo::addChild(const std::string &collectionName, const std::string &childName) {
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * ConnectorDestinationInfo::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

const string & ConnectorDestinationInfo::ipaddr() const {
    return m_ipaddr;
}

const string & ConnectorDestinationInfo::username() const {
    return m_username;
}

const string & ConnectorDestinationInfo::password() const {
    return m_password;
}

