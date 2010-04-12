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

#include <cassert>
#include "group.h"
#include "catalog.h"
#include "userref.h"

using namespace catalog;
using namespace std;

Group::Group(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_users(catalog, this, path + "/" + "users")
{
    CatalogValue value;
    m_childCollections["users"] = &m_users;
    m_fields["sysproc"] = value;
    m_fields["adhoc"] = value;
}

void Group::update() {
    m_sysproc = m_fields["sysproc"].intValue;
    m_adhoc = m_fields["adhoc"].intValue;
}

CatalogType * Group::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("users") == 0) {
        CatalogType *exists = m_users.get(childName);
        if (exists)
            return NULL;
        return m_users.add(childName);
    }
    return NULL;
}

CatalogType * Group::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("users") == 0)
        return m_users.get(childName);
    return NULL;
}

void Group::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("users") == 0)
        return m_users.remove(childName);
}

const CatalogMap<UserRef> & Group::users() const {
    return m_users;
}

bool Group::sysproc() const {
    return m_sysproc;
}

bool Group::adhoc() const {
    return m_adhoc;
}

