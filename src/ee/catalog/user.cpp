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
#include "user.h"
#include "catalog.h"
#include "groupref.h"

using namespace catalog;
using namespace std;

User::User(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_groups(catalog, this, path + "/" + "groups")
{
    CatalogValue value;
    m_childCollections["groups"] = &m_groups;
    m_fields["sysproc"] = value;
    m_fields["adhoc"] = value;
    m_fields["shadowPassword"] = value;
}

void User::update() {
    m_sysproc = m_fields["sysproc"].intValue;
    m_adhoc = m_fields["adhoc"].intValue;
    m_shadowPassword = m_fields["shadowPassword"].strValue.c_str();
}

CatalogType * User::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("groups") == 0) {
        CatalogType *exists = m_groups.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_groups.add(childName);
    }
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * User::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("groups") == 0)
        return m_groups.get(childName);
    return NULL;
}

void User::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("groups") == 0)
        return m_groups.remove(childName);
}

const CatalogMap<GroupRef> & User::groups() const {
    return m_groups;
}

bool User::sysproc() const {
    return m_sysproc;
}

bool User::adhoc() const {
    return m_adhoc;
}

const string & User::shadowPassword() const {
    return m_shadowPassword;
}

