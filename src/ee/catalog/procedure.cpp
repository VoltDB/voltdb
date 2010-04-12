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
#include "procedure.h"
#include "catalog.h"
#include "procparameter.h"
#include "column.h"
#include "userref.h"
#include "authprogram.h"
#include "groupref.h"
#include "statement.h"
#include "table.h"

using namespace catalog;
using namespace std;

Procedure::Procedure(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_authUsers(catalog, this, path + "/" + "authUsers"), m_authGroups(catalog, this, path + "/" + "authGroups"), m_authPrograms(catalog, this, path + "/" + "authPrograms"), m_statements(catalog, this, path + "/" + "statements"), m_parameters(catalog, this, path + "/" + "parameters")
{
    CatalogValue value;
    m_fields["classname"] = value;
    m_childCollections["authUsers"] = &m_authUsers;
    m_childCollections["authGroups"] = &m_authGroups;
    m_fields["readonly"] = value;
    m_fields["singlepartition"] = value;
    m_fields["everysite"] = value;
    m_fields["systemproc"] = value;
    m_fields["hasjava"] = value;
    m_fields["partitiontable"] = value;
    m_fields["partitioncolumn"] = value;
    m_fields["partitionparameter"] = value;
    m_childCollections["authPrograms"] = &m_authPrograms;
    m_childCollections["statements"] = &m_statements;
    m_childCollections["parameters"] = &m_parameters;
}

void Procedure::update() {
    m_classname = m_fields["classname"].strValue.c_str();
    m_readonly = m_fields["readonly"].intValue;
    m_singlepartition = m_fields["singlepartition"].intValue;
    m_everysite = m_fields["everysite"].intValue;
    m_systemproc = m_fields["systemproc"].intValue;
    m_hasjava = m_fields["hasjava"].intValue;
    m_partitiontable = m_fields["partitiontable"].typeValue;
    m_partitioncolumn = m_fields["partitioncolumn"].typeValue;
    m_partitionparameter = m_fields["partitionparameter"].intValue;
}

CatalogType * Procedure::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("authUsers") == 0) {
        CatalogType *exists = m_authUsers.get(childName);
        if (exists)
            return NULL;
        return m_authUsers.add(childName);
    }
    if (collectionName.compare("authGroups") == 0) {
        CatalogType *exists = m_authGroups.get(childName);
        if (exists)
            return NULL;
        return m_authGroups.add(childName);
    }
    if (collectionName.compare("authPrograms") == 0) {
        CatalogType *exists = m_authPrograms.get(childName);
        if (exists)
            return NULL;
        return m_authPrograms.add(childName);
    }
    if (collectionName.compare("statements") == 0) {
        CatalogType *exists = m_statements.get(childName);
        if (exists)
            return NULL;
        return m_statements.add(childName);
    }
    if (collectionName.compare("parameters") == 0) {
        CatalogType *exists = m_parameters.get(childName);
        if (exists)
            return NULL;
        return m_parameters.add(childName);
    }
    return NULL;
}

CatalogType * Procedure::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("authUsers") == 0)
        return m_authUsers.get(childName);
    if (collectionName.compare("authGroups") == 0)
        return m_authGroups.get(childName);
    if (collectionName.compare("authPrograms") == 0)
        return m_authPrograms.get(childName);
    if (collectionName.compare("statements") == 0)
        return m_statements.get(childName);
    if (collectionName.compare("parameters") == 0)
        return m_parameters.get(childName);
    return NULL;
}

void Procedure::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("authUsers") == 0)
        return m_authUsers.remove(childName);
    if (collectionName.compare("authGroups") == 0)
        return m_authGroups.remove(childName);
    if (collectionName.compare("authPrograms") == 0)
        return m_authPrograms.remove(childName);
    if (collectionName.compare("statements") == 0)
        return m_statements.remove(childName);
    if (collectionName.compare("parameters") == 0)
        return m_parameters.remove(childName);
}

const string & Procedure::classname() const {
    return m_classname;
}

const CatalogMap<UserRef> & Procedure::authUsers() const {
    return m_authUsers;
}

const CatalogMap<GroupRef> & Procedure::authGroups() const {
    return m_authGroups;
}

bool Procedure::readonly() const {
    return m_readonly;
}

bool Procedure::singlepartition() const {
    return m_singlepartition;
}

bool Procedure::everysite() const {
    return m_everysite;
}

bool Procedure::systemproc() const {
    return m_systemproc;
}

bool Procedure::hasjava() const {
    return m_hasjava;
}

const Table * Procedure::partitiontable() const {
    return dynamic_cast<Table*>(m_partitiontable);
}

const Column * Procedure::partitioncolumn() const {
    return dynamic_cast<Column*>(m_partitioncolumn);
}

int32_t Procedure::partitionparameter() const {
    return m_partitionparameter;
}

const CatalogMap<AuthProgram> & Procedure::authPrograms() const {
    return m_authPrograms;
}

const CatalogMap<Statement> & Procedure::statements() const {
    return m_statements;
}

const CatalogMap<ProcParameter> & Procedure::parameters() const {
    return m_parameters;
}

