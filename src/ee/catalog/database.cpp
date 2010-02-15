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

#include "database.h"
#include "catalog.h"
#include "user.h"
#include "group.h"
#include "connector.h"
#include "procedure.h"
#include "snapshotschedule.h"
#include "program.h"
#include "table.h"

using namespace catalog;
using namespace std;

Database::Database(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_users(catalog, this, path + "/" + "users"), m_groups(catalog, this, path + "/" + "groups"), m_tables(catalog, this, path + "/" + "tables"), m_programs(catalog, this, path + "/" + "programs"), m_procedures(catalog, this, path + "/" + "procedures"), m_connectors(catalog, this, path + "/" + "connectors"), m_snapshotSchedule(catalog, this, path + "/" + "snapshotSchedule")
{
    CatalogValue value;
    m_fields["schema"] = value;
    m_childCollections["users"] = &m_users;
    m_childCollections["groups"] = &m_groups;
    m_childCollections["tables"] = &m_tables;
    m_childCollections["programs"] = &m_programs;
    m_childCollections["procedures"] = &m_procedures;
    m_childCollections["connectors"] = &m_connectors;
    m_childCollections["snapshotSchedule"] = &m_snapshotSchedule;
}

void Database::update() {
    m_schema = m_fields["schema"].strValue.c_str();
}

CatalogType * Database::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("users") == 0) {
        CatalogType *exists = m_users.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_users.add(childName);
    }
    if (collectionName.compare("groups") == 0) {
        CatalogType *exists = m_groups.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_groups.add(childName);
    }
    if (collectionName.compare("tables") == 0) {
        CatalogType *exists = m_tables.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_tables.add(childName);
    }
    if (collectionName.compare("programs") == 0) {
        CatalogType *exists = m_programs.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_programs.add(childName);
    }
    if (collectionName.compare("procedures") == 0) {
        CatalogType *exists = m_procedures.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_procedures.add(childName);
    }
    if (collectionName.compare("connectors") == 0) {
        CatalogType *exists = m_connectors.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_connectors.add(childName);
    }
    if (collectionName.compare("snapshotSchedule") == 0) {
        CatalogType *exists = m_snapshotSchedule.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_snapshotSchedule.add(childName);
    }
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * Database::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("users") == 0)
        return m_users.get(childName);
    if (collectionName.compare("groups") == 0)
        return m_groups.get(childName);
    if (collectionName.compare("tables") == 0)
        return m_tables.get(childName);
    if (collectionName.compare("programs") == 0)
        return m_programs.get(childName);
    if (collectionName.compare("procedures") == 0)
        return m_procedures.get(childName);
    if (collectionName.compare("connectors") == 0)
        return m_connectors.get(childName);
    if (collectionName.compare("snapshotSchedule") == 0)
        return m_snapshotSchedule.get(childName);
    return NULL;
}

const string & Database::schema() const {
    return m_schema;
}

const CatalogMap<User> & Database::users() const {
    return m_users;
}

const CatalogMap<Group> & Database::groups() const {
    return m_groups;
}

const CatalogMap<Table> & Database::tables() const {
    return m_tables;
}

const CatalogMap<Program> & Database::programs() const {
    return m_programs;
}

const CatalogMap<Procedure> & Database::procedures() const {
    return m_procedures;
}

const CatalogMap<Connector> & Database::connectors() const {
    return m_connectors;
}

const CatalogMap<SnapshotSchedule> & Database::snapshotSchedule() const {
    return m_snapshotSchedule;
}

