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

#include "cluster.h"
#include "catalog.h"
#include "partition.h"
#include "database.h"
#include "host.h"
#include "site.h"

using namespace catalog;
using namespace std;

Cluster::Cluster(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_databases(catalog, this, path + "/" + "databases"), m_hosts(catalog, this, path + "/" + "hosts"), m_sites(catalog, this, path + "/" + "sites"), m_partitions(catalog, this, path + "/" + "partitions")
{
    CatalogValue value;
    m_childCollections["databases"] = &m_databases;
    m_childCollections["hosts"] = &m_hosts;
    m_childCollections["sites"] = &m_sites;
    m_childCollections["partitions"] = &m_partitions;
    m_fields["leaderaddress"] = value;
    m_fields["localepoch"] = value;
    m_fields["securityEnabled"] = value;
}

void Cluster::update() {
    m_leaderaddress = m_fields["leaderaddress"].strValue.c_str();
    m_localepoch = m_fields["localepoch"].intValue;
    m_securityEnabled = m_fields["securityEnabled"].intValue;
}

CatalogType * Cluster::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("databases") == 0) {
        CatalogType *exists = m_databases.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_databases.add(childName);
    }
    if (collectionName.compare("hosts") == 0) {
        CatalogType *exists = m_hosts.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_hosts.add(childName);
    }
    if (collectionName.compare("sites") == 0) {
        CatalogType *exists = m_sites.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_sites.add(childName);
    }
    if (collectionName.compare("partitions") == 0) {
        CatalogType *exists = m_partitions.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_partitions.add(childName);
    }
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * Cluster::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("databases") == 0)
        return m_databases.get(childName);
    if (collectionName.compare("hosts") == 0)
        return m_hosts.get(childName);
    if (collectionName.compare("sites") == 0)
        return m_sites.get(childName);
    if (collectionName.compare("partitions") == 0)
        return m_partitions.get(childName);
    return NULL;
}

const CatalogMap<Database> & Cluster::databases() const {
    return m_databases;
}

const CatalogMap<Host> & Cluster::hosts() const {
    return m_hosts;
}

const CatalogMap<Site> & Cluster::sites() const {
    return m_sites;
}

const CatalogMap<Partition> & Cluster::partitions() const {
    return m_partitions;
}

const string & Cluster::leaderaddress() const {
    return m_leaderaddress;
}

int32_t Cluster::localepoch() const {
    return m_localepoch;
}

bool Cluster::securityEnabled() const {
    return m_securityEnabled;
}

