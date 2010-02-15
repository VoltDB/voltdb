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

#ifndef  CATALOG_CLUSTER_H_
#define  CATALOG_CLUSTER_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Database;
class Host;
class Site;
class Partition;
/**
 * A set of connected hosts running one or more database application contexts
 */
class Cluster : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Cluster>;

protected:
    Cluster(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogMap<Database> m_databases;
    CatalogMap<Host> m_hosts;
    CatalogMap<Site> m_sites;
    CatalogMap<Partition> m_partitions;
    std::string m_leaderaddress;
    int32_t m_localepoch;
    bool m_securityEnabled;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;

public:
    /** GETTER: The set of databases the cluster is running */
    const CatalogMap<Database> & databases() const;
    /** GETTER: The set of host that belong to this cluster */
    const CatalogMap<Host> & hosts() const;
    /** GETTER: The set of physical execution contexts executing on this cluster */
    const CatalogMap<Site> & sites() const;
    /** GETTER: The set of logical partitions in this cluster */
    const CatalogMap<Partition> & partitions() const;
    /** GETTER: The ip or hostname of the cluster 'leader' - see docs for details */
    const std::string & leaderaddress() const;
    /** GETTER: The number of seconds since the epoch that we're calling our local epoch */
    int32_t localepoch() const;
    /** GETTER: Whether security and authentication should be enabled/disabled */
    bool securityEnabled() const;
};

} // namespace catalog

#endif //  CATALOG_CLUSTER_H_
