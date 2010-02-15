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

#ifndef  CATALOG_DATABASE_H_
#define  CATALOG_DATABASE_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class User;
class Group;
class Table;
class Program;
class Procedure;
class Connector;
class SnapshotSchedule;
/**
 * A set of schema, procedures and other metadata that together comprise an application
 */
class Database : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Database>;

protected:
    Database(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    std::string m_schema;
    CatalogMap<User> m_users;
    CatalogMap<Group> m_groups;
    CatalogMap<Table> m_tables;
    CatalogMap<Program> m_programs;
    CatalogMap<Procedure> m_procedures;
    CatalogMap<Connector> m_connectors;
    CatalogMap<SnapshotSchedule> m_snapshotSchedule;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;

public:
    /** GETTER: Full SQL DDL for the database's schema */
    const std::string & schema() const;
    /** GETTER: The set of users */
    const CatalogMap<User> & users() const;
    /** GETTER: The set of groups */
    const CatalogMap<Group> & groups() const;
    /** GETTER: The set of Tables for the database */
    const CatalogMap<Table> & tables() const;
    /** GETTER: The set of programs allowed to access this database */
    const CatalogMap<Program> & programs() const;
    /** GETTER: The set of stored procedures/transactions for this database */
    const CatalogMap<Procedure> & procedures() const;
    /** GETTER: Export connector configuration */
    const CatalogMap<Connector> & connectors() const;
    /** GETTER: Schedule for automated snapshots */
    const CatalogMap<SnapshotSchedule> & snapshotSchedule() const;
};

} // namespace catalog

#endif //  CATALOG_DATABASE_H_
