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

package org.voltdb.catalog;

/**
 * A set of schema, procedures and other metadata that together comprise an application
 */
public class Database extends CatalogType {

    String m_schema = new String();
    CatalogMap<User> m_users;
    CatalogMap<Group> m_groups;
    CatalogMap<Table> m_tables;
    CatalogMap<Program> m_programs;
    CatalogMap<Procedure> m_procedures;
    CatalogMap<Connector> m_connectors;
    CatalogMap<SnapshotSchedule> m_snapshotSchedule;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("schema", m_schema);
        m_users = new CatalogMap<User>(catalog, this, path + "/" + "users", User.class);
        m_childCollections.put("users", m_users);
        m_groups = new CatalogMap<Group>(catalog, this, path + "/" + "groups", Group.class);
        m_childCollections.put("groups", m_groups);
        m_tables = new CatalogMap<Table>(catalog, this, path + "/" + "tables", Table.class);
        m_childCollections.put("tables", m_tables);
        m_programs = new CatalogMap<Program>(catalog, this, path + "/" + "programs", Program.class);
        m_childCollections.put("programs", m_programs);
        m_procedures = new CatalogMap<Procedure>(catalog, this, path + "/" + "procedures", Procedure.class);
        m_childCollections.put("procedures", m_procedures);
        m_connectors = new CatalogMap<Connector>(catalog, this, path + "/" + "connectors", Connector.class);
        m_childCollections.put("connectors", m_connectors);
        m_snapshotSchedule = new CatalogMap<SnapshotSchedule>(catalog, this, path + "/" + "snapshotSchedule", SnapshotSchedule.class);
        m_childCollections.put("snapshotSchedule", m_snapshotSchedule);
    }

    void update() {
        m_schema = (String) m_fields.get("schema");
    }

    /** GETTER: Full SQL DDL for the database's schema */
    public String getSchema() {
        return m_schema;
    }

    /** GETTER: The set of users */
    public CatalogMap<User> getUsers() {
        return m_users;
    }

    /** GETTER: The set of groups */
    public CatalogMap<Group> getGroups() {
        return m_groups;
    }

    /** GETTER: The set of Tables for the database */
    public CatalogMap<Table> getTables() {
        return m_tables;
    }

    /** GETTER: The set of programs allowed to access this database */
    public CatalogMap<Program> getPrograms() {
        return m_programs;
    }

    /** GETTER: The set of stored procedures/transactions for this database */
    public CatalogMap<Procedure> getProcedures() {
        return m_procedures;
    }

    /** GETTER: Export connector configuration */
    public CatalogMap<Connector> getConnectors() {
        return m_connectors;
    }

    /** GETTER: Schedule for automated snapshots */
    public CatalogMap<SnapshotSchedule> getSnapshotschedule() {
        return m_snapshotSchedule;
    }

    /** SETTER: Full SQL DDL for the database's schema */
    public void setSchema(String value) {
        m_schema = value; m_fields.put("schema", value);
    }

}
