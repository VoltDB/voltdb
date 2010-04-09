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
 * A stored procedure (transaction) in the system
 */
public class Procedure extends CatalogType {

    String m_classname = new String();
    CatalogMap<UserRef> m_authUsers;
    CatalogMap<GroupRef> m_authGroups;
    boolean m_readonly;
    boolean m_singlepartition;
    boolean m_everysite;
    boolean m_systemproc;
    boolean m_hasjava;
    int m_partitionparameter;
    CatalogMap<AuthProgram> m_authPrograms;
    CatalogMap<Statement> m_statements;
    CatalogMap<ProcParameter> m_parameters;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("classname", m_classname);
        m_authUsers = new CatalogMap<UserRef>(catalog, this, path + "/" + "authUsers", UserRef.class);
        m_childCollections.put("authUsers", m_authUsers);
        m_authGroups = new CatalogMap<GroupRef>(catalog, this, path + "/" + "authGroups", GroupRef.class);
        m_childCollections.put("authGroups", m_authGroups);
        m_fields.put("readonly", m_readonly);
        m_fields.put("singlepartition", m_singlepartition);
        m_fields.put("everysite", m_everysite);
        m_fields.put("systemproc", m_systemproc);
        m_fields.put("hasjava", m_hasjava);
        m_fields.put("partitiontable", null);
        m_fields.put("partitioncolumn", null);
        m_fields.put("partitionparameter", m_partitionparameter);
        m_authPrograms = new CatalogMap<AuthProgram>(catalog, this, path + "/" + "authPrograms", AuthProgram.class);
        m_childCollections.put("authPrograms", m_authPrograms);
        m_statements = new CatalogMap<Statement>(catalog, this, path + "/" + "statements", Statement.class);
        m_childCollections.put("statements", m_statements);
        m_parameters = new CatalogMap<ProcParameter>(catalog, this, path + "/" + "parameters", ProcParameter.class);
        m_childCollections.put("parameters", m_parameters);
    }

    void update() {
        m_classname = (String) m_fields.get("classname");
        m_readonly = (Boolean) m_fields.get("readonly");
        m_singlepartition = (Boolean) m_fields.get("singlepartition");
        m_everysite = (Boolean) m_fields.get("everysite");
        m_systemproc = (Boolean) m_fields.get("systemproc");
        m_hasjava = (Boolean) m_fields.get("hasjava");
        m_partitionparameter = (Integer) m_fields.get("partitionparameter");
    }

    /** GETTER: The full class name for the Java class for this procedure */
    public String getClassname() {
        return m_classname;
    }

    /** GETTER: Users authorized to invoke this procedure */
    public CatalogMap<UserRef> getAuthusers() {
        return m_authUsers;
    }

    /** GETTER: Groups authorized to invoke this procedure */
    public CatalogMap<GroupRef> getAuthgroups() {
        return m_authGroups;
    }

    /** GETTER: Can the stored procedure modify data */
    public boolean getReadonly() {
        return m_readonly;
    }

    /** GETTER: Does the stored procedure need data on more than one partition? */
    public boolean getSinglepartition() {
        return m_singlepartition;
    }

    /** GETTER: Does the stored procedure as a single procedure txn at every site? */
    public boolean getEverysite() {
        return m_everysite;
    }

    /** GETTER: Is this procedure an internal system procedure? */
    public boolean getSystemproc() {
        return m_systemproc;
    }

    /** GETTER: Is this a full java stored procedure or is it just a single stmt? */
    public boolean getHasjava() {
        return m_hasjava;
    }

    /** GETTER: Which table contains the partition column for this procedure? */
    public Table getPartitiontable() {
        Object o = getField("partitiontable");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("partitiontable", retval);
            return retval;
        }
        return (Table) o;
    }

    /** GETTER: Which column in the partitioned table is this procedure mapped on? */
    public Column getPartitioncolumn() {
        Object o = getField("partitioncolumn");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Column retval = (Column) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("partitioncolumn", retval);
            return retval;
        }
        return (Column) o;
    }

    /** GETTER: Which parameter identifies the partition column? */
    public int getPartitionparameter() {
        return m_partitionparameter;
    }

    /** GETTER: The set of authorized programs for this procedure (users) */
    public CatalogMap<AuthProgram> getAuthprograms() {
        return m_authPrograms;
    }

    /** GETTER: The set of SQL statements this procedure may call */
    public CatalogMap<Statement> getStatements() {
        return m_statements;
    }

    /** GETTER: The set of parameters to this stored procedure */
    public CatalogMap<ProcParameter> getParameters() {
        return m_parameters;
    }

    /** SETTER: The full class name for the Java class for this procedure */
    public void setClassname(String value) {
        m_classname = value; m_fields.put("classname", value);
    }

    /** SETTER: Can the stored procedure modify data */
    public void setReadonly(boolean value) {
        m_readonly = value; m_fields.put("readonly", value);
    }

    /** SETTER: Does the stored procedure need data on more than one partition? */
    public void setSinglepartition(boolean value) {
        m_singlepartition = value; m_fields.put("singlepartition", value);
    }

    /** SETTER: Does the stored procedure as a single procedure txn at every site? */
    public void setEverysite(boolean value) {
        m_everysite = value; m_fields.put("everysite", value);
    }

    /** SETTER: Is this procedure an internal system procedure? */
    public void setSystemproc(boolean value) {
        m_systemproc = value; m_fields.put("systemproc", value);
    }

    /** SETTER: Is this a full java stored procedure or is it just a single stmt? */
    public void setHasjava(boolean value) {
        m_hasjava = value; m_fields.put("hasjava", value);
    }

    /** SETTER: Which table contains the partition column for this procedure? */
    public void setPartitiontable(Table value) {
        m_fields.put("partitiontable", value);
    }

    /** SETTER: Which column in the partitioned table is this procedure mapped on? */
    public void setPartitioncolumn(Column value) {
        m_fields.put("partitioncolumn", value);
    }

    /** SETTER: Which parameter identifies the partition column? */
    public void setPartitionparameter(int value) {
        m_partitionparameter = value; m_fields.put("partitionparameter", value);
    }

}
