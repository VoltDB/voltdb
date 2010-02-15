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
 * A table (relation) in the database
 */
public class Table extends CatalogType {

    CatalogMap<Column> m_columns;
    CatalogMap<Index> m_indexes;
    CatalogMap<Constraint> m_constraints;
    boolean m_isreplicated;
    int m_estimatedtuplecount;
    CatalogMap<MaterializedViewInfo> m_views;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_columns = new CatalogMap<Column>(catalog, this, path + "/" + "columns", Column.class);
        m_childCollections.put("columns", m_columns);
        m_indexes = new CatalogMap<Index>(catalog, this, path + "/" + "indexes", Index.class);
        m_childCollections.put("indexes", m_indexes);
        m_constraints = new CatalogMap<Constraint>(catalog, this, path + "/" + "constraints", Constraint.class);
        m_childCollections.put("constraints", m_constraints);
        m_fields.put("isreplicated", m_isreplicated);
        m_fields.put("partitioncolumn", null);
        m_fields.put("estimatedtuplecount", m_estimatedtuplecount);
        m_views = new CatalogMap<MaterializedViewInfo>(catalog, this, path + "/" + "views", MaterializedViewInfo.class);
        m_childCollections.put("views", m_views);
        m_fields.put("materializer", null);
    }

    void update() {
        m_isreplicated = (Boolean) m_fields.get("isreplicated");
        m_estimatedtuplecount = (Integer) m_fields.get("estimatedtuplecount");
    }

    /** GETTER: The set of columns in the table */
    public CatalogMap<Column> getColumns() {
        return m_columns;
    }

    /** GETTER: The set of indexes on the columns in the table */
    public CatalogMap<Index> getIndexes() {
        return m_indexes;
    }

    /** GETTER: The set of constraints on the table */
    public CatalogMap<Constraint> getConstraints() {
        return m_constraints;
    }

    /** GETTER: Is the table replicated? */
    public boolean getIsreplicated() {
        return m_isreplicated;
    }

    /** GETTER: On which column is the table partitioned */
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

    /** GETTER: A rough estimate of the number of tuples in the table; used for planning */
    public int getEstimatedtuplecount() {
        return m_estimatedtuplecount;
    }

    /** GETTER: Information about materialized views based on this table's content */
    public CatalogMap<MaterializedViewInfo> getViews() {
        return m_views;
    }

    /** GETTER: If this is a materialized view, this field stores the source table */
    public Table getMaterializer() {
        Object o = getField("materializer");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("materializer", retval);
            return retval;
        }
        return (Table) o;
    }

    /** SETTER: Is the table replicated? */
    public void setIsreplicated(boolean value) {
        m_isreplicated = value; m_fields.put("isreplicated", value);
    }

    /** SETTER: On which column is the table partitioned */
    public void setPartitioncolumn(Column value) {
        m_fields.put("partitioncolumn", value);
    }

    /** SETTER: A rough estimate of the number of tuples in the table; used for planning */
    public void setEstimatedtuplecount(int value) {
        m_estimatedtuplecount = value; m_fields.put("estimatedtuplecount", value);
    }

    /** SETTER: If this is a materialized view, this field stores the source table */
    public void setMaterializer(Table value) {
        m_fields.put("materializer", value);
    }

}
