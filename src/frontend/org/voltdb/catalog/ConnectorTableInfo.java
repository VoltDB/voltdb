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
 * Per-export connector table configuration
 */
public class ConnectorTableInfo extends CatalogType {

    boolean m_appendOnly;

    @Override
    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("table", null);
        m_fields.put("appendOnly", m_appendOnly);
    }

    @Override
    void update() {
        m_appendOnly = (Boolean) m_fields.get("appendOnly");
    }

    /** GETTER: Reference to the table being amended */
    public Table getTable() {
        Object o = getField("table");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("table", retval);
            return retval;
        }
        return (Table) o;
    }

    /** GETTER: True if this table is an append-only table for export. */
    public boolean getAppendonly() {
        return m_appendOnly;
    }

    /** SETTER: Reference to the table being amended */
    public void setTable(Table value) {
        m_fields.put("table", value);
    }

    /** SETTER: True if this table is an append-only table for export. */
    public void setAppendonly(boolean value) {
        m_appendOnly = value; m_fields.put("appendOnly", value);
    }

}
