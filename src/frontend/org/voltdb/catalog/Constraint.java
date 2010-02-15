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
 * A constraint on a database table
 */
public class Constraint extends CatalogType {

    int m_type;
    String m_oncommit = new String();
    CatalogMap<ColumnRef> m_foreignkeycols;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("type", m_type);
        m_fields.put("oncommit", m_oncommit);
        m_fields.put("index", null);
        m_fields.put("foreignkeytable", null);
        m_foreignkeycols = new CatalogMap<ColumnRef>(catalog, this, path + "/" + "foreignkeycols", ColumnRef.class);
        m_childCollections.put("foreignkeycols", m_foreignkeycols);
    }

    void update() {
        m_type = (Integer) m_fields.get("type");
        m_oncommit = (String) m_fields.get("oncommit");
    }

    /** GETTER: The type of constraint */
    public int getType() {
        return m_type;
    }

    /** GETTER: (currently unused) */
    public String getOncommit() {
        return m_oncommit;
    }

    /** GETTER: The index used by this constraint (if needed) */
    public Index getIndex() {
        Object o = getField("index");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Index retval = (Index) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("index", retval);
            return retval;
        }
        return (Index) o;
    }

    /** GETTER: The table referenced by the foreign key (if needed) */
    public Table getForeignkeytable() {
        Object o = getField("foreignkeytable");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("foreignkeytable", retval);
            return retval;
        }
        return (Table) o;
    }

    /** GETTER: The columns in the foreign table referenced by the constraint (if needed) */
    public CatalogMap<ColumnRef> getForeignkeycols() {
        return m_foreignkeycols;
    }

    /** SETTER: The type of constraint */
    public void setType(int value) {
        m_type = value; m_fields.put("type", value);
    }

    /** SETTER: (currently unused) */
    public void setOncommit(String value) {
        m_oncommit = value; m_fields.put("oncommit", value);
    }

    /** SETTER: The index used by this constraint (if needed) */
    public void setIndex(Index value) {
        m_fields.put("index", value);
    }

    /** SETTER: The table referenced by the foreign key (if needed) */
    public void setForeignkeytable(Table value) {
        m_fields.put("foreignkeytable", value);
    }

}
