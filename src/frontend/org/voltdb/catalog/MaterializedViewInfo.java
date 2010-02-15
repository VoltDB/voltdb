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
 * Information used to build and update a materialized view
 */
public class MaterializedViewInfo extends CatalogType {

    CatalogMap<ColumnRef> m_groupbycols;
    String m_predicate = new String();

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("dest", null);
        m_groupbycols = new CatalogMap<ColumnRef>(catalog, this, path + "/" + "groupbycols", ColumnRef.class);
        m_childCollections.put("groupbycols", m_groupbycols);
        m_fields.put("predicate", m_predicate);
    }

    void update() {
        m_predicate = (String) m_fields.get("predicate");
    }

    /** GETTER: The table which will be updated when the source table is updated */
    public Table getDest() {
        Object o = getField("dest");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("dest", retval);
            return retval;
        }
        return (Table) o;
    }

    /** GETTER: The columns involved in the group by of the aggregation */
    public CatalogMap<ColumnRef> getGroupbycols() {
        return m_groupbycols;
    }

    /** GETTER: A filtering predicate */
    public String getPredicate() {
        return m_predicate;
    }

    /** SETTER: The table which will be updated when the source table is updated */
    public void setDest(Table value) {
        m_fields.put("dest", value);
    }

    /** SETTER: A filtering predicate */
    public void setPredicate(String value) {
        m_predicate = value; m_fields.put("predicate", value);
    }

}
