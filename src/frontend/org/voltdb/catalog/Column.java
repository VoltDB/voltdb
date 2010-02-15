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
 * A table column
 */
public class Column extends CatalogType {

    int m_index;
    int m_type;
    int m_size;
    boolean m_nullable;
    String m_name = new String();
    String m_defaultvalue = new String();
    int m_defaulttype;
    CatalogMap<ConstraintRef> m_constraints;
    int m_aggregatetype;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("index", m_index);
        m_fields.put("type", m_type);
        m_fields.put("size", m_size);
        m_fields.put("nullable", m_nullable);
        m_fields.put("name", m_name);
        m_fields.put("defaultvalue", m_defaultvalue);
        m_fields.put("defaulttype", m_defaulttype);
        m_constraints = new CatalogMap<ConstraintRef>(catalog, this, path + "/" + "constraints", ConstraintRef.class);
        m_childCollections.put("constraints", m_constraints);
        m_fields.put("matview", null);
        m_fields.put("aggregatetype", m_aggregatetype);
        m_fields.put("matviewsource", null);
    }

    void update() {
        m_index = (Integer) m_fields.get("index");
        m_type = (Integer) m_fields.get("type");
        m_size = (Integer) m_fields.get("size");
        m_nullable = (Boolean) m_fields.get("nullable");
        m_name = (String) m_fields.get("name");
        m_defaultvalue = (String) m_fields.get("defaultvalue");
        m_defaulttype = (Integer) m_fields.get("defaulttype");
        m_aggregatetype = (Integer) m_fields.get("aggregatetype");
    }

    /** GETTER: The column's order in the table */
    public int getIndex() {
        return m_index;
    }

    /** GETTER: The type of the column (int/double/date/etc) */
    public int getType() {
        return m_type;
    }

    /** GETTER: (currently unused) */
    public int getSize() {
        return m_size;
    }

    /** GETTER: Is the column nullable? */
    public boolean getNullable() {
        return m_nullable;
    }

    /** GETTER: Name of column */
    public String getName() {
        return m_name;
    }

    /** GETTER: Default value of the column */
    public String getDefaultvalue() {
        return m_defaultvalue;
    }

    /** GETTER: Type of the default value of the column */
    public int getDefaulttype() {
        return m_defaulttype;
    }

    /** GETTER: Constraints that use this column */
    public CatalogMap<ConstraintRef> getConstraints() {
        return m_constraints;
    }

    /** GETTER: If part of a materialized view, ref of view info */
    public MaterializedViewInfo getMatview() {
        Object o = getField("matview");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            MaterializedViewInfo retval = (MaterializedViewInfo) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("matview", retval);
            return retval;
        }
        return (MaterializedViewInfo) o;
    }

    /** GETTER: If part of a materialized view, represents aggregate type */
    public int getAggregatetype() {
        return m_aggregatetype;
    }

    /** GETTER: If part of a materialized view, represents source column */
    public Column getMatviewsource() {
        Object o = getField("matviewsource");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Column retval = (Column) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("matviewsource", retval);
            return retval;
        }
        return (Column) o;
    }

    /** SETTER: The column's order in the table */
    public void setIndex(int value) {
        m_index = value; m_fields.put("index", value);
    }

    /** SETTER: The type of the column (int/double/date/etc) */
    public void setType(int value) {
        m_type = value; m_fields.put("type", value);
    }

    /** SETTER: (currently unused) */
    public void setSize(int value) {
        m_size = value; m_fields.put("size", value);
    }

    /** SETTER: Is the column nullable? */
    public void setNullable(boolean value) {
        m_nullable = value; m_fields.put("nullable", value);
    }

    /** SETTER: Name of column */
    public void setName(String value) {
        m_name = value; m_fields.put("name", value);
    }

    /** SETTER: Default value of the column */
    public void setDefaultvalue(String value) {
        m_defaultvalue = value; m_fields.put("defaultvalue", value);
    }

    /** SETTER: Type of the default value of the column */
    public void setDefaulttype(int value) {
        m_defaulttype = value; m_fields.put("defaulttype", value);
    }

    /** SETTER: If part of a materialized view, ref of view info */
    public void setMatview(MaterializedViewInfo value) {
        m_fields.put("matview", value);
    }

    /** SETTER: If part of a materialized view, represents aggregate type */
    public void setAggregatetype(int value) {
        m_aggregatetype = value; m_fields.put("aggregatetype", value);
    }

    /** SETTER: If part of a materialized view, represents source column */
    public void setMatviewsource(Column value) {
        m_fields.put("matviewsource", value);
    }

}
