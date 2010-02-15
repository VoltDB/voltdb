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
 * A parameter for a parameterized SQL statement
 */
public class StmtParameter extends CatalogType {

    int m_sqltype;
    int m_javatype;
    int m_index;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("sqltype", m_sqltype);
        m_fields.put("javatype", m_javatype);
        m_fields.put("index", m_index);
        m_fields.put("procparameter", null);
    }

    void update() {
        m_sqltype = (Integer) m_fields.get("sqltype");
        m_javatype = (Integer) m_fields.get("javatype");
        m_index = (Integer) m_fields.get("index");
    }

    /** GETTER: The SQL type of the parameter (int/float/date/etc) */
    public int getSqltype() {
        return m_sqltype;
    }

    /** GETTER: The Java class of the parameter (int/float/date/etc) */
    public int getJavatype() {
        return m_javatype;
    }

    /** GETTER: The index of the parameter in the set of statement parameters */
    public int getIndex() {
        return m_index;
    }

    /** GETTER: Reference back to original input parameter */
    public ProcParameter getProcparameter() {
        Object o = getField("procparameter");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            ProcParameter retval = (ProcParameter) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("procparameter", retval);
            return retval;
        }
        return (ProcParameter) o;
    }

    /** SETTER: The SQL type of the parameter (int/float/date/etc) */
    public void setSqltype(int value) {
        m_sqltype = value; m_fields.put("sqltype", value);
    }

    /** SETTER: The Java class of the parameter (int/float/date/etc) */
    public void setJavatype(int value) {
        m_javatype = value; m_fields.put("javatype", value);
    }

    /** SETTER: The index of the parameter in the set of statement parameters */
    public void setIndex(int value) {
        m_index = value; m_fields.put("index", value);
    }

    /** SETTER: Reference back to original input parameter */
    public void setProcparameter(ProcParameter value) {
        m_fields.put("procparameter", value);
    }

}
