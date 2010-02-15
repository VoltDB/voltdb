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
 * Metadata for a parameter to a stored procedure
 */
public class ProcParameter extends CatalogType {

    int m_type;
    boolean m_isarray;
    int m_index;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("type", m_type);
        m_fields.put("isarray", m_isarray);
        m_fields.put("index", m_index);
    }

    void update() {
        m_type = (Integer) m_fields.get("type");
        m_isarray = (Boolean) m_fields.get("isarray");
        m_index = (Integer) m_fields.get("index");
    }

    /** GETTER: The data type for the parameter (int/float/date/etc) */
    public int getType() {
        return m_type;
    }

    /** GETTER: Is the parameter an array of value */
    public boolean getIsarray() {
        return m_isarray;
    }

    /** GETTER: The index of the parameter within the list of parameters for the stored procedure */
    public int getIndex() {
        return m_index;
    }

    /** SETTER: The data type for the parameter (int/float/date/etc) */
    public void setType(int value) {
        m_type = value; m_fields.put("type", value);
    }

    /** SETTER: Is the parameter an array of value */
    public void setIsarray(boolean value) {
        m_isarray = value; m_fields.put("isarray", value);
    }

    /** SETTER: The index of the parameter within the list of parameters for the stored procedure */
    public void setIndex(int value) {
        m_index = value; m_fields.put("index", value);
    }

}
