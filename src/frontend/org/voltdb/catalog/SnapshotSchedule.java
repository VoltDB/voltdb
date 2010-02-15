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
 * A schedule for the database to follow when creating automated snapshots
 */
public class SnapshotSchedule extends CatalogType {

    String m_frequencyUnit = new String();
    int m_frequencyValue;
    int m_retain;
    String m_path = new String();
    String m_prefix = new String();

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("frequencyUnit", m_frequencyUnit);
        m_fields.put("frequencyValue", m_frequencyValue);
        m_fields.put("retain", m_retain);
        m_fields.put("path", m_path);
        m_fields.put("prefix", m_prefix);
    }

    void update() {
        m_frequencyUnit = (String) m_fields.get("frequencyUnit");
        m_frequencyValue = (Integer) m_fields.get("frequencyValue");
        m_retain = (Integer) m_fields.get("retain");
        m_path = (String) m_fields.get("path");
        m_prefix = (String) m_fields.get("prefix");
    }

    /** GETTER: Unit of time frequency is specified in */
    public String getFrequencyunit() {
        return m_frequencyUnit;
    }

    /** GETTER: Frequency in some unit */
    public int getFrequencyvalue() {
        return m_frequencyValue;
    }

    /** GETTER: How many snapshots to retain */
    public int getRetain() {
        return m_retain;
    }

    /** GETTER: Path where snapshots should be stored */
    public String getPath() {
        return m_path;
    }

    /** GETTER: Prefix for snapshot filenames */
    public String getPrefix() {
        return m_prefix;
    }

    /** SETTER: Unit of time frequency is specified in */
    public void setFrequencyunit(String value) {
        m_frequencyUnit = value; m_fields.put("frequencyUnit", value);
    }

    /** SETTER: Frequency in some unit */
    public void setFrequencyvalue(int value) {
        m_frequencyValue = value; m_fields.put("frequencyValue", value);
    }

    /** SETTER: How many snapshots to retain */
    public void setRetain(int value) {
        m_retain = value; m_fields.put("retain", value);
    }

    /** SETTER: Path where snapshots should be stored */
    public void setPath(String value) {
        m_path = value; m_fields.put("path", value);
    }

    /** SETTER: Prefix for snapshot filenames */
    public void setPrefix(String value) {
        m_prefix = value; m_fields.put("prefix", value);
    }

}
