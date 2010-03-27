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
 * A physical execution context for the system
 */
public class Site extends CatalogType {

    boolean m_isexec;
    int m_initiatorid;
    boolean m_isUp;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("isexec", m_isexec);
        m_fields.put("host", null);
        m_fields.put("partition", null);
        m_fields.put("initiatorid", m_initiatorid);
        m_fields.put("isUp", m_isUp);
    }

    void update() {
        m_isexec = (Boolean) m_fields.get("isexec");
        m_initiatorid = (Integer) m_fields.get("initiatorid");
        m_isUp = (Boolean) m_fields.get("isUp");
    }

    /** GETTER: Does the site execute workunits? */
    public boolean getIsexec() {
        return m_isexec;
    }

    /** GETTER: Which host does the site belong to? */
    public Host getHost() {
        Object o = getField("host");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Host retval = (Host) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("host", retval);
            return retval;
        }
        return (Host) o;
    }

    /** GETTER: Which logical data partition does this host process? */
    public Partition getPartition() {
        Object o = getField("partition");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Partition retval = (Partition) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("partition", retval);
            return retval;
        }
        return (Partition) o;
    }

    /** GETTER: If the site is an initiator, this is its tightly packed id */
    public int getInitiatorid() {
        return m_initiatorid;
    }

    /** GETTER: Is the site up? */
    public boolean getIsup() {
        return m_isUp;
    }

    /** SETTER: Does the site execute workunits? */
    public void setIsexec(boolean value) {
        m_isexec = value; m_fields.put("isexec", value);
    }

    /** SETTER: Which host does the site belong to? */
    public void setHost(Host value) {
        m_fields.put("host", value);
    }

    /** SETTER: Which logical data partition does this host process? */
    public void setPartition(Partition value) {
        m_fields.put("partition", value);
    }

    /** SETTER: If the site is an initiator, this is its tightly packed id */
    public void setInitiatorid(int value) {
        m_initiatorid = value; m_fields.put("initiatorid", value);
    }

    /** SETTER: Is the site up? */
    public void setIsup(boolean value) {
        m_isUp = value; m_fields.put("isUp", value);
    }

}
