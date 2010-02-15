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
 * A single host participating in the cluster
 */
public class Host extends CatalogType {

    String m_ipaddr = new String();

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("ipaddr", m_ipaddr);
    }

    void update() {
        m_ipaddr = (String) m_fields.get("ipaddr");
    }

    /** GETTER: The ip address or hostname of the host */
    public String getIpaddr() {
        return m_ipaddr;
    }

    /** SETTER: The ip address or hostname of the host */
    public void setIpaddr(String value) {
        m_ipaddr = value; m_fields.put("ipaddr", value);
    }

}
