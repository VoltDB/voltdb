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

public class ConnectorDestinationInfo extends CatalogType {

    String m_url = new String();
    String m_username = new String();
    String m_password = new String();

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("url", m_url);
        m_fields.put("username", m_username);
        m_fields.put("password", m_password);
    }

    void update() {
        m_url = (String) m_fields.get("url");
        m_username = (String) m_fields.get("username");
        m_password = (String) m_fields.get("password");
    }

    /** GETTER: The protocol and address of the destination */
    public String getUrl() {
        return m_url;
    }

    /** GETTER: Authentication name */
    public String getUsername() {
        return m_username;
    }

    public String getPassword() {
        return m_password;
    }

    /** SETTER: The protocol and address of the destination */
    public void setUrl(String value) {
        m_url = value; m_fields.put("url", value);
    }

    /** SETTER: Authentication name */
    public void setUsername(String value) {
        m_username = value; m_fields.put("username", value);
    }

    public void setPassword(String value) {
        m_password = value; m_fields.put("password", value);
    }

}
