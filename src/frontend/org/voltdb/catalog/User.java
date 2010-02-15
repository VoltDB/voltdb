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

public class User extends CatalogType {

    CatalogMap<GroupRef> m_groups;
    boolean m_sysproc;
    boolean m_adhoc;
    String m_shadowPassword = new String();

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_groups = new CatalogMap<GroupRef>(catalog, this, path + "/" + "groups", GroupRef.class);
        m_childCollections.put("groups", m_groups);
        m_fields.put("sysproc", m_sysproc);
        m_fields.put("adhoc", m_adhoc);
        m_fields.put("shadowPassword", m_shadowPassword);
    }

    void update() {
        m_sysproc = (Boolean) m_fields.get("sysproc");
        m_adhoc = (Boolean) m_fields.get("adhoc");
        m_shadowPassword = (String) m_fields.get("shadowPassword");
    }

    public CatalogMap<GroupRef> getGroups() {
        return m_groups;
    }

    /** GETTER: Can invoke system procedures */
    public boolean getSysproc() {
        return m_sysproc;
    }

    /** GETTER: Can invoke the adhoc system procedure */
    public boolean getAdhoc() {
        return m_adhoc;
    }

    /** GETTER: SHA-1 double hashed hex encoded version of the password */
    public String getShadowpassword() {
        return m_shadowPassword;
    }

    /** SETTER: Can invoke system procedures */
    public void setSysproc(boolean value) {
        m_sysproc = value; m_fields.put("sysproc", value);
    }

    /** SETTER: Can invoke the adhoc system procedure */
    public void setAdhoc(boolean value) {
        m_adhoc = value; m_fields.put("adhoc", value);
    }

    /** SETTER: SHA-1 double hashed hex encoded version of the password */
    public void setShadowpassword(String value) {
        m_shadowPassword = value; m_fields.put("shadowPassword", value);
    }

}
