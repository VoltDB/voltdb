/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.client.Client;

//This is returned after authentication for convenience the client and other information is built and returned.
public class AuthenticationResult {
    final public Client m_client;
    final public boolean m_adminMode; //Needed to release connection
    final public String m_user;
    final public String m_message;
    final private boolean m_authenticated;
    final public String[] m_perms;
    final public AuthUser m_authUser;

    //Is user authenticated or not depends on client connection there or not.
    public AuthenticationResult(Client client, boolean adminMode, String user, String message) {
        this.m_adminMode = adminMode;
        this.m_client = client;
        final AuthSystem authSystem = VoltDB.instance().getCatalogContext().authSystem;
        //null user when security is disabled.
        if (!authSystem.isSecurityEnabled()) {
            this.m_user = null;
        } else {
            this.m_user = user;
        }
        this.m_message = message;
        this.m_authenticated = (m_client != null);
        m_perms = authSystem.getUserPermissionList(m_user);
        m_authUser = authSystem.getUser(user);
    }

    public boolean isAuthenticated() {
        return m_authenticated;
    }
}
