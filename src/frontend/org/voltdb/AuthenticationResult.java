/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.client.ClientAuthScheme;

//This is returned after authentication for convenience the client and other information is built and returned.
public class AuthenticationResult {
    final public boolean m_adminMode; //Needed to release connection
    final public String m_user;
    final public String m_message;
    final public String[] m_perms;
    final public AuthUser m_authUser;
    final public ClientAuthScheme m_scheme;
    final public boolean m_authenticated;

    //Is user authenticated or not depends on client connection there or not.
    public AuthenticationResult(boolean authenticated, ClientAuthScheme scheme, boolean adminMode, String user, String message) {
        m_adminMode = adminMode;
        m_authenticated = authenticated;
        m_scheme = scheme;
        //null user when security is disabled.
        if (!getAuthSystem().isSecurityEnabled()) {
            m_user = null;
        } else {
            m_user = user;
        }
        m_message = message;
        m_perms = getAuthSystem().getUserPermissionList(m_user);
        m_authUser = getAuthSystem().getUser(user);
    }

    public boolean isAuthenticated() {
        return m_authenticated;
    }

    private AuthSystem getAuthSystem() {
        return VoltDB.instance().getCatalogContext().authSystem;
    }
}
