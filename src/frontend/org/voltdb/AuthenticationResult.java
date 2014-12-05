/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.client.Client;

//This is returned after authentication for convenience the client and other information is built and returned.
public class AuthenticationResult {
    final public Client m_client;
    final public boolean m_adminMode; //Needed to release connection
    final public String m_user;
    final public String m_message;
    final private boolean m_authenticated;

    public AuthenticationResult(Client client, boolean adminMode, String user, String message) {
        this.m_adminMode = adminMode;
        this.m_client = client;
        this.m_user = user;
        this.m_message = message;
        this.m_authenticated = (m_client != null);
    }

    public boolean isAuthenticated() {
        return m_authenticated;
    }
}
