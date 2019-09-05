/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.security;

import org.voltdb.client.ClientAuthScheme;

public abstract class AuthenticationRequest {

    protected String m_authenticatedUser;
    protected volatile boolean m_done = false;
    protected Exception m_authenticationFailure = null;

    /**
     * Perform the authentication request
     * @param scheme is the type of Hash scheme
     * @param fromAddress is the remote IP address of this authenticate request
     * @return true if authenticated, false if not
     * @throws {@link IllegalStateException} if this request was already made
     */
    public boolean authenticate(ClientAuthScheme scheme, String fromAddress) {
        if (m_done) throw new IllegalStateException("this authentication request has a result");
        boolean authenticated = false;
        try {
            authenticated = authenticateImpl(scheme, fromAddress);
        } catch (Exception ex) {
            m_authenticationFailure = ex;
        }
        finally {
            m_done = true;
        }
        return authenticated;
    }

    /**
     * Authentication provider implementation of the request
     * @return true if authenticated, false if not
     * @throws Exception raised by the provider (if any)
     */
    protected abstract boolean authenticateImpl(ClientAuthScheme scheme, String fromAddress) throws Exception;

    /**
     * if the request is successful it returns the authenticated user name
     * @return if the request is successful it returns the authenticated user name null if not
     */
    public final String getAuthenticatedUser() {
        if (!m_done) throw new IllegalStateException("this authentication request has not been made yet");
        return m_authenticatedUser;
    }

    /**
     * if the request fails it returns the underlying provider exception
     * @return if the request fails it returns the underlying provider exception, null if it succeeded
     */
    public final Exception getAuthenticationFailureException() {
        if (!m_done) throw new IllegalStateException("this authentication request has not been made yet");
        return m_authenticationFailure;
    }

}
