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

package org.voltdb.security;

import org.voltdb.client.ClientAuthScheme;

public abstract class AuthenticationRequest {

    protected String m_authenticatedUser;
    protected Exception m_authException;
    protected volatile boolean m_done;

    /**
     * Perform the authentication request.
     * <p>
     * <code>fromAddress</code> is used for logging success/failure
     * of the request. It should be 'the same' for repeated requests
     * from the same source, so it is inadvisable to include a TCP
     * port number, for example.
     * <p>
     * The implementation handles logging of all successful and
     * failed authentication attempts. Logging may however
     * be rate-limited.
     *
     * @param scheme is the type of hash scheme
     * @param fromAddress is the remote IP address of this authentication request
     * @return true if authenticated, false if not
     * @throws {@link IllegalStateException} if this request was already made
     */
    public boolean authenticate(ClientAuthScheme scheme, String fromAddress) {
        if (m_done) throw new IllegalStateException("this authentication request has a result");
        boolean authenticated = authenticateImpl(scheme, fromAddress);
        m_done = true;
        return authenticated;
    }

    /**
     * Authentication provider implementation of the request
     * <p>
     * See {@link #authenticate} for the use of <code>fromAddress</code>
     * <p>
     * The implementation must log authentication success or
     * failure. Logging may be rate-limited, at the discretion of
     * the implementation, possibly depending on expectations that
     * a failure will recur on immediate retry by a client.
     *
     * @return true if authenticated, false if not
      */
    protected abstract boolean authenticateImpl(ClientAuthScheme scheme, String fromAddress);

    /**
     * Following the completion of an <code>authenticate</code> call,
     * returns the authenticated user name if authentication succeeded,
     * otherwise returns null.
     *
     * @return authenticated user name, null if authentication failed
     */
    public final String getAuthenticatedUser() {
        if (!m_done) throw new IllegalStateException("this authentication request has not been made yet");
        return m_authenticatedUser;
    }

    /**
     * If the <code>authenicate</code> call failed because of some unexpected
     * exception, then the call returns the underlying provider exception,
     * otherwise it returns null.
     * <p>
     * Not all failures are reported via an exception.
     *
     * @return provider exception, null if none
     */
    public final Exception getAuthenticationFailureException() {
        if (!m_done) throw new IllegalStateException("this authentication request has not been made yet");
        return m_authException;
    }
}
