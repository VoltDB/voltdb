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

package org.voltdb.client;

@SuppressWarnings("deprecation")
class ClientStatusListenerWrapper extends ClientStatusListenerExt {

    final ClientStatusListener m_csl;

    ClientStatusListenerWrapper(ClientStatusListener csl) {
        m_csl = csl;
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.ClientStatusListenerExt#connectionLost(java.lang.String, int, int, ClientStatusListenerExt.DisconnectCause)
     */
    @Override
    public void connectionLost(String hostname, int port, int connectionsLeft,
            ClientStatusListenerExt.DisconnectCause cause) {
        // TODO Auto-generated method stub
        m_csl.connectionLost(hostname, connectionsLeft);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.ClientStatusListenerExt#backpressure(boolean)
     */
    @Override
    public void backpressure(boolean status) {
        m_csl.backpressure(status);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.ClientStatusListenerExt#uncaughtException(org.voltdb.client.ProcedureCallback, org.voltdb.client.ClientResponse, java.lang.Throwable)
     */
    @Override
    public void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e) {
        m_csl.uncaughtException(callback, r, e);
    }
}
