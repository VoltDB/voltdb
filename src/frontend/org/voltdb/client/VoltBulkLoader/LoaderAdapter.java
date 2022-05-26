/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.client.VoltBulkLoader;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.Client2Impl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ProcCallException;

/**
 * This interface presents a common call interface to
 * Client/Client2 methods used by the VoltBulkLoader
 * and its PerPatitionTable class.
 *
 * No other code should be using this; it is subject
 * to change without notice, according to the needs
 * of the bulk loader.
 */
interface LoaderAdapter {
    ClientResponse callProcedure(String procname, Object... params) throws IOException, ProcCallException;
    void callProcedure(ProcedureCallback callback, String procname, Object... params) throws IOException;
    boolean autoconnectEnabled();
    boolean waitForTopology();
    int getPartitionForParameter(byte type, Object value);
    void drainClient() throws InterruptedException;
}

/**
 * Client interface
 */
class Client1LoaderAdapter implements LoaderAdapter {

    private final ClientImpl m_client;

    public Client1LoaderAdapter(ClientImpl client) {
        m_client = client;
    }

    @Override
    public ClientResponse callProcedure(String procname, Object... params) throws IOException, ProcCallException {
            return m_client.callProcedure(procname, params);
    }

    @Override
    public void callProcedure(ProcedureCallback callback, String procname, Object... params) throws IOException {
        m_client.callProcedure(callback, procname, params);
    }

    @Override
    public boolean autoconnectEnabled() {
        return m_client.isAutoReconnectEnabled();
    }

    @Override
    public boolean waitForTopology() {
        return m_client.waitForTopology(60_000);
    }

    @Override
    public int getPartitionForParameter(byte type, Object value) {
        return (int) m_client.getPartitionForParameter(type, value);
    }

    @Override
    public void drainClient() throws InterruptedException {
        m_client.drain();
    }
}

/**
 * Client2 Interface
 */
class Client2LoaderAdapter implements LoaderAdapter {

    private final Client2Impl m_client2;

    public Client2LoaderAdapter(Client2Impl client2) {
        m_client2 = client2;
    }

    @Override
    public ClientResponse callProcedure(String procname, Object... params) throws IOException, ProcCallException {
        return m_client2.callProcedureSync(procname, params);
    }

    @Override
    public void callProcedure(ProcedureCallback callback, String procname, Object... params) throws IOException {
        m_client2.callProcedureAsync(procname, params)
                .whenComplete((r, t) -> callbackAdapter(r, t, callback));
    }

    private Void callbackAdapter(ClientResponse resp, Throwable th, ProcedureCallback cb) {
        try {
            if (th != null) {
                String text = th.getMessage();
                if (text == null) {
                    text = "Unexpected exceptional completion";
                }
                resp = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE,
                                              new VoltTable[0], text);
            }
            cb.clientCallback(resp);
        }
        catch (Exception ex) { // this exception is a bug in our callback code
            VoltLogger logger = new VoltLogger("LOADER");
            logger.error("Exception from loader callback: " + ex);
        }
        return null;
    }

    @Override
    public boolean autoconnectEnabled() {
        return m_client2.autoConnectionMgmt();
    }

    @Override
    public boolean waitForTopology() {
        return m_client2.waitForTopology(60, TimeUnit.SECONDS);
    }

    @Override
    public int getPartitionForParameter(byte type, Object value) {
        return m_client2.getPartitionForParameter(type, value);
    }

    @Override
    public void drainClient() throws InterruptedException {
        m_client2.drain();
    }
}
