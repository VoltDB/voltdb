/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.client.exampleutils;

import org.voltdb.client.*;
import java.util.HashMap;
import java.io.Closeable;
import java.util.concurrent.Future;

public class ClientConnection implements Closeable
{
    private final PerfCounterMap Statistics;
    protected final String KeyBase;
    protected final String Key;
    protected final Client Client;
    protected short Users;
    protected ClientConnection(String clientConnectionKeyBase, String clientConnectionKey, String[] servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns) throws Exception
    {
        this.KeyBase = clientConnectionKeyBase;
        this.Key = clientConnectionKey;
        this.Statistics = ClientConnectionPool.getStatistics(clientConnectionKeyBase);

        // Create configuration
        final ClientConfig config = new ClientConfig(user, password);
        config.setHeavyweight(isHeavyWeight);
        if (maxOutstandingTxns > 0)
            config.setMaxOutstandingTxns(maxOutstandingTxns);

        // Create client
        final Client client = ClientFactory.createClient(config);

        // Create ClientConnections
        for (String server : servers)
            if (server.trim().length() > 0)
                client.createConnection(server.trim(), port);

        this.Client = client;
        this.Users = 0;
    }
    protected ClientConnection use()
    {
        this.Users++;
        return this;
    }
    protected void dispose()
    {
        this.Users--;
        if (this.Users == 0)
        {
            try { this.Client.close(); } catch(Exception x) {}
        }
    }
    @Override
    public void close()
    {
        ClientConnectionPool.dispose(this);
    }
    public ClientResponse execute(String procedure, Object... parameters) throws Exception
    {
        long start = System.currentTimeMillis();
        try
        {
            ClientResponse response = this.Client.callProcedure(procedure, parameters);
            Statistics.update(procedure, response);
            return response;
        }
        catch(ProcCallException pce)
        {
            Statistics.update(procedure, System.currentTimeMillis()-start,false);
            throw pce;
        }
        catch(Exception x)
        {
            throw x;
        }
    }

    private static class TrackingCallback  implements ProcedureCallback
    {
        private final ClientConnection Owner;
        private final String Procedure;
        private final ProcedureCallback UserCallback;
        public TrackingCallback(ClientConnection owner, String procedure, ProcedureCallback userCallback)
        {
            this.Owner = owner;
            this.Procedure = procedure;
            this.UserCallback = userCallback;
        }

        @Override
        public void clientCallback(ClientResponse response)
        {
            try
            {
                this.Owner.Statistics.update(this.Procedure, response);
                if (this.UserCallback != null)
                    this.UserCallback.clientCallback(response);
            }
            catch(Exception x) {} // If the user callback crashes, nothign we can do (user should handle exceptions on his own, we're just wrapping around for tracking!)
        }
    }
    public boolean executeAsync(ProcedureCallback callback, String procedure, Object... parameters) throws Exception
    {
        return this.Client.callProcedure(new TrackingCallback(this, procedure, callback), procedure, parameters);
    }

    public Future<ClientResponse> executeAsync(String procedure, Object... parameters) throws Exception
    {
        final ExecutionFuture future = new ExecutionFuture(25000);
        this.Client.callProcedure(
                                   new TrackingCallback( this
                                                       , procedure
                                                       , new ProcedureCallback()
                                                         {
                                                             final ExecutionFuture result;
                                                             {
                                                                 this.result = future;
                                                             }
                                                             @Override
                                                             public void clientCallback(ClientResponse response) throws Exception
                                                             {
                                                                 future.set(response);
                                                             }
                                                         }
                                                       )
                                 , procedure
                                 , parameters
                                 );
        return future;
    }
    public PerfCounterMap getStatistics()
    {
        return ClientConnectionPool.getStatistics(this);
    }
    public PerfCounter getStatistics(String procedure)
    {
        return ClientConnectionPool.getStatistics(this).get(procedure);
    }
    public PerfCounter getStatistics(String... procedures)
    {
        PerfCounterMap map = ClientConnectionPool.getStatistics(this);
        PerfCounter result = new PerfCounter(false);
        for(String procedure : procedures)
            result.merge(map.get(procedure));
        return result;
    }
}

