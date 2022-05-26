/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.utils;

import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;

/**
 * This is a status listener you can use by default which just prints all the events.
 *
 */
public class OverprintingStatusListener extends ClientStatusListenerExt {

    String clientId;

    public OverprintingStatusListener(String clientId) {
        this.clientId = clientId;
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.ClientStatusListenerExt#connectionLost(java.lang.String, int, int, org.voltdb.client.ClientStatusListenerExt.DisconnectCause)
     */
    @Override
    public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
        System.err.printf("Client %s lost connection to %s:%d (%d connections left) for reason: %s.\n",
                clientId, hostname, port, connectionsLeft, cause.toString());
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.ClientStatusListenerExt#uncaughtException(org.voltdb.client.ProcedureCallback, org.voltdb.client.ClientResponse, java.lang.Throwable)
     */
    @Override
    public void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e) {
        System.err.printf("Client %s got an Uncaught Exception for response: \n%s\n",
                clientId, ((ClientResponseImpl) r).toJSONString());
        e.printStackTrace();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.ClientStatusListenerExt#lateProcedureResponse(org.voltdb.client.ClientResponse, java.lang.String, int)
     */
    @Override
    public void lateProcedureResponse(ClientResponse r, String hostname, int port) {
        System.err.printf("Client %s got a late response from %s:%d: \n%s\n",
                clientId, hostname, port, ((ClientResponseImpl) r).toJSONString());
    }
}
