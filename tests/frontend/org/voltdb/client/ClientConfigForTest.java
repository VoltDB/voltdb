/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.client;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;

/**
 * {@link ClientStatusListenerExt} implementation that just logs the
 * output of all calls.
 */
public class ClientConfigForTest extends ClientConfig {

    static final VoltLogger log = new VoltLogger("HOST");

    static class LoggingCSL extends ClientStatusListenerExt {

        /* (non-Javadoc)
         * @see org.voltdb.client.ClientStatusListenerExt#connectionLost(java.lang.String, int)
         */
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft,
                ClientStatusListenerExt.DisconnectCause cause) {
            log.info(String.format("ClientConfigForTest reports connection lost due to %s at host %s:%d with %d connections left",
                    cause.toString(), hostname, port, connectionsLeft));
        }

        /* (non-Javadoc)
         * @see org.voltdb.client.ClientStatusListenerExt#backpressure(boolean)
         */
        @Override
        public void backpressure(boolean status) {
            /*
             * Commented out because it is pretty noisy and not very informative.
             */
            //log.info(String.format("ClientConfigForTest reports new backpressure status: %b", status));
        }

        /* (non-Javadoc)
         * @see org.voltdb.client.ClientStatusListenerExt#uncaughtException(org.voltdb.client.ProcedureCallback, org.voltdb.client.ClientResponse, java.lang.Throwable)
         */
        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e) {
            log.info(String.format("ClientConfigForTest reports uncaught exception in callback: %s",
                    callback != null ? callback.getClass().getSimpleName() : "null"));
            log.info(((ClientResponseImpl) r).toJSONString());
            log.info(e);
        }

        /* (non-Javadoc)
         * @see org.voltdb.client.ClientStatusListenerExt#lateProcedureResponse(org.voltdb.client.ClientResponse, java.lang.String, int)
         */
        @Override
        public void lateProcedureResponse(ClientResponse r, String hostname, int port) {
            log.info(String.format("ClientConfigForTest reports late procedure response from %s:%d",
                    hostname, port));
            log.info(((ClientResponseImpl) r).toJSONString());
        }

    }

    public ClientConfigForTest() {
        super("", "", new LoggingCSL());
    }

    public ClientConfigForTest(String user, String password) {
        super(user, password, new LoggingCSL());
    }
}
