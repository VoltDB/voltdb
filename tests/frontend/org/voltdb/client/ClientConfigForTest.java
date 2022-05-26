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

package org.voltdb.client;

import org.voltdb.ClientResponseImpl;

/**
 * {@link ClientStatusListenerExt} implementation that just logs the
 * output of all calls for testing.  Logging is by prints to standard
 * output, so this shows up in junit console output.
 *
 * @see ClientStatusListenerExt
 */
public class ClientConfigForTest extends ClientConfig {

    static class LoggingCSL extends ClientStatusListenerExt {

        @Override
        public void connectionCreated(String hostname, int port, ClientStatusListenerExt.AutoConnectionStatus status) {
            if (status == ClientStatusListenerExt.AutoConnectionStatus.SUCCESS) {
                print("connection created to host %s:%d", hostname, port);
            } else {
                print("connection failure to host %s:%d because %s", hostname, port, status);
            }
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, ClientStatusListenerExt.DisconnectCause cause) {
            print("connection lost to host %s:%d lost due to %s; %d connections left",
                  hostname, port, cause, connectionsLeft);
        }

        @Override
        public void backpressure(boolean status) {
            // Commented out because it is pretty noisy and not very informative.
            // print("new backpressure status: %b", status);
        }

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e) {
            print("uncaught exception in callback %s", callback != null ? callback.getClass().getSimpleName() : "null");
            print(((ClientResponseImpl) r).toJSONString());
            print(e.toString());
        }

        @Override
        public void lateProcedureResponse(ClientResponse r, String hostname, int port) {
            print("late procedure response from %s:%d", hostname, port);
            print(((ClientResponseImpl) r).toJSONString());
        }

        private void print(String msg, Object... args) {
            if (args.length != 0) { // no args, then plain print (no prefix)
                msg = "ClientConfigForTest reports " + String.format(msg, args);
            }
            else if (msg == null) {
                msg = "<null>";
            }
            System.out.println(msg);
        }
    }

    public ClientConfigForTest() {
        super("", "", new LoggingCSL());
    }

    //By default this will use SHA256
    public ClientConfigForTest(String user, String password) {
        super(user, password, new LoggingCSL());
    }

    public ClientConfigForTest(String user, String password, ClientAuthScheme scheme) {
        super(user, password, new LoggingCSL(), scheme);
    }
}
