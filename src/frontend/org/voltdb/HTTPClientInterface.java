/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.servlet_voltpatches.http.HttpServletResponse;

import org.eclipse.jetty_voltpatches.continuation.Continuation;
import org.eclipse.jetty_voltpatches.continuation.ContinuationSupport;
import org.eclipse.jetty_voltpatches.server.Request;
import org.voltdb.client.AuthenticatedConnectionCache;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.Encoder;

public class HTTPClientInterface {

    AuthenticatedConnectionCache m_connections = null;
    MessageDigest m_md = null;
    static final int CACHE_TARGET_SIZE = 10;

    class JSONProcCallback implements ProcedureCallback {

        Request m_request;
        final Continuation m_continuation;

        public JSONProcCallback(Request request, Continuation continuation) {
            assert(request != null);
            assert(continuation != null);

            m_request = request;
            m_continuation = continuation;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            ClientResponseImpl rimpl = (ClientResponseImpl) clientResponse;
            String msg = rimpl.toJSONString();

            HttpServletResponse response = (HttpServletResponse) m_continuation.getServletResponse();
            response.setStatus(HttpServletResponse.SC_OK);
            m_request.setHandled(true);
            response.getWriter().print(msg);
            m_continuation.complete();
        }
    }

    public HTTPClientInterface() {
        try {
            m_md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("JVM doesn't support SHA-1 hashing. Please use a supported JVM", e);
        }
    }

    public void process(Request request, HttpServletResponse response) {
        String msg;

        Client client = null;

        Continuation continuation = ContinuationSupport.getContinuation(request);
        continuation.suspend(response);

        try {
            if (m_connections == null) {
                int port = VoltDB.instance().getConfig().m_port;
                m_connections = new AuthenticatedConnectionCache(10, "localhost", port);
            }

            String username = request.getParameter("User");
            String password = request.getParameter("Password");
            String hashedPassword = request.getParameter("Hashedpassword");
            String procName = request.getParameter("Procedure");
            String params = request.getParameter("Parameters");

            // The SHA-1 hash of the password
            byte[] hashedPasswordBytes = null;

            if (password != null) {
                try {
                    hashedPasswordBytes = m_md.digest(password.getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("JVM doesn't support UTF-8. Please use a supported JVM", e);
                }
            }
            // note that HTTP Var "Hashedpassword" has a higher priority
            // Hashedassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded)
            if (hashedPassword != null) {
                if (hashedPassword.length() != 40) {
                    throw new Exception("Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded).");
                }
                try {
                    hashedPasswordBytes = Encoder.hexDecode(hashedPassword);
                }
                catch (Exception e) {
                    throw new Exception("Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded).");
                }
            }

            assert((hashedPasswordBytes == null) || (hashedPasswordBytes.length == 20));

            // get a connection to localhost from the pool
            client = m_connections.getClient(username, hashedPasswordBytes);

            JSONProcCallback cb = new JSONProcCallback(request, continuation);
            boolean success;

            if (params != null) {
                ParameterSet paramSet = ParameterSet.fromJSONString(params);
                success =  client.callProcedure(cb, procName, paramSet.toArray());
            }
            else {
                success = client.callProcedure(cb, procName);
            }
            if (!success) {
                throw new Exception("Server is not accepting work at this time.");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            msg = e.getMessage();
            ClientResponseImpl rimpl = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
            //e.printStackTrace();
            msg = rimpl.toJSONString();
            response.setStatus(HttpServletResponse.SC_OK);
            request.setHandled(true);
            try {
                response.getWriter().print(msg);
                continuation.complete();
            } catch (IOException e1) {}
        }
        finally {
            if (client != null) {
                assert(m_connections != null);
                m_connections.releaseClient(client);
            }
        }
    }
}
