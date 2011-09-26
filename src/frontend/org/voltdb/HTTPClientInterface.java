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

package org.voltdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;
import org.eclipse.jetty.server.Request;
import org.voltdb.client.AuthenticatedConnectionCache;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.Encoder;

public class HTTPClientInterface {

    AuthenticatedConnectionCache m_connections = null;
    MessageDigest m_md = null;
    static final int CACHE_TARGET_SIZE = 10;
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    class JSONProcCallback implements ProcedureCallback {

        final Request m_request;
        final Continuation m_continuation;
        final String m_jsonp;
        final CountDownLatch m_latch = new CountDownLatch(1);

        public JSONProcCallback(Request request, Continuation continuation, String jsonp) {
            assert(request != null);
            assert(continuation != null);

            m_request = request;
            m_continuation = continuation;
            m_jsonp = jsonp;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            ClientResponseImpl rimpl = (ClientResponseImpl) clientResponse;
            String msg = rimpl.toJSONString();

            // handle jsonp pattern
            // http://en.wikipedia.org/wiki/JSON#The_Basic_Idea:_Retrieving_JSON_via_Script_Tags
            if (m_jsonp != null)
                msg = String.format("%s( %s )", m_jsonp, msg);

            // send the response back through jetty
            HttpServletResponse response = (HttpServletResponse) m_continuation.getServletResponse();
            response.setStatus(HttpServletResponse.SC_OK);
            m_request.setHandled(true);
            response.getWriter().print(msg);
            m_continuation.complete();
            m_latch.countDown();
        }

        public void waitForResponse() throws InterruptedException {
            m_latch.await();
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
        boolean adminMode = false;

        Continuation continuation = ContinuationSupport.getContinuation(request);
        continuation.suspend(response);

        try {
            // first check for a catalog update and purge the cached connections
            // if one has happened since we were here last
            if (m_shouldUpdateCatalog.compareAndSet(true, false))
            {
                m_connections.closeAll();
                // Just null the old object so we'll create a new one with
                // updated state below
                m_connections = null;
            }

            if (m_connections == null) {
                int port = VoltDB.instance().getConfig().m_port;
                int adminPort = VoltDB.instance().getConfig().m_adminPort;
                m_connections = new AuthenticatedConnectionCache(10, "localhost", port, adminPort);
            }

            String username = request.getParameter("User");
            String password = request.getParameter("Password");
            String hashedPassword = request.getParameter("Hashedpassword");
            String procName = request.getParameter("Procedure");
            String params = request.getParameter("Parameters");
            String jsonp = request.getParameter("jsonp");
            String admin = request.getParameter("admin");

            // check for admin mode
            if (admin != null) {
                if (admin.compareToIgnoreCase("true") == 0)
                    adminMode = true;
            }

            // null procs are bad news
            if (procName == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                continuation.complete();
                return;
            }

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
            client = m_connections.getClient(username, hashedPasswordBytes, adminMode);

            JSONProcCallback cb = new JSONProcCallback(request, continuation, jsonp);
            boolean success;

            if (params != null) {
                ParameterSet paramSet = null;
                try {
                    paramSet = ParameterSet.fromJSONString(params);
                }
                // if decoding params has a fail, then fail
                catch (Exception e) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    continuation.complete();
                    return;
                }
                // if the paramset has content, but decodes to null, fail
                if (paramSet == null) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    continuation.complete();
                    return;
                }
                success = client.callProcedure(cb, procName, paramSet.toArray());
            }
            else {
                success = client.callProcedure(cb, procName);
            }
            if (!success) {
                throw new Exception("Server is not accepting work at this time.");
            }
            if (adminMode) {
                cb.waitForResponse();
            }
        }
        catch (Exception e) {
            msg = e.getMessage();
            VoltLogger log = new VoltLogger("HOST");
            log.warn("JSON interface: " + msg);
            ClientResponseImpl rimpl = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
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
                // admin connections aren't cached
                if (adminMode) {
                    if (client != null) {
                        try {
                            client.close();
                        } catch (InterruptedException e) {
                            VoltLogger log = new VoltLogger("HOST");
                            log.warn("JSON interface was interrupted while closing an internal admin client connection.");
                        }
                    }
                }
                // other connections are cached
                else {
                    m_connections.releaseClient(client);
                }
            }
        }
    }

    public void notifyOfCatalogUpdate()
    {
        m_shouldUpdateCatalog.set(true);
    }
}
