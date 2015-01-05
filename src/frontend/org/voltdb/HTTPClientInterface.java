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
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.AuthenticatedConnectionCache;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltcore.logging.Level;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.utils.Base64;
import org.voltdb.utils.Encoder;

public class HTTPClientInterface {

    private static final VoltLogger m_log = new VoltLogger("HOST");
    private static final RateLimitedLogger m_rate_limited_log = new RateLimitedLogger(10 * 1000, m_log, Level.WARN);

    AuthenticatedConnectionCache m_connections = null;
    static final int CACHE_TARGET_SIZE = 10;
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    public static final String PARAM_USERNAME = "User";
    public static final String PARAM_PASSWORD = "Password";
    public static final String PARAM_HASHEDPASSWORD = "Hashedpassword";
    public static final String PARAM_ADMIN = "admin";


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
            if (m_jsonp != null) {
                msg = String.format("%s( %s )", m_jsonp, msg);
            }

            // send the response back through jetty
            HttpServletResponse response = (HttpServletResponse) m_continuation.getServletResponse();
            response.setStatus(HttpServletResponse.SC_OK);
            m_request.setHandled(true);
            response.getWriter().print(msg);
            try{
                m_continuation.complete();
             } catch (IllegalStateException e){
                // Thrown when we shut down the server via the JSON/HTTP (web studio) API
                // Essentially we're closing everything down from underneath the HTTP request.
                 m_log.warn("JSON request completion exception: ", e);
             }
            m_latch.countDown();
        }

        public void waitForResponse() throws InterruptedException {
            m_latch.await();
        }
    }

    public HTTPClientInterface() {
    }

    public void process(Request request, HttpServletResponse response) {
        AuthenticationResult authResult = null;

        Continuation continuation = ContinuationSupport.getContinuation(request);
        continuation.suspend(response);
        String jsonp = null;
        try {
            jsonp = request.getParameter("jsonp");
            if (request.getMethod().equalsIgnoreCase("POST")) {
                int queryParamSize = request.getContentLength();
                if (queryParamSize > 150000) {
                    // We don't want to be building huge strings
                    throw new Exception("Query string too large: " + String.valueOf(request.getContentLength()));
                }
                if (queryParamSize == 0) {
                    throw new Exception("Received POST with no parameters in the body.");
                }
            }

            String procName = request.getParameter("Procedure");
            String params = request.getParameter("Parameters");

            // null procs are bad news
            if (procName == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                continuation.complete();
                return;
            }

            authResult = authenticate(request);
            if (!authResult.isAuthenticated()) {
                String msg = authResult.m_message;
                ClientResponseImpl rimpl = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
                msg = rimpl.toJSONString();
                if (jsonp != null) {
                    msg = String.format("%s( %s )", jsonp, msg);
                }
                response.setStatus(HttpServletResponse.SC_OK);
                request.setHandled(true);
                try {
                    response.getWriter().print(msg);
                    continuation.complete();
                } catch (IOException e1) {} // Ignore this as browser must have closed.
                return;
            }

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
                success = authResult.m_client.callProcedure(cb, procName, paramSet.toArray());
            }
            else {
                success = authResult.m_client.callProcedure(cb, procName);
            }
            if (!success) {
                throw new Exception("Server is not accepting work at this time.");
            }
            if (authResult.m_adminMode) {
                cb.waitForResponse();
            }
        } catch (Exception e) {
            String msg = e.getMessage();
            m_rate_limited_log.log("JSON interface exception: " + msg, EstTime.currentTimeMillis());
            ClientResponseImpl rimpl = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
            msg = rimpl.toJSONString();
            if (jsonp != null) {
                msg = String.format("%s( %s )", jsonp, msg);
            }
            response.setStatus(HttpServletResponse.SC_OK);
            request.setHandled(true);
            try {
                response.getWriter().print(msg);
                continuation.complete();
            } catch (IOException e1) {} // Ignore this as browser must have closed.
        } finally {
            releaseClient(authResult);
        }
    }

    private AuthenticationResult getAuthenticationResult(Request request) {
        boolean adminMode = false;

        String username = null;
        String hashedPassword = null;
        String password = null;
        //Check authorization header
        String auth = request.getHeader("Authorization");
        boolean validAuthHeader = false;
        if (auth != null) {
            String schemeAndHandle[] = auth.split(" ");
            if (schemeAndHandle.length == 2) {
                if (schemeAndHandle[0].equalsIgnoreCase("hashed")) {
                    String up[] = schemeAndHandle[1].split(":");
                    if (up.length == 2) {
                        username = up[0];
                        hashedPassword = up[1];
                        validAuthHeader = true;
                    }
                } else if (schemeAndHandle[0].equalsIgnoreCase("basic")) {
                    String unpw = new String(Base64.decode(schemeAndHandle[1]));
                    String up[] = unpw.split(":");
                    if (up.length == 2) {
                        username = up[0];
                        password = up[1];
                        validAuthHeader = true;
                    }
                }
            }
        }
        if (!validAuthHeader) {
            username = request.getParameter(PARAM_USERNAME);
            hashedPassword = request.getParameter(PARAM_HASHEDPASSWORD);
            password = request.getParameter(PARAM_PASSWORD);
        }
        String admin = request.getParameter(PARAM_ADMIN);

        // first check for a catalog update and purge the cached connections
        // if one has happened since we were here last
        if (m_shouldUpdateCatalog.compareAndSet(true, false))
        {
            if (m_connections != null) {
                m_connections.closeAll();
                // Just null the old object so we'll create a new one with
                // updated state below
                m_connections = null;
            }
        }

        if (m_connections == null) {
            Configuration config = VoltDB.instance().getConfig();
            int port = config.m_port;
            int adminPort = config.m_adminPort;
            String externalInterface = config.m_externalInterface;
            String adminInterface = "localhost";
            String clientInterface = "localhost";
            if (externalInterface != null && !externalInterface.isEmpty()) {
                clientInterface = externalInterface;
                adminInterface = externalInterface;
            }
            //If individual override is available use them.
            if (config.m_clientInterface.length() > 0) {
                clientInterface = config.m_clientInterface;
            }
            if (config.m_adminInterface.length() > 0) {
                adminInterface = config.m_adminInterface;
            }
            m_connections = new AuthenticatedConnectionCache(10, clientInterface, port, adminInterface, adminPort);
        }

        // check for admin mode
        if (admin != null) {
            if (admin.compareToIgnoreCase("true") == 0) {
                adminMode = true;
            }
        }

        // The SHA-1 hash of the password
        byte[] hashedPasswordBytes = null;

        if (password != null) {
            try {
                // Create a MessageDigest every time because MessageDigest is not thread safe (ENG-5438)
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                hashedPasswordBytes = md.digest(password.getBytes("UTF-8"));
            } catch (NoSuchAlgorithmException e) {
                return new AuthenticationResult(null, adminMode, username, "JVM doesn't support SHA-1 hashing. Please use a supported JVM" + e);
            } catch (UnsupportedEncodingException e) {
                return new AuthenticationResult(null, adminMode, username, "JVM doesn't support UTF-8. Please use a supported JVM" + e);
            }
        }
        // note that HTTP Var "Hashedpassword" has a higher priority
        // Hashedassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded)
        if (hashedPassword != null) {
            if (hashedPassword.length() != 40) {
                return new AuthenticationResult(null, adminMode, username, "Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded).");
            }
            try {
                hashedPasswordBytes = Encoder.hexDecode(hashedPassword);
            }
            catch (Exception e) {
                return new AuthenticationResult(null, adminMode, username, "Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded).");
            }
        }

        assert((hashedPasswordBytes == null) || (hashedPasswordBytes.length == 20));

        try {
            // get a connection to localhost from the pool
            Client client = m_connections.getClient(username, password, hashedPasswordBytes, adminMode);
            if (client != null) {
                return new AuthenticationResult(client, adminMode, username, "");
            }
            return new AuthenticationResult(null, adminMode, username, "Failed to get client.");
        } catch (IOException ex) {
            return new AuthenticationResult(null, adminMode, username, ex.getMessage());
        }
    }

    //Remember to call releaseClient if you authenticate which will close admin clients and refcount-- others.
    public AuthenticationResult authenticate(Request request) {
        AuthenticationResult authResult = getAuthenticationResult(request);
        if (!authResult.isAuthenticated()) {
            m_rate_limited_log.log("JSON interface exception: " + authResult.m_message, EstTime.currentTimeMillis());
        }
        return authResult;
    }

    //Must be called by all who call authenticate.
    public void releaseClient(AuthenticationResult authResult) {
        if (authResult != null && authResult.m_client != null) {
            assert(m_connections != null);
            // admin connections aren't cached
            if (authResult.m_adminMode) {
                try {
                    authResult.m_client.close();
                } catch (InterruptedException e) {
                    m_log.warn("JSON interface was interrupted while closing an internal admin client connection.");
                }
            }
            // other connections are cached
            else {
                m_connections.releaseClient(authResult.m_client);
            }
        }
    }

    public void notifyOfCatalogUpdate()
    {
        m_shouldUpdateCatalog.set(true);
    }
}
