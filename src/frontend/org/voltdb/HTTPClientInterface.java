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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;
import org.eclipse.jetty.server.Request;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.AuthenticatedConnectionCache;
import org.voltdb.client.ClientAuthHashScheme;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.Base64;
import org.voltdb.utils.Encoder;

public class HTTPClientInterface {

    private static final VoltLogger m_log = new VoltLogger("HOST");
    private static final RateLimitedLogger m_rate_limited_log = new RateLimitedLogger(10 * 1000, m_log, Level.WARN);

    AtomicReference<AuthenticatedConnectionCache> m_connections = new AtomicReference<AuthenticatedConnectionCache>();
    private AuthenticatedConnectionCache m_oldCache;
    static final int CACHE_TARGET_SIZE = 10;

    public static final String PARAM_USERNAME = "User";
    public static final String PARAM_PASSWORD = "Password";
    public static final String PARAM_HASHEDPASSWORD = "Hashedpassword";
    public static final String PARAM_ADMIN = "admin";
    int m_timeout = 0;
    final String m_timeoutResponse;
    private final ExecutorService m_closeAllExecutor;

    public final static int MAX_QUERY_PARAM_SIZE = 2 * 1024 * 1024; // 2MB

    public void setTimeout(int seconds) {
        m_timeout = seconds * 1000;
    }

    class JSONProcCallback implements ProcedureCallback {

        final Request m_request;
        final Continuation m_continuation;
        final String m_jsonp;

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

            m_request.setAttribute("result", msg);
            if (!m_continuation.isInitial()) try {
                m_continuation.resume();
            } catch (IllegalStateException e) {
                // Thrown when we shut down the server via the JSON/HTTP (web studio) API
                // Essentially we're closing everything down from underneath the HTTP request.
                 m_log.warn("JSON request completion exception: ", e);
            }
        }
    }

    public HTTPClientInterface() {
        final ClientResponseImpl r = new ClientResponseImpl(ClientResponse.CONNECTION_TIMEOUT,
                new VoltTable[0], "Request Timeout");
        m_timeoutResponse = r.toJSONString();
        m_closeAllExecutor = CoreUtils.getSingleThreadExecutor("HttpClientInterface-closeAll");
    }

    public void stop() {
        // This is called on server shutdown.
        // So, it is OK if we interrupt threads trying to close client connections.
        m_closeAllExecutor.shutdownNow();
    }

    public void process(Request request, HttpServletResponse response) {
        AuthenticationResult authResult = null;
        boolean suspended = false;
        boolean forceClose = false;
        Continuation continuation = ContinuationSupport.getContinuation(request);
        if (m_timeout > 0) {
            continuation.setTimeout(m_timeout);
        }
        String result = (String )request.getAttribute("result");
        if (result != null) {
            try {
                response.setStatus(HttpServletResponse.SC_OK);
                request.setHandled(true);
                response.getWriter().print(result);
            } catch (IllegalStateException | IOException e){
               // Thrown when we shut down the server via the JSON/HTTP (web studio) API
               // Essentially we're closing everything down from underneath the HTTP request.
                m_log.warn("JSON failed to send response: ", e);
            }
            return;
        }
        //Check if this is resumed request.
        if (Boolean.TRUE.equals(request.getAttribute("SQLSUBMITTED"))) {
            try {
                continuation.suspend(response);
            } catch (IllegalStateException e){
                // Thrown when we shut down the server via the JSON/HTTP (web studio) API
                // Essentially we're closing everything down from underneath the HTTP request.
                 m_log.warn("JSON request completion exception in process: ", e);
            }
            return;
        }
        String jsonp = null;
        try {
            jsonp = request.getParameter("jsonp");
            if (request.getMethod().equalsIgnoreCase("POST")) {
                int queryParamSize = request.getContentLength();
                if (queryParamSize > MAX_QUERY_PARAM_SIZE) {
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
                } catch (IOException e1) {} // Ignore this as browser must have closed.
                return;
            }

            continuation.suspend(response);
            suspended = true;
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
            if (jsonp != null) {
                request.setAttribute("jsonp", jsonp);
            }
            request.setAttribute("SQLSUBMITTED", Boolean.TRUE);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (e instanceof IOException || e instanceof NoConnectionsException) {
                forceClose = true;
            }
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
                if (suspended) {
                    continuation.complete();
                }
            } catch (IOException e1) {} // Ignore this as browser must have closed.
        } finally {
            releaseClient(authResult, forceClose);
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

        AuthenticatedConnectionCache connection_cache = m_connections.get();
        while (connection_cache == null) { // Need a while loop here because there is a small chance that
            // catalog update could null out the cache immediately after another request thread sets it.
            if (m_oldCache!=null) {
                closeAllAsync(m_oldCache);
            }
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
            AuthenticatedConnectionCache newCache = new AuthenticatedConnectionCache(10, clientInterface, port, adminInterface, adminPort);
            boolean setNewValue = m_connections.compareAndSet(null, newCache);
            if (setNewValue) {
                connection_cache = newCache;
            } else {
                connection_cache = m_connections.get();
            }
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
                MessageDigest md = MessageDigest.getInstance(ClientAuthHashScheme.getDigestScheme(ClientAuthHashScheme.HASH_SHA256));
                hashedPasswordBytes = md.digest(password.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                return new AuthenticationResult(null, null, null, adminMode, username, "JVM doesn't support SHA-1 hashing. Please use a supported JVM" + e);
            }
        }
        // note that HTTP Var "Hashedpassword" has a higher priority
        // Hashedassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded)
        // OR
        // Hashedassword must be a 64-byte hex-encoded SHA-256 hash (32 bytes unencoded)
        if (hashedPassword != null) {
            if (hashedPassword.length() != 40 && hashedPassword.length() != 64) {
                return new AuthenticationResult(null, null, null, adminMode, username, "Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded). "
                        + "or 64-byte hex-encoded SHA-256 hash (32 bytes unencoded)");
            }
            try {
                hashedPasswordBytes = Encoder.hexDecode(hashedPassword);
            }
            catch (Exception e) {
                return new AuthenticationResult(null, null, null, adminMode, username, "Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded). "
                        + "or 64-byte hex-encoded SHA-256 hash (32 bytes unencoded)");
            }
        }

        assert((hashedPasswordBytes == null) || (hashedPasswordBytes.length == 20) || (hashedPasswordBytes.length == 32));

        try {
            // get a connection to localhost from the pool
            AuthenticatedConnectionCache.ClientWithHashScheme clientWithScheme =
                    connection_cache.getClient(username, password, hashedPasswordBytes, adminMode);
            if (clientWithScheme != null && clientWithScheme.m_client != null && clientWithScheme.m_scheme != null) {
                return new AuthenticationResult(clientWithScheme.m_client, connection_cache, clientWithScheme.m_scheme,
                        adminMode, username, "");
            }
            return new AuthenticationResult(null, null, null, adminMode, username, "Failed to get client.");
        } catch (IOException ex) {
            return new AuthenticationResult(null, null, null, adminMode, username, ex.getMessage());
        }
    }

    private void closeAllAsync(final AuthenticatedConnectionCache cache) {
        if (cache.isClosing()) {
            return;
        }

        try {
            m_closeAllExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    cache.closeAll();
                }
            });
        } catch(RejectedExecutionException e) {
            m_rate_limited_log.log(System.currentTimeMillis(), Level.WARN, e,
                    "Task to close connections in old connection cache was rejected. Old connections will not be closed");
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
    public void releaseClient(AuthenticationResult authResult, boolean force) {
        if (authResult != null && authResult.m_client != null && authResult.m_connectionCache != null) {
            authResult.m_connectionCache.releaseClient(authResult.m_client, authResult.m_scheme, force || (authResult.m_connectionCache != m_connections.get()));
        }
    }

    public void notifyOfCatalogUpdate()
    {
        AuthenticatedConnectionCache cache = m_connections.get();
        if (cache!=null) { // save the old cache so that it will be closed before we create cache again.
            // Check for null to make sure that two consecutive catalog updates won't null out old cache erroneously.
            m_oldCache = cache;
        }
        m_connections.set(null);
    }
}
