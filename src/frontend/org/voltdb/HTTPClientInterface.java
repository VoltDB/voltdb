/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationListener;
import org.eclipse.jetty.continuation.ContinuationSupport;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.B64Code;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.Priority;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.security.AuthenticationRequest;
import org.voltdb.utils.Base64;
import org.voltdb.utils.ClientResponseToJsonApiV2;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.base.Throwables;

public class HTTPClientInterface {

    public static final String QUERY_TIMEOUT_PARAM = "Querytimeout";
    public static final String JSONP = "jsonp";
    public static final Pattern JSONP_PATTERN = Pattern.compile("^[a-zA-Z0-9_$]*$");
    private static final VoltLogger m_log = new VoltLogger("HOST");
    private static final RateLimitedLogger m_rate_limited_log = new RateLimitedLogger(10 * 1000, m_log, Level.WARN);

    static final int CACHE_TARGET_SIZE = 10;

    public static final String PARAM_USERNAME = "User";
    public static final String PARAM_PASSWORD = "Password";
    public static final String PARAM_HASHEDPASSWORD = "Hashedpassword";
    public static final String PARAM_ADMIN = "admin";
    public static final String AUTH_USER_SESSION_KEY = "authuser";
    //Hidden property for session inactive timeout.
    public static final int MAX_SESSION_INACTIVITY_SECONDS = Integer.getInteger("HTTP_SESSION_TIMEOUT_SECONDS", 30);
    //Hidden property for disable session management and use always auth mode.
    public static final boolean HTTP_DONT_USE_SESSION = Boolean.getBoolean("HTTP_DONT_USE_SESSION");

    int m_timeout = 0;

    final boolean m_spnegoEnabled;
    final String m_servicePrincipal;

    final String m_timeoutResponse;

    private volatile boolean m_dontUseSession = false;

    private final Supplier<InternalConnectionHandler> m_invocationHandler =
            Suppliers.memoize(new Supplier<InternalConnectionHandler>() {

        @Override
        public InternalConnectionHandler get() {
            ClientInterface ci = VoltDB.instance().getClientInterface();
            if (ci == null || !ci.isAcceptingConnections()) {
                throw new IllegalStateException("Client interface is not ready to be used or has been closed.");
            }
            return ci.getInternalConnectionHandler();
        }

    });

    public final static int MAX_QUERY_PARAM_SIZE = 2 * 1024 * 1024; // 2MB
    public final static int MAX_FORM_KEYS = 512;

    public void setTimeout(int seconds) {
        m_timeout = seconds * 1000;
    }

    class JSONProcCallback implements ProcedureCallback, ContinuationListener {

        final AtomicBoolean m_complete = new AtomicBoolean(false);
        final Continuation m_continuation;
        final String m_jsonp;
        private int m_api_version;

        public JSONProcCallback(Continuation continuation, String jsonp, int api_version) {
            assert continuation != null : "given continuation is null";

            m_continuation = continuation;
            m_continuation.addContinuationListener(this);
            m_jsonp = jsonp;
            m_api_version = api_version;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {

            if (!m_complete.compareAndSet(false, true)) {
                if (clientResponse.getStatus() != ClientResponse.RESPONSE_UNKNOWN) {
                    m_rate_limited_log.log(
                            EstTime.currentTimeMillis(), Level.WARN, null,
                            "Procedure response arrived for a request that was timed out by jetty"
                    );
                }
                return;
            }
            ClientResponseImpl rimpl = (ClientResponseImpl) clientResponse;
            String msg = null;
            if (m_api_version == 1)
                msg = rimpl.toJSONString();
            else if (m_api_version == 2)
                msg = ClientResponseToJsonApiV2.toJSONStringV2(rimpl);

            // handle jsonp pattern
            // http://en.wikipedia.org/wiki/JSON#The_Basic_Idea:_Retrieving_JSON_via_Script_Tags
            msg = asJsonp(m_jsonp, msg);

            m_continuation.setAttribute("result", msg);
            try {
                m_continuation.resume();
            } catch (IllegalStateException e) {
                // Thrown when we shut down the server via the JSON/HTTP (web studio) API
                // Essentially we're closing everything down from underneath the HTTP request.
                 m_log.warn("JSON request cannot be completed. The server is shutting down. " + e.getMessage());
            }
        }

        @Override
        public void onComplete(Continuation continuation) {
            if(!m_complete.get()) {
                m_complete.compareAndSet(false, true);
            }
        }

        @Override
        public void onTimeout(Continuation continuation) {
            if (m_complete.compareAndSet(false, true)) {
                m_continuation.setAttribute("result", m_timeoutResponse);
                m_continuation.resume();
            }
        }
    }

    public HTTPClientInterface() {
        final ClientResponseImpl r = new ClientResponseImpl(ClientResponse.CONNECTION_TIMEOUT,
                new VoltTable[0], "Request Timeout");
        m_timeoutResponse = r.toJSONString();
        m_servicePrincipal = getAuthSystem().getServicePrincipal();
        m_spnegoEnabled = m_servicePrincipal != null && !m_servicePrincipal.isEmpty();
    }

    public void stop() {
    }

    public final static String asJsonp(String jsonp, String msg) {
        if (jsonp == null) return msg;
        StringBuilder sb = new StringBuilder(jsonp.length() + msg.length() + 8);
        return sb.append(jsonp).append("( ").append(msg).append(" )").toString();
    }

    private final static void simpleJsonResponse(String jsonp, String message, HttpServletResponse rsp, int code) {
        simpleJsonResponse(jsonp, message, rsp, code, ClientResponse.UNEXPECTED_FAILURE);
    }

    private final static void simpleJsonResponse(String jsonp, String message, HttpServletResponse rsp, int code,
            byte status) {
        ClientResponseImpl rimpl = new ClientResponseImpl(
                status, new VoltTable[0], message);
        String msg = rimpl.toJSONString();
        msg = asJsonp(jsonp, msg);
        rsp.setStatus(code);
        try {
            rsp.getWriter().print(msg);
            rsp.getWriter().flush();
        } catch (IOException ignoreThisAsBrowserMustHaveClosed) {
        }
    }

    private final static void unavailable(String jsonp, String message, HttpServletResponse rsp) {
        simpleJsonResponse(jsonp, message, rsp, HttpServletResponse.SC_NOT_FOUND,
                ClientResponse.SERVER_UNAVAILABLE);
    }

    private final static void badRequest(String jsonp, String message, HttpServletResponse rsp) {
        simpleJsonResponse(jsonp, message, rsp, HttpServletResponse.SC_BAD_REQUEST);
    }

    private final static void unauthorized(String jsonp, String message, HttpServletResponse rsp) {
        simpleJsonResponse(jsonp, message, rsp, HttpServletResponse.SC_UNAUTHORIZED);
    }

    private final static void ok(String jsonp, String message, HttpServletResponse rsp) {
        simpleJsonResponse(jsonp, message, rsp, HttpServletResponse.SC_OK);
    }

    public static boolean validateJSONP(String jsonp, Request request, HttpServletResponse response) {
        if (jsonp != null && !JSONP_PATTERN.matcher(jsonp).matches()) {
            badRequest(null, "Invalid jsonp callback function name", response);
            request.setHandled(true);
            return false;
        }
        return true;
    }

    public void process(Request request, HttpServletResponse response) {
        AuthenticationResult authResult = null;
        boolean suspended = false;

        String jsonp = request.getHeader(JSONP);
        if (!validateJSONP(jsonp, request, response)) {
            return;
        }
        String authHeader = request.getHeader(HttpHeader.AUTHORIZATION.asString());
        if (m_spnegoEnabled && (authHeader == null || !authHeader.startsWith(HttpHeader.NEGOTIATE.asString()))) {
            m_log.debug("SpengoAuthenticator: sending challenge");
            response.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
            unauthorized(jsonp, "must initiate SPNEGO negotiation", response);
            request.setHandled(true);
            return;
        }

        final Continuation continuation = ContinuationSupport.getContinuation(request);
        String result = (String)continuation.getAttribute("result");
        if (result != null) {
            try {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().print(result);
                request.setHandled(true);
            } catch (IllegalStateException | IOException e){
               // Thrown when we shut down the server via the JSON/HTTP (web studio) API
               // Essentially we're closing everything down from underneath the HTTP request.
                m_log.warn("JSON failed to send response: ", e);
            }
            return;
        }
        //Check if this is resumed request.
        if (Boolean.TRUE.equals(continuation.getAttribute("SQLSUBMITTED"))) {
            try {
                continuation.suspend(response);
            } catch (IllegalStateException e){
                // Thrown when we shut down the server via the JSON/HTTP (web studio) API
                // Essentially we're closing everything down from underneath the HTTP request.
                 m_log.warn("JSON request completion exception in process: ", e);
            }
            return;
        }

        if (m_timeout > 0 && continuation.isInitial()) {
            continuation.setTimeout(m_timeout);
        }

        try {
            if (request.getMethod().equalsIgnoreCase("POST")) {
                int queryParamSize = request.getContentLength();

                if (queryParamSize > MAX_QUERY_PARAM_SIZE) {
                    ok(jsonp, "Query string too large: " + String.valueOf(request.getContentLength()), response);
                    request.setHandled(true);
                    return;
                }
                if (queryParamSize == 0) {
                    ok(jsonp, "Received POST with no parameters in the body.", response);
                    request.setHandled(true);
                    return;
                }
            }
            if (jsonp == null) {
                jsonp = request.getParameter(JSONP);
                if (!validateJSONP(jsonp, request, response)) {
                    return;
                }
            }
            String procName = request.getParameter("Procedure");
            String params = request.getParameter("Parameters");
            String timeoutStr = request.getParameter(QUERY_TIMEOUT_PARAM);
            String prioStr = request.getParameter("Priority");

            // null procs are bad news
            if (procName == null) {
                badRequest(jsonp, "Procedure parameter is missing", response);
                request.setHandled(true);
                return;
            }

            int queryTimeout = -1;
            if (timeoutStr != null) {
                try {
                    queryTimeout = parseIntegerParam(timeoutStr, 1, Integer.MAX_VALUE);
                } catch(NumberFormatException e) {
                    badRequest(jsonp, "invalid query timeout: " + timeoutStr, response);
                    request.setHandled(true);
                    return;
                }
            }

            int priority = Priority.DEFAULT_PRIORITY;
            if (prioStr != null) {
                try {
                    priority = parseIntegerParam(prioStr, Priority.HIGHEST_PRIORITY, Priority.LOWEST_PRIORITY);
                } catch(NumberFormatException e) {
                    badRequest(jsonp, "invalid priority: " + prioStr, response);
                    request.setHandled(true);
                    return;
                }
            }

            if (VoltDB.instance().getMode() == OperationMode.SHUTTINGDOWN) {
                unavailable(jsonp, "database is shutting down.", response);
                request.setHandled(true);
                return;
            }

            authResult = authenticate(request);
            if (!authResult.isAuthenticated()) {
                unauthorized(jsonp, authResult.m_message, response);
                request.setHandled(true);
                return;
            }

            if (InvocationDispatcher.getProcedureFromName(procName, VoltDB.instance().getCatalogContext()) == null) {
                String err = String.format("Unknown procedure name '%s'.", procName);
                ok(jsonp, err, response); // HTTP status = ok, VoltDB status = failure
                request.setHandled(true);
                return;
            }

            continuation.suspend(response);
            suspended = true;
            JSONProcCallback cb;
            if (request.getServletPath().equals("/api/2.0"))
                cb = new JSONProcCallback(continuation, jsonp, 2);
            else
                cb = new JSONProcCallback(continuation, jsonp, 1);

            boolean success;
            String hostname = request.getRemoteHost();
            if (params != null) {
                ParameterSet paramSet = null;
                try {
                    paramSet = ParameterSet.fromJSONString(params);
                }
                // if decoding params has a fail, then fail
                catch (Exception e) {
                    badRequest(jsonp, "failed to parse invocation parameters", response);
                    request.setHandled(true);
                    continuation.complete();
                    return;
                }
                // if the paramset has content, but decodes to null, fail
                if (paramSet == null) {
                    badRequest(jsonp, "failed to decode invocation parameters", response);
                    request.setHandled(true);
                    continuation.complete();
                    return;
                }
                success = callProcedure(hostname, authResult, queryTimeout, priority, cb, procName, paramSet.toArray());
            }
            else {
                success = callProcedure(hostname, authResult, queryTimeout, priority, cb, procName);
            }
            if (!success) {
                ok(jsonp, "Server is not accepting work at this time.", response);
                request.setHandled(true);
                continuation.complete();
                return;
            }
            if (jsonp != null) {
                request.setAttribute("jsonp", jsonp);
            }
            continuation.setAttribute("SQLSUBMITTED", Boolean.TRUE);
        } catch (Exception e) {
            String msg = Throwables.getStackTraceAsString(e);
            m_rate_limited_log.log(EstTime.currentTimeMillis(), Level.WARN, e, "JSON interface exception");
            ok(jsonp, msg, response);
            if (suspended) {
                continuation.complete();
            }
            request.setHandled(true);
        }
    }

    private static int parseIntegerParam(String str, int low, int high) {
        int val = Integer.parseInt(str); // may throw
        if (val < low || val > high) {
            throw new NumberFormatException(String.format("value %d out of range (%d, %d)",
                                                          val, low, high));
        }
        return val;
    }

    // With request priority, defaulted to Priority.DEFAULT_PRIORITY.
    // Always resolved before we call InternalConnectionHandler since we do not want
    // the priority to default to SYSTEM_PRIORITY as it's a user request.
    private boolean callProcedure(String hostname, AuthenticationResult ar, int timeout, int priority, ProcedureCallback cb, String procName, Object...args) {
        assert priority > 0;
        InternalConnectionHandler internal = m_invocationHandler.get();
        return internal.callProcedure(hostname, ar.m_authUser, ar.m_adminMode, timeout, priority, cb, false, null, procName, args);
    }

    // Called from utils/DeploymentRequestServlet for @UpdateApplicationCatalog
    public boolean callProcedure(String hostname, AuthenticationResult ar, int timeout, ProcedureCallback cb, String procName, Object...args) {
        InternalConnectionHandler internal = m_invocationHandler.get();
        return internal.callProcedure(hostname, ar.m_authUser, ar.m_adminMode, timeout, Priority.DEFAULT_PRIORITY, cb, false, null, procName, args);
    }

    // TODO - Apparently unused
    private boolean callProcedure(String hostname, AuthUser user, boolean adminMode, int timeout, ProcedureCallback cb, String procName, Object...args) {
        InternalConnectionHandler internal = m_invocationHandler.get();
        return internal.callProcedure(hostname, user, adminMode, timeout, cb, procName, args);
    }

    Configuration getVoltDBConfig() {
        return VoltDB.instance().getConfig();
    }

    private AuthenticationResult getAuthenticationResult(HttpServletRequest request) {
        boolean adminMode = false;

        String username = null;
        String hashedPassword = null;
        String password = null;
        String token = null;
        //Check authorization header
        String auth = request.getHeader(HttpHeader.AUTHORIZATION.asString());
        boolean validAuthHeader = false;
        if (auth != null) {
            String schemeAndHandle[] = auth.split(" ");
            if (auth.startsWith(HttpHeader.NEGOTIATE.asString())) {
                token = (auth.length() >= 10 ? auth.substring(10) : "");
                validAuthHeader = true;
            } else if (schemeAndHandle.length == 2) {
                if (schemeAndHandle[0].equalsIgnoreCase("hashed")) {
                    String up[] = schemeAndHandle[1].split(":");
                    if (up.length == 2) {
                        username = up[0];
                        hashedPassword = up[1];
                        validAuthHeader = true;
                    }
                } else if (schemeAndHandle[0].equalsIgnoreCase("basic")) {
                    try {
                        String unpw = new String(Base64.decode(schemeAndHandle[1]));
                        String up[] = unpw.split(":");
                        if (up.length == 2) {
                            username = up[0];
                            password = up[1];
                            validAuthHeader = true;
                        }
                    } catch (Exception ex) {
                        m_rate_limited_log.log(EstTime.currentTimeMillis(), Level.WARN, null,
                                               "Malformed base64 string in basic-auth header");
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

        adminMode = "true".equalsIgnoreCase(admin);

        // The SHA-1 hash of the password
        byte[] hashedPasswordBytes = null;

        if (password != null) {
            try {
                // Create a MessageDigest every time because MessageDigest is not thread safe (ENG-5438)
                MessageDigest md = MessageDigest.getInstance(ClientAuthScheme.getDigestScheme(ClientAuthScheme.HASH_SHA256));
                hashedPasswordBytes = md.digest(password.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                return new AuthenticationResult(false, null, adminMode, username, "JVM doesn't support SHA-256 hashing. Please use a supported JVM" + e);
            }
        }
        // note that HTTP Var "Hashedpassword" has a higher priority
        // Hashedassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded)
        // OR
        // Hashedassword must be a 64-byte hex-encoded SHA-256 hash (32 bytes unencoded)
        if (hashedPassword != null) {
            if (hashedPassword.length() != 40 && hashedPassword.length() != 64) {
                return new AuthenticationResult(false, null, adminMode, username, "Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded). "
                        + "or 64-byte hex-encoded SHA-256 hash (32 bytes unencoded)");
            }
            try {
                hashedPasswordBytes = Encoder.hexDecode(hashedPassword);
            }
            catch (Exception e) {
                return new AuthenticationResult(false, null, adminMode, username, "Hashedpassword must be a 40-byte hex-encoded SHA-1 hash (20 bytes unencoded). "
                        + "or 64-byte hex-encoded SHA-256 hash (32 bytes unencoded)");
            }
        }

        assert((hashedPasswordBytes == null) || (hashedPasswordBytes.length == 20) || (hashedPasswordBytes.length == 32));

        String fromAddress = request.getRemoteAddr();
        if (fromAddress == null) fromAddress = "NULL";

        if (m_spnegoEnabled) {
            final String principal = spnegoLogin(token);
            AuthenticationRequest authReq = getAuthSystem().new SpnegoPassthroughRequest(principal);
            if (!authReq.authenticate(ClientAuthScheme.SPNEGO, fromAddress)) {
                return new AuthenticationResult(
                        false, null, adminMode, principal,
                        "User " + principal + " from " + fromAddress + " failed to authenticate"
                        );
            }
            return new AuthenticationResult(true, ClientAuthScheme.SPNEGO, adminMode, principal, "");
        } else {
            AuthenticationRequest authReq = getAuthSystem().new HashAuthenticationRequest(username, hashedPasswordBytes);
            ClientAuthScheme scheme = hashedPasswordBytes != null ?
                    ClientAuthScheme.getByUnencodedLength(hashedPasswordBytes.length)
                    : ClientAuthScheme.HASH_SHA256;
            if (!authReq.authenticate(scheme, fromAddress)) {
                return new AuthenticationResult(
                        false, null, adminMode, username,
                        "User " + username + " from " + fromAddress + " failed to authenticate"
                        );
            }
            return new AuthenticationResult(true, scheme, adminMode, username, "");
        }
    }

    private String spnegoLogin(String encodedToken) {
        byte[] token = B64Code.decode(encodedToken);
        try {
            if (encodedToken == null || encodedToken.isEmpty()) {
                return null;
            }
            final Oid spnegoOid = new Oid("1.3.6.1.5.5.2");

            GSSManager manager = GSSManager.getInstance();
            GSSName name = manager.createName(m_servicePrincipal, null);
            GSSContext ctx = manager.createContext(
                    name.canonicalize(spnegoOid), spnegoOid,
                    null, GSSContext.INDEFINITE_LIFETIME
                    );
            if (ctx == null) {
                m_rate_limited_log.log(
                        EstTime.currentTimeMillis(),
                        Level.ERROR, null,
                        "Failed to establish security context for SPNEGO authentication"
                        );
                return null;
            }
            while (!ctx.isEstablished()) {
                token = ctx.acceptSecContext(token, 0, token.length);
            }
            if (ctx.isEstablished()) {
                if (ctx.getSrcName() == null) {
                    m_rate_limited_log.log(
                            EstTime.currentTimeMillis(),
                            Level.ERROR, null,
                            "Failed to read source name from established SPNEGO security context"
                            );
                    return null;
                }
                String user = ctx.getSrcName().toString();
                if (m_log.isDebugEnabled()) {
                    m_log.debug("established SPNEGO security context for " + user);
                }
                return user;
            }
            return null;
        } catch (GSSException e) {
            m_rate_limited_log.log(EstTime.currentTimeMillis(), Level.ERROR, e, "failed SPNEGO authentication");
            return null;
        }
    }

    AuthSystem getAuthSystem() {
        return VoltDB.instance().getCatalogContext().authSystem;
    }

    //Clear session of any auth attributes and invalidate as well. Just invalidate is not enough for some reason jetty
    //reuses it and happily validates it.
    public void unauthenticate(HttpServletRequest request) {
        if (HTTP_DONT_USE_SESSION) return;
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.removeAttribute(AUTH_USER_SESSION_KEY);
            session.invalidate();
        }
    }

    //Look to get session if no session found or created fallback to always authenticate mode.
    public AuthenticationResult authenticate(HttpServletRequest request) {
        HttpSession session = null;
        AuthenticationResult authResult = null;
        if (!HTTP_DONT_USE_SESSION && !m_dontUseSession) {
            try {
                session = request.getSession();
                if (session != null) {
                    if (session.isNew()) {
                        session.setMaxInactiveInterval(MAX_SESSION_INACTIVITY_SECONDS);
                    }
                    authResult = (AuthenticationResult )session.getAttribute(AUTH_USER_SESSION_KEY);
                }
            } catch (Exception ex) {
                //Use no session mode meaning whatever VMC sends as hashed password is used to authenticate.
                session = null;
                m_rate_limited_log.log(EstTime.currentTimeMillis(), Level.ERROR, ex, "Failed to get or create HTTP Session. authenticating user explicitely.");
            }
        }
        if (authResult == null) {
            authResult = getAuthenticationResult(request);
            if (!authResult.isAuthenticated()) {
                if (session != null) {
                    session.removeAttribute(AUTH_USER_SESSION_KEY);
                }
                m_rate_limited_log.log("JSON interface exception: " + authResult.m_message, EstTime.currentTimeMillis());
            } else {
                if (session != null) {
                    //Cache the authResult in session so we dont authenticate again.
                    session.setAttribute(AUTH_USER_SESSION_KEY, authResult);
                }
            }
        }
        return authResult;
    }

    //Do not store AuthenticationResult in sessions and let all the sessions expired during catalog update.
    //After all the sessions time out, AuthenticationResult will again be stored in sessions to avoid repeated
    //authentication.
    public void dontStoreAuthenticationResultInHttpSession() {
        m_dontUseSession = true;
        final Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                m_dontUseSession = false;
                timer.cancel();
            }
        }, TimeUnit.SECONDS.toMillis(MAX_SESSION_INACTIVITY_SECONDS));
    }
}
