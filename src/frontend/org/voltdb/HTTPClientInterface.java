/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpHeader;
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
import org.voltdb.client.ProcedureCallback;
import org.voltdb.security.AuthenticationRequest;
import org.voltdb.utils.Base64;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.base.Throwables;
import java.util.concurrent.CountDownLatch;
import javax.servlet.http.HttpServletRequest;

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
    int m_timeout = 0;

    final boolean m_spnegoEnabled;
    final String m_servicePrincipal;

    final String m_timeoutResponse;

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

    class JSONProcCallback implements ProcedureCallback {

        final AtomicBoolean m_complete = new AtomicBoolean(false);
        final String m_jsonp;
        final CountDownLatch m_latch;
        String m_msg = "";
        public JSONProcCallback(CountDownLatch latch, String jsonp) {
            m_jsonp = jsonp;
            m_latch = latch;
        }

        public String getResult() {
            return m_msg;
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
                m_latch.countDown();
                return;
            }
            ClientResponseImpl rimpl = (ClientResponseImpl) clientResponse;
            String msg = rimpl.toJSONString();

            // handle jsonp pattern
            // http://en.wikipedia.org/wiki/JSON#The_Basic_Idea:_Retrieving_JSON_via_Script_Tags
            m_msg = asJsonp(m_jsonp, msg);
            m_latch.countDown();
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
        ClientResponseImpl rimpl = new ClientResponseImpl(
                ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], message);
        String msg = rimpl.toJSONString();
        msg = asJsonp(jsonp, msg);
        rsp.setStatus(code);
        try {
            rsp.getWriter().print(msg);
            rsp.getWriter().flush();
        } catch (IOException ignoreThisAsBrowserMustHaveClosed) {
        }
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

    public static boolean validateJSONP(String jsonp, HttpServletRequest request, HttpServletResponse response) {
        if (jsonp != null && !JSONP_PATTERN.matcher(jsonp).matches()) {
            badRequest(null, "Invalid jsonp callback function name", response);
            return false;
        }
        return true;
    }

    public void process(HttpServletRequest request, HttpServletResponse response) {
        AuthenticationResult authResult = null;

        String jsonp = request.getHeader(JSONP);
        if (!validateJSONP(jsonp, request, response)) {
            return;
        }
        String authHeader = request.getHeader(HttpHeader.AUTHORIZATION.asString());
        if (m_spnegoEnabled && (authHeader == null || !authHeader.startsWith(HttpHeader.NEGOTIATE.asString()))) {
            m_log.debug("SpengoAuthenticator: sending challenge");
            response.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
            unauthorized(jsonp, "must initiate SPNEGO negotiation", response);
            return;
        }

        try {
            if (request.getMethod().equalsIgnoreCase("POST")) {
                int queryParamSize = request.getContentLength();

                if (queryParamSize > MAX_QUERY_PARAM_SIZE) {
                    ok(jsonp, "Query string too large: " + String.valueOf(request.getContentLength()), response);
                    return;
                }
                if (queryParamSize == 0) {
                    ok(jsonp, "Received POST with no parameters in the body.", response);
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

            // null procs are bad news
            if (procName == null) {
                badRequest(jsonp, "Procedure parameter is missing", response);
                return;
            }

            int queryTimeout = -1;
            if (timeoutStr != null) {
                try {
                    queryTimeout = Integer.parseInt(timeoutStr);
                    if (queryTimeout <= 0) {
                        throw new NumberFormatException("negative query timeout");
                    }
                } catch(NumberFormatException e) {
                    badRequest(jsonp, "invalid query timeout: " + timeoutStr, response);
                    return;
                }
            }

            authResult = authenticate(request);
            if (!authResult.isAuthenticated()) {
                unauthorized(jsonp, authResult.m_message, response);
                return;
            }
            CountDownLatch latch = new CountDownLatch(1);
            JSONProcCallback cb = new JSONProcCallback(latch, jsonp);
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
                    return;
                }
                // if the paramset has content, but decodes to null, fail
                if (paramSet == null) {
                    badRequest(jsonp, "failed to decode invocation parameters", response);
                    return;
                }
                success = callProcedure(hostname, authResult, queryTimeout, cb, procName, paramSet.toArray());
            }
            else {
                success = callProcedure(hostname, authResult, queryTimeout, cb, procName);
            }
            if (!success) {
                ok(jsonp, "Server is not accepting work at this time.", response);
                return;
            }
            if (jsonp != null) {
                request.setAttribute("jsonp", jsonp);
            }
            latch.await();
            response.getOutputStream().write(cb.getResult().getBytes(), 0, cb.getResult().length());
        } catch (Exception e) {
            String msg = Throwables.getStackTraceAsString(e);
            m_rate_limited_log.log(EstTime.currentTimeMillis(), Level.WARN, e, "JSON interface exception");
            ok(jsonp, msg, response);
        }
    }

    public boolean callProcedure(String hostname, final AuthenticationResult ar, int timeout, ProcedureCallback cb, String procName, Object...args) {
        return m_invocationHandler.get().callProcedure(hostname, ar.m_authUser, ar.m_adminMode, timeout, cb, false, null, procName, args);
    }

    public boolean callProcedure(String hostname, final AuthUser user, boolean adminMode, int timeout, ProcedureCallback cb, String procName, Object...args) {
        return m_invocationHandler.get().callProcedure(hostname, user, adminMode, timeout, cb, false, null, procName, args);
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

    //Remember to call releaseClient if you authenticate which will close admin clients and refcount-- others.
    public AuthenticationResult authenticate(HttpServletRequest request) {
        AuthenticationResult authResult = getAuthenticationResult(request);
        if (!authResult.isAuthenticated()) {
            m_rate_limited_log.log("JSON interface exception: " + authResult.m_message, EstTime.currentTimeMillis());
        }
        return authResult;
    }

    public void notifyOfCatalogUpdate() {
        // NOOP
    }
}
