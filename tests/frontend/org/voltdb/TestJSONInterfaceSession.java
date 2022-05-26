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
/*
 Copyright (c) 2008 Twilio, Inc.

 Permission is hereby granted, free of charge, to any person
 obtaining a copy of this software and associated documentation
 files (the "Software"), to deal in the Software without
 restriction, including without limitation the rights to use,
 copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the
 Software is furnished to do so, subject to the following
 conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * What's up with the Twilio stuff? They have MIT licensed
 * REST clients in lots of languages, even Java. It's not a
 * direct copy/paste, but this code borrows heavily from what
 * they did in a few ways.
 * Thanks Twilio!
 */
package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.utils.Base64;
import org.voltdb.utils.Encoder;

import junit.framework.TestCase;
import org.apache.http.Header;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.Test;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJSONInterfaceSession extends TestCase {
    final static ContentType utf8ApplicationFormUrlEncoded =
            ContentType.create("application/x-www-form-urlencoded","UTF-8");
    private static final String VALID_JSONP = "good_$123";
    private static final String INVALID_JSONP;
    private static final Set<Integer> HANDLED_CLIENT_ERRORS = new HashSet<>();

    static {
        String pval = "jQuery111106314619798213243_1487039392105\"'></XSS/*-*/STYLE=xss:e/**/xpression(try{a=firstTime}catch(e){firstTime=1;alert(9096)})>";
        try {
            pval = URLEncoder.encode("jQuery111106314619798213243_1487039392105\"'></XSS/*-*/STYLE=xss:e/**/xpression(try{a=firstTime}catch(e){firstTime=1;alert(9096)})>", "UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        INVALID_JSONP = pval;

        HANDLED_CLIENT_ERRORS.add(400);
        HANDLED_CLIENT_ERRORS.add(401);
        HANDLED_CLIENT_ERRORS.add(404);
    }

    ServerThread server;
    Client client;
    public final static String protocolPrefix = ClientConfig.ENABLE_SSL_FOR_TEST ? "https://" : "http://";


    static String japaneseTestVarStrings = "Procedure=Insert&Parameters=%5B%22%5Cu3053%5Cu3093%5Cu306b%5Cu3061%5Cu306f%22%2C%22%5Cu4e16%5Cu754c%22%2C%22Japanese%22%5D";

    static String getHTTPVarString(Map<String, String> params) throws UnsupportedEncodingException {
        String s = "";
        if (params == null) return s;
        for (Entry<String, String> e : params.entrySet()) {
            String encodedValue = URLEncoder.encode(e.getValue(), "UTF-8");
            s += "&" + e.getKey() + "=" + encodedValue;
        }
        s = s.substring(1);
        return s;
    }

    static String getHTTPURL(Integer port, String path) {
        if (port == null) {
            port = VoltDB.DEFAULT_HTTP_PORT;
        }
        return String.format(protocolPrefix + "localhost:%d/%s", port, path);
    }

    public static ResponseMeta callProcOverJSONRaw(Map<String, String> params, final int expectedCode, String sessionId) throws Exception {
        return httpUrlOverJSON("POST", protocolPrefix + "localhost:8095/api/1.0/", null, null, null, expectedCode, null, params, sessionId);
    }

    public static ResponseMeta callProcOverJSONRaw(Map<String, String> params, int httpPort, final int expectedCode, String sessionId) throws Exception {
        return httpUrlOverJSON("POST", protocolPrefix + "localhost:" + httpPort + "/api/1.0/", null, null, null, expectedCode, null, params, sessionId);
    }

    public static ResponseMeta callProcOverJSONRaw(String varString, final int expectedCode, String sessionId) throws Exception {
        return httpUrlOverJSONExecuteWithMeta("POST", protocolPrefix + "localhost:8095/api/1.0/", null, null, null, expectedCode, null, varString, sessionId);
    }

    public static ResponseMeta getUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt, String sessionId) throws Exception {
        return httpUrlOverJSON("GET", url, user, password, scheme, expectedCode, expectedCt, null, sessionId);
    }

    public static ResponseMeta postUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt, Map<String,String> params, String sessionId) throws Exception {
        return httpUrlOverJSON("POST", url, user, password, scheme, expectedCode, expectedCt, params, sessionId);
    }

    private static ResponseMeta putUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt, Map<String,String> params, String sessionId) throws Exception {
        return httpUrlOverJSON("PUT", url, user, password, scheme, expectedCode, expectedCt, params, sessionId);
    }

    private static ResponseMeta deleteUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt, String sessionId) throws Exception {
        return httpUrlOverJSON("DELETE", url, user, password, scheme, expectedCode, expectedCt, null, sessionId);
    }

    static class ResponseMeta {
        String response;
        String sessionId;
    }

    private static CloseableHttpClient httpclient;
    @Override
    public void setUp() {
        try {
            String simpleSchema
                    = "CREATE TABLE test1 (\n"
                    + "    fld1 BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (fld1)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("test1", "fld1");
            VoltProjectBuilder.UserInfo[] ui = new VoltProjectBuilder.UserInfo[2];
            VoltProjectBuilder.RoleInfo ri = new VoltProjectBuilder.RoleInfo("role1", true, false, true, true, false, false);
            builder.addRoles(new VoltProjectBuilder.RoleInfo[] { ri });

            for (int i = 0; i < ui.length; i++) {
                ui[i] = new VoltProjectBuilder.UserInfo("user" + String.valueOf(i), "password" + String.valueOf(i), new String[] { "role1" } );
            }
            builder.addUsers(ui);

            builder.setSecurityEnabled(true, true);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(VoltDB.Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build();
            SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(sslContext,
              SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

            HttpClientBuilder hb = HttpClientBuilder.create();
            hb.setSslcontext(sslContext);
            httpclient = hb.build();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to setup");
        }
    }

    @Override
    public void tearDown() {
        if (httpclient != null) {
            try {
                httpclient.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        if (server != null) {
            try {
                server.shutdown();
                server.join();
            } catch (InterruptedException ex) {
                ;
            }
        }
        server = null;

    }

    private static ResponseMeta httpUrlOverJSONExecuteWithMeta(String method, String url, String user, String password, String scheme, int expectedCode, String expectedCt, String varString, String sessionId) throws Exception {
            HttpRequestBase request;
            switch (method) {
                case "POST":
                    HttpPost post = new HttpPost(url);
                    post.setEntity(new StringEntity(varString, utf8ApplicationFormUrlEncoded));
                    request = post;
                    break;
                case "PUT":
                    HttpPut put = new HttpPut(url);
                    put.setEntity(new StringEntity(varString, utf8ApplicationFormUrlEncoded));
                    request = put;
                    break;
                case "DELETE":
                    HttpDelete delete = new HttpDelete(url);
                    request = delete;
                    break;
                case "GET":
                    request = new HttpGet(url + ((varString != null && varString.trim().length() > 0) ? ("?" + varString.trim()) : ""));
                    break;
                default:
                    request = new HttpGet(url + ((varString != null && varString.trim().length() > 0) ? ("?" + varString.trim()) : ""));
                    break;
            }
            // play nice by using HTTP 1.1 continue requests where the client sends the request headers first
            // to the server to see if the server is willing to accept it. This allows us to test large requests
            // without incurring server socket connection terminations
            RequestConfig rc = RequestConfig.copy(RequestConfig.DEFAULT).setExpectContinueEnabled(true).build();
            request.setProtocolVersion(HttpVersion.HTTP_1_1);
            request.setConfig(rc);
            if (user != null && password != null) {
                if (scheme.equalsIgnoreCase("hashed")) {
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    byte hashedPasswordBytes[] = md.digest(password.getBytes("UTF-8"));
                    String h = user + ":" + Encoder.hexEncode(hashedPasswordBytes);
                    request.setHeader("Authorization", "Hashed " + h);
                } else if (scheme.equalsIgnoreCase("hashed256")) {
                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    byte hashedPasswordBytes[] = md.digest(password.getBytes("UTF-8"));
                    String h = user + ":" + Encoder.hexEncode(hashedPasswordBytes);
                    request.setHeader("Authorization", "Hashed " + h);
                } else if (scheme.equalsIgnoreCase("basic")) {
                    request.setHeader("Authorization", "Basic " + new String(Base64.encodeToString(new String(user + ":" + password).getBytes(), false)));
                }
            }
            ResponseHandler<ResponseMeta> rh = new ResponseHandler<ResponseMeta>() {
                @Override
                public ResponseMeta handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    String ct = response.getHeaders("Content-Type")[0].getValue();
                    if (expectedCt != null) {
                        assertTrue(ct.contains(expectedCt));
                    }
                    assertEquals(expectedCode, status);
                    ResponseMeta r = new ResponseMeta();
                    if (status == 200) {
                        //If we got a good response we should have a cookie. Some good responses dont have cookie so check header present.
                        Header hs[] = response.getHeaders("Set-Cookie");
                        if (hs != null && hs.length > 0) {
                            String cookie = hs[0].getValue();
                            assertTrue(cookie.contains("vmc"));
                            if (sessionId != null) {
                                assertEquals(cookie, sessionId);
                            }
                            if (cookie != null) {
                                System.out.println("Got Session id: " + cookie);
                                r.sessionId = cookie;
                            }
                        }
                    }
                    if ((status >= 200 && status < 300) || HANDLED_CLIENT_ERRORS.contains(status)) {
                        HttpEntity entity = response.getEntity();
                        r.response = entity != null ? EntityUtils.toString(entity) : null;

                        return r;
                    }
                    return null;
                }
            };
        return httpclient.execute(request,rh);
    }

    private static ResponseMeta httpUrlOverJSON(String method, String url, String user, String password, String scheme, int expectedCode, String expectedCt, Map<String,String> params, String sessionId) throws Exception {
        return httpUrlOverJSONExecuteWithMeta(method, url, user, password, scheme, expectedCode, expectedCt, getHTTPVarString(params), sessionId);
    }

    public static String getHashedPasswordForHTTPVar(String password, ClientAuthScheme scheme) {
        assert (password != null);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(ClientAuthScheme.getDigestScheme(scheme));
        } catch (NoSuchAlgorithmException e) {
            fail();
        }
        byte hashedPassword[] = null;
        try {
            hashedPassword = md.digest(password.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("JVM doesn't support UTF-8. Please use a supported JVM", e);
        }

        String retval = Encoder.hexEncode(hashedPassword);
        assertEquals(ClientAuthScheme.getHexencodedDigestLength(scheme), retval.length());
        return retval;
    }

    public static ResponseMeta callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, String sessionId) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, false, 200 /* HTTP_OK */, ClientAuthScheme.HASH_SHA256, sessionId);
    }

    public static ResponseMeta callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin, String sessionId) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, admin, 200 /* HTTP_OK */, ClientAuthScheme.HASH_SHA256, sessionId);
    }

    public static ResponseMeta callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin, int expectedCode, ClientAuthScheme scheme, String sessionId) throws Exception {
        return callProcOverJSON(procName, pset, 8095, username, password, preHash, admin, expectedCode /* HTTP_OK */, scheme, -1, sessionId);
    }

    public static ResponseMeta callProcOverJSON(String procName, ParameterSet pset, int httpPort, String username, String password, boolean preHash, boolean admin, int expectedCode, ClientAuthScheme scheme, int procCallTimeout, String sessionId) throws Exception {
        // Call insert
        String paramsInJSON = pset.toJSONString();
        //System.out.println(paramsInJSON);
        Map<String, String> params = new HashMap<>();
        params.put("Procedure", procName);
        params.put("Parameters", paramsInJSON);
        if (procCallTimeout > 0) {
            params.put(HTTPClientInterface.QUERY_TIMEOUT_PARAM, String.valueOf(procCallTimeout));
        }
        if (username != null) {
            params.put("User", username);
        }
        if (password != null) {
            if (preHash) {
                params.put("Hashedpassword", getHashedPasswordForHTTPVar(password, scheme));
            } else {
                params.put("Password", password);
            }
        }
        if (admin) {
            params.put("admin", "true");
        }

        ResponseMeta ret = callProcOverJSONRaw(params, httpPort, expectedCode, sessionId);
        // Update application catalog sometimes changes the password.
        // The second procedure call will fail in that case, so don't call it a second time.
        if (preHash && !procName.equals("@UpdateApplicationCatalog")) {
            //If prehash make same call with SHA1 to check expected code.
            params.put("Hashedpassword", getHashedPasswordForHTTPVar(password, ClientAuthScheme.HASH_SHA1));
            callProcOverJSONRaw(params, httpPort, expectedCode, sessionId);
        }
        return ret;
    }

    public static boolean canAccessHttpPort(int port) throws Exception {
        String url = "http://127.0.0.1:" + port + "/deployment";
        try {
            getUrlOverJSON(url, null, null, null, 200,  "application/json", null);
            return true;
        } catch (HttpHostConnectException e) {
            return false;
        }
    }

    @Test
    public void testSameSession() throws Exception {
        String sessionId = null;
        ResponseMeta res1 = getUrlOverJSON(protocolPrefix + "localhost:8095/deployment/",
                                "user0", "password0", "hashed", 200,  "application/json", null);
        sessionId = res1.sessionId;
        assertNotNull(res1.sessionId);
        ResponseMeta res2 = getUrlOverJSON(protocolPrefix + "localhost:8095/deployment/",
                null, null, "hashed", 200,  "application/json", sessionId);
        //Null session id and 200 response means we used session that was existing before and not username and password.
        assertNull(res2.sessionId);
    }

    @Test
    public void testNewSessionForDiffUsers() throws Exception {
        ResponseMeta res1 = getUrlOverJSON(protocolPrefix + "localhost:8095/deployment/", "user0", "password0", "hashed", 200,  "application/json", null);
        assertNotNull(res1.sessionId);
        ResponseMeta res2 = getUrlOverJSON(protocolPrefix + "localhost:8095/deployment/", "user1", "password1", "hashed", 200,  "application/json", null);
        //2 sessions for different users are different.
        assertFalse(res1.sessionId.equals(res2.sessionId));
        //Sleep for more than 10 seconds to make session get invalidated so next request with same user will get us new session.
        Thread.sleep(15);
        ResponseMeta res3 = getUrlOverJSON(protocolPrefix + "localhost:8095/deployment/", "user0", "password0", "hashed", 200,  "application/json", null);
        assertFalse(res1.sessionId.equals(res3.sessionId));
        //Logout
        getUrlOverJSON(protocolPrefix + "localhost:8095/logout/", "user0", "password0", "hashed", 200,  "text/html", null);
        //Login Again res1 has original cookie after logout we should have a diff cookie.
        ResponseMeta res4 = getUrlOverJSON(protocolPrefix + "localhost:8095/deployment/", "user0", "password0", "hashed", 200,  "application/json", null);
        assertNotNull(res4.sessionId);
        assertFalse(res1.sessionId.equals(res4.sessionId));
    }

}
