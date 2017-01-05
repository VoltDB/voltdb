/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType.Memorylimit;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Query;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.procedures.CrazyBlahProc;
import org.voltdb.compiler.procedures.DelayProc;
import org.voltdb.compiler.procedures.SelectStarHelloWorld;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Base64;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;
import org.voltdb.compiler.deploymentfile.SnmpType;

public class TestJSONInterface extends TestCase {
    final static ContentType utf8ApplicationFormUrlEncoded =
            ContentType.create("application/x-www-form-urlencoded","UTF-8");

    ServerThread server;
    Client client;

    static class Response {

        public byte status = 0;
        public String statusString = null;
        public byte appStatus = Byte.MIN_VALUE;
        public String appStatusString = null;
        public VoltTable[] results = new VoltTable[0];
        public String exception = null;
    }

    static String japaneseTestVarStrings = "Procedure=Insert&Parameters=%5B%22%5Cu3053%5Cu3093%5Cu306b%5Cu3061%5Cu306f%22%2C%22%5Cu4e16%5Cu754c%22%2C%22Japanese%22%5D";

    static String getHTTPVarString(Map<String, String> params) throws UnsupportedEncodingException {
        String s = "";
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
        return String.format("http://localhost:%d/%s", port, path);
    }

    public static String callProcOverJSONRaw(String varString, final int expectedCode) throws Exception {
        URI jsonAPIURI = URI.create("http://localhost:8095/api/1.0/");
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(jsonAPIURI);
            // play nice by using HTTP 1.1 continue requests where the client sends the request headers first
            // to the server to see if the server is willing to accept it. This allows us to test large requests
            // without incurring server socket connection terminations
            RequestConfig rc = RequestConfig.copy(RequestConfig.DEFAULT).setExpectContinueEnabled(true).build();
            post.setProtocolVersion(HttpVersion.HTTP_1_1);
            post.setConfig(rc);
            post.setEntity(new StringEntity(varString, utf8ApplicationFormUrlEncoded));
            ResponseHandler<String> rh = new ResponseHandler<String>() {
                @Override
                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    assertEquals(expectedCode, status);
                    if ((status >= 200 && status < 300) || status == 400) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    }
                    return null;
                }
            };
            return httpclient.execute(post,rh);
        }
    }

    public static String getUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt) throws Exception {
        return httpUrlOverJSON("GET", url, user, password, scheme, expectedCode, expectedCt, null);
    }

    public static String postUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt, Map<String,String> params) throws Exception {
        return httpUrlOverJSON("POST", url, user, password, scheme, expectedCode, expectedCt, params);
    }

    private static String putUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt, Map<String,String> params) throws Exception {
        return httpUrlOverJSON("PUT", url, user, password, scheme, expectedCode, expectedCt, params);
    }

    private static String deleteUrlOverJSON(String url, String user, String password, String scheme, int expectedCode, String expectedCt) throws Exception {
        return httpUrlOverJSON("DELETE", url, user, password, scheme, expectedCode, expectedCt, null);
    }

    private static String httpUrlOverJSON(String method, String url, String user, String password, String scheme, int expectedCode, String expectedCt, Map<String,String> params) throws Exception {
        URL jsonAPIURL = new URL(url);

        HttpURLConnection conn = (HttpURLConnection) jsonAPIURL.openConnection();
        conn.setRequestMethod(method);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        if (user != null && password != null) {
            if (scheme.equalsIgnoreCase("hashed")) {
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                byte hashedPasswordBytes[] = md.digest(password.getBytes("UTF-8"));
                String h = user + ":" + Encoder.hexEncode(hashedPasswordBytes);
                conn.setRequestProperty("Authorization", "Hashed " + h);
            } else if (scheme.equalsIgnoreCase("hashed256")) {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte hashedPasswordBytes[] = md.digest(password.getBytes("UTF-8"));
                String h = user + ":" + Encoder.hexEncode(hashedPasswordBytes);
                conn.setRequestProperty("Authorization", "Hashed " + h);
            } else if (scheme.equalsIgnoreCase("basic")) {
                conn.setRequestProperty("Authorization", "Basic " + new String(Base64.encodeToString(new String(user + ":" + password).getBytes(), false)));
            }
        }
        conn.connect();
        byte andbyte[] = String.valueOf('&').getBytes();
        if (params != null && params.size() > 0) {
            OutputStream os = conn.getOutputStream();
            for (String key : params.keySet()) {
                os.write(key.getBytes());
                if (params.get(key) != null) {
                    String b = "=" + params.get(key);
                    os.write(b.getBytes());
                }
                os.write(andbyte);
            }
        }

        BufferedReader in = null;
        try {
            if (conn.getInputStream() != null) {
                in = new BufferedReader(
                        new InputStreamReader(
                                conn.getInputStream(), "UTF-8"));
            }
        } catch (IOException e) {
            if (conn.getErrorStream() != null) {
                in = new BufferedReader(
                        new InputStreamReader(
                                conn.getErrorStream(), "UTF-8"));
            }
        }
        if (in == null) {
            throw new Exception("Unable to read response from server");
        }
        String ct = conn.getContentType();
        assertTrue(ct.contains(expectedCt));

        StringBuilder decodedString = new StringBuilder();
        String line;
        try {
            while ((line = in.readLine()) != null) {
                decodedString.append(line);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            in.close();
            in = null;
        }
        // get result code
        int responseCode = conn.getResponseCode();

        String response = decodedString.toString();

        assertEquals(expectedCode, responseCode);

        try {
            conn.getInputStream().close();
            conn.disconnect();
        } // ignore closing problems here
        catch (Exception e) {
        }
        conn = null;

        //System.err.println(response);
        return response;
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

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, false, 200 /* HTTP_OK */, ClientAuthScheme.HASH_SHA256);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, admin, 200 /* HTTP_OK */, ClientAuthScheme.HASH_SHA256);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin, int expectedCode, ClientAuthScheme scheme) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, admin, expectedCode /* HTTP_OK */, scheme, -1);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin, int expectedCode, ClientAuthScheme scheme, int procCallTimeout) throws Exception {
        // Call insert
        String paramsInJSON = pset.toJSONString();
        //System.out.println(paramsInJSON);
        HashMap<String, String> params = new HashMap<>();
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

        String varString = getHTTPVarString(params);

        varString = getHTTPVarString(params);

        String ret = callProcOverJSONRaw(varString, expectedCode);
        if (preHash) {
            //If prehash make same call with SHA1 to check expected code.
            params.put("Hashedpassword", getHashedPasswordForHTTPVar(password, ClientAuthScheme.HASH_SHA1));
            varString = getHTTPVarString(params);

            varString = getHTTPVarString(params);
            callProcOverJSONRaw(varString, expectedCode);
        }
        return ret;
    }

    public static Response responseFromJSON(String jsonStr) throws JSONException, IOException {
        Response response = new Response();
        JSONObject jsonObj = new JSONObject(jsonStr);
        JSONArray resultsJson = jsonObj.getJSONArray("results");
        response.results = new VoltTable[resultsJson.length()];
        for (int i = 0; i < response.results.length; i++) {
            JSONObject tableJson = resultsJson.getJSONObject(i);
            response.results[i] = VoltTable.fromJSONObject(tableJson);
        }
        if (jsonObj.isNull("status") == false) {
            response.status = (byte) jsonObj.getInt("status");
        }
        if (jsonObj.isNull("appstatus") == false) {
            response.appStatus = (byte) jsonObj.getInt("appstatus");
        }
        if (jsonObj.isNull("statusstring") == false) {
            response.statusString = jsonObj.getString("statusstring");
        }
        if (jsonObj.isNull("appstatusstring") == false) {
            response.appStatusString = jsonObj.getString("appstatusstring");
        }
        if (jsonObj.isNull("exception") == false) {
            response.exception = jsonObj.getString("exception");
        }

        return response;
    }

    public void testPausedMode() throws Exception {
        try {
            String testSchema
                    = "CREATE TABLE foo (\n"
                    + "  ival bigint default 23 not null, "
                    + "  sval varchar(200) default 'foo', "
                    + "  PRIMARY KEY (ival)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(testSchema);
            builder.addPartitionInfo("foo", "ival");
            builder.addStmtProcedure("fooinsert", "insert into foo values (?, ?);");
            builder.addStmtProcedure("foocount", "select count(*) from foo;");
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            int pkStart = 0;
            pkStart = runPauseTests(pkStart, false, false);
            pkStart = runPauseTests(pkStart, false, true);

            // pause server
            ParameterSet pset = ParameterSet.emptyParameterSet();
            Response response = responseFromJSON(callProcOverJSON("@Pause", pset, null, null, false, true));
            assertTrue(response.status == ClientResponse.SUCCESS);

            pkStart = runPauseTests(pkStart, true, false);
            pkStart = runPauseTests(pkStart, true, true);

            // resume server
            pset = ParameterSet.emptyParameterSet();
            response = responseFromJSON(callProcOverJSON("@Resume", pset, null, null, false, true));
            assertTrue(response.status == ClientResponse.SUCCESS);

            pkStart = runPauseTests(pkStart, false, false);
            pkStart = runPauseTests(pkStart, false, true);

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    private int runPauseTests(int pkStart, boolean paused, boolean useAdmin) throws Exception {
        ParameterSet pset;
        String responseJSON;
        Response response;

        pset = ParameterSet.fromArrayNoCopy(pkStart++, "hello");
        responseJSON = callProcOverJSON("fooinsert", pset, null, null, false, useAdmin);
        response = responseFromJSON(responseJSON);
        if (paused && !useAdmin) {
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, response.status);
            assertTrue(response.statusString.contains("is paused"));
            pkStart--;
        } else {
            assertEquals(ClientResponse.SUCCESS, response.status);
            assertEquals(1L, response.results[0].fetchRow(0).getLong(0));
        }

        pset = ParameterSet.emptyParameterSet();
        responseJSON = callProcOverJSON("foocount", pset, null, null, false, useAdmin);
        response = responseFromJSON(responseJSON);
        assertEquals(ClientResponse.SUCCESS, response.status);
        assertEquals(pkStart, response.results[0].fetchRow(0).getLong(0));

        // try AdHoc
        pset = ParameterSet.fromArrayNoCopy("insert into foo values (" + (pkStart++) + ", 'adhochello')");
        responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false, useAdmin);
        response = responseFromJSON(responseJSON);
        if (paused && !useAdmin) {
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, response.status);
            assertTrue(response.statusString.contains("is paused"));
            pkStart--;
        } else {
            assertEquals(ClientResponse.SUCCESS, response.status);
        }

        pset = ParameterSet.fromArrayNoCopy("select count(*) from foo");
        responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false, useAdmin);
        response = responseFromJSON(responseJSON);
        assertEquals(ClientResponse.SUCCESS, response.status);
        assertEquals(pkStart, response.results[0].fetchRow(0).getLong(0));

        return pkStart;
    }

    public void testaUpdateDeploymentSnmpConfig() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment
            String jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            Map<String,String> params = new HashMap<>();

            ObjectMapper mapper = new ObjectMapper();
            DeploymentType deptype = mapper.readValue(jdep, DeploymentType.class);

            SnmpType snmpConfig = new SnmpType();
            snmpConfig.setTarget("localhost");
            deptype.setSnmp(snmpConfig);
            String ndeptype = URLEncoder.encode(mapper.writeValueAsString(deptype), StandardCharsets.UTF_8.toString());
            params.put("deployment", ndeptype);
            String pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            DeploymentType gotValue = mapper.readValue(jdep, DeploymentType.class);
            assertEquals("public", gotValue.getSnmp().getCommunity());

            snmpConfig.setCommunity("foobar");
            deptype.setSnmp(snmpConfig);
            ndeptype = URLEncoder.encode(mapper.writeValueAsString(deptype), StandardCharsets.UTF_8.toString());
            params.put("deployment", ndeptype);
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            gotValue = mapper.readValue(jdep, DeploymentType.class);
            assertEquals("foobar", gotValue.getSnmp().getCommunity());

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testAJAXAndClientTogether() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            client = ClientFactory.createClient(new ClientConfig());
            client.createConnection("localhost");

            final AtomicLong fcnt = new AtomicLong(0);
            final AtomicLong scnt = new AtomicLong(0);
            final AtomicLong cfcnt = new AtomicLong(0);
            final AtomicLong cscnt = new AtomicLong(0);
            final int jsonRunnerCount = 50;
            final int clientRunnerCount = 50;
            final ParameterSet pset = ParameterSet.fromArrayNoCopy("select count(*) from foo");
            String responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            Response r = responseFromJSON(responseJSON);
            assertEquals(ClientResponse.SUCCESS, r.status);
            //Do replicated table read.
            class JSONRunner implements Runnable {

                @Override
                public void run() {
                    try {
                        String rresponseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
                        System.out.println("Response: " + rresponseJSON);
                        Response rr = responseFromJSON(rresponseJSON);
                        assertEquals(ClientResponse.SUCCESS, rr.status);
                        scnt.incrementAndGet();
                    } catch (Exception ex) {
                        fcnt.incrementAndGet();
                        ex.printStackTrace();
                    }
                }

            }

            //Do replicated table read.
            class ClientRunner implements Runnable {

                class Callback implements ProcedureCallback {

                    @Override
                    public void clientCallback(ClientResponse clientResponse) throws Exception {
                        if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                            cscnt.incrementAndGet();
                        } else {
                            System.out.println("Client failed: " + clientResponse.getStatusString());
                            cfcnt.incrementAndGet();
                        }
                    }

                }
                @Override
                public void run() {
                    try {
                        if (!client.callProcedure(new Callback(), "@AdHoc", "SELECT count(*) from foo")) {
                            cfcnt.decrementAndGet();
                        }
                    } catch (Exception ex) {
                        fcnt.incrementAndGet();
                        ex.printStackTrace();
                    }
                }

            }

            //Start runners
            ExecutorService es = CoreUtils.getBoundedSingleThreadExecutor("runners", jsonRunnerCount);
            for (int i = 0; i < jsonRunnerCount; i++) {
                es.submit(new JSONRunner());
            }
            ExecutorService ces = CoreUtils.getBoundedSingleThreadExecutor("crunners", clientRunnerCount);
            for (int i = 0; i < clientRunnerCount; i++) {
                ces.submit(new ClientRunner());
            }

            es.shutdown();
            es.awaitTermination(1, TimeUnit.DAYS);
            assertEquals(jsonRunnerCount, scnt.get());
            ces.shutdown();
            ces.awaitTermination(1, TimeUnit.DAYS);
            client.drain();
            assertEquals(clientRunnerCount, cscnt.get());
            responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            r = responseFromJSON(responseJSON);
            assertEquals(ClientResponse.SUCCESS, r.status);
            //Make sure we are still good.
            ClientResponse resp = client.callProcedure("@AdHoc", "SELECT count(*) from foo");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
            if (client != null) {
                client.close();
            }
        }
    }


    public void testAdminMode() throws Exception {
        try {
            String simpleSchema
                    = "create table blah ("
                    + "ival bigint default 23 not null, "
                    + "sval varchar(200) default 'foo', "
                    + "dateval timestamp, "
                    + "fval float, "
                    + "decval decimal, "
                    + "PRIMARY KEY(ival));";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltDB.Configuration config = new VoltDB.Configuration();

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("blah", "ival");
            builder.addStmtProcedure("Insert", "insert into blah values (?,?,?,?,?);");
            builder.addProcedures(CrazyBlahProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"), 1, 1, 0, 21213, true, 0);
            assertTrue(success);

            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();

            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            ParameterSet pset;
            String responseJSON;
            Response response;

            // Call insert on admin port
            pset = ParameterSet.fromArrayNoCopy(1, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false, true);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // Call insert on closed client port and expect failure
            pset = ParameterSet.fromArrayNoCopy(2, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SERVER_UNAVAILABLE);

            // open client port
            pset = ParameterSet.emptyParameterSet();
            responseJSON = callProcOverJSON("@Resume", pset, null, null, false, true);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // call insert on open client port
            pset = ParameterSet.fromArrayNoCopy(2, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // call insert on admin port again (now that both ports are open)
            pset = ParameterSet.fromArrayNoCopy(3, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false, true);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // put the system in admin mode
            pset = ParameterSet.emptyParameterSet();
            responseJSON = callProcOverJSON("@Pause", pset, null, null, false, true);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // Call insert on admin port
            pset = ParameterSet.fromArrayNoCopy(4, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false, true);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // Call insert on closed client port and expect failure
            pset = ParameterSet.fromArrayNoCopy(5, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SERVER_UNAVAILABLE);
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testSimple() throws Exception {
        try {
            String simpleSchema
                    = "create table blah ("
                    + "ival bigint default 23 not null, "
                    + "sval varchar(200) default 'foo', "
                    + "dateval timestamp, "
                    + "fval float, "
                    + "decval decimal, "
                    + "PRIMARY KEY(ival));";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltDB.Configuration config = new VoltDB.Configuration();

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("blah", "ival");
            builder.addStmtProcedure("Insert", "insert into blah values (?,?,?,?,?);");
            builder.addProcedures(CrazyBlahProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"), 1, 1, 0, 21213, false, 0);
            assertTrue(success);

            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();

            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            ParameterSet pset;
            String responseJSON;
            Response response;

            // Call insert
            pset = ParameterSet.fromArrayNoCopy(1, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
            responseJSON = callProcOverJSON("Insert", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status == ClientResponse.SUCCESS);

            // Call insert again (with failure expected)
            responseJSON = callProcOverJSON("Insert", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertTrue(response.status != ClientResponse.SUCCESS);

            // Call proc with complex params
            pset = ParameterSet.fromArrayNoCopy(1,
                    5,
                    new double[]{1.5, 6.0, 4},
                    new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                    new BigDecimal(5),
                    new BigDecimal[]{},
                    new TimestampType(System.currentTimeMillis()));

            responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertEquals(ClientResponse.SUCCESS, response.status);

            // check the JSON itself makes sense
            JSONObject jsonObj = new JSONObject(responseJSON);
            JSONArray results = jsonObj.getJSONArray("results");
            assertEquals(4, response.results.length);
            JSONObject table = results.getJSONObject(0);
            JSONArray data = table.getJSONArray("data");
            assertEquals(1, data.length());
            JSONArray row = data.getJSONArray(0);
            assertEquals(1, row.length());
            long value = row.getLong(0);
            assertEquals(1, value);

            // try to pass a string as a date
            java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
            ts.setNanos(123456000);
            pset = ParameterSet.fromArrayNoCopy(1,
                    5,
                    new double[]{1.5, 6.0, 4},
                    new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                    new BigDecimal(5),
                    new BigDecimal[]{},
                    ts.toString());

            responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertEquals(ClientResponse.SUCCESS, response.status);
            response.results[3].advanceRow();
            System.out.println(response.results[3].getTimestampAsTimestamp(0).getTime());
            assertEquals(123456, response.results[3].getTimestampAsTimestamp(0).getTime() % 1000000);

            // now try a null short value sent as a int  (param expects short)
            pset = ParameterSet.fromArrayNoCopy(1,
                    VoltType.NULL_SMALLINT,
                    new double[]{1.5, 6.0, 4},
                    new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                    new BigDecimal(5),
                    new BigDecimal[]{},
                    new TimestampType(System.currentTimeMillis()));

            responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertFalse(response.status == ClientResponse.SUCCESS);

            // now try an out of range long value (param expects short)
            pset = ParameterSet.fromArrayNoCopy(1,
                    Long.MAX_VALUE - 100,
                    new double[]{1.5, 6.0, 4},
                    new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                    new BigDecimal(5),
                    new BigDecimal[]{},
                    new TimestampType(System.currentTimeMillis()));

            responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            assertFalse(response.status == ClientResponse.SUCCESS);

            // now try bigdecimal with small value
            pset = ParameterSet.fromArrayNoCopy(1,
                    4,
                    new double[]{1.5, 6.0, 4},
                    new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                    5,
                    new BigDecimal[]{},
                    new TimestampType(System.currentTimeMillis()));

            responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            System.out.println(response.statusString);
            assertEquals(ClientResponse.SUCCESS, response.status);

            // now try null
            pset = ParameterSet.fromArrayNoCopy(1,
                    4,
                    new double[]{1.5, 6.0, 4},
                    new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                    5,
                    new BigDecimal[]{},
                    null);

            responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            System.out.println(response.statusString);
            assertEquals(ClientResponse.SUCCESS, response.status);

            // now try jsonp
            responseJSON = callProcOverJSONRaw("Procedure=@Statistics&Parameters=[TABLE]&jsonp=fooBar", 200);
            System.out.println(responseJSON);
            assertTrue(responseJSON.startsWith("fooBar("));

            // now try adhoc
            pset = ParameterSet.fromArrayNoCopy("select * from blah");
            responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            System.out.println(response.statusString);
            assertEquals(ClientResponse.SUCCESS, response.status);

            // now try adhoc insert with a huge bigint
            pset = ParameterSet.fromArrayNoCopy("insert into blah values (974599638818488300, NULL, NULL, NULL, NULL);");
            responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            System.out.println(response.statusString);
            assertEquals(ClientResponse.SUCCESS, response.status);

            pset = ParameterSet.fromArrayNoCopy("select * from blah where ival = 974599638818488300;");
            responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            System.out.println(responseJSON);
            response = responseFromJSON(responseJSON);
            System.out.println(response.statusString);
            assertEquals(ClientResponse.SUCCESS, response.status);
            assertEquals(1, response.results.length);
            assertEquals(1, response.results[0].getRowCount());

            // Call @AdHoc with zero parameters
            pset = ParameterSet.emptyParameterSet();
            responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            assertTrue(responseJSON.contains("Adhoc system procedure requires at least the query parameter."));

            // Call @AdHoc with many parameters (more than 2)
            pset = ParameterSet.fromArrayNoCopy("select * from blah", "foo", "bar");
            responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            System.err.println(responseJSON);
            assertTrue(responseJSON.contains("Too many actual arguments were passed for the parameters in the sql "
                    + "statement(s): (2 vs. 0)"));

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testJapaneseNastiness() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE HELLOWORLD (\n"
                    + "    HELLO VARCHAR(15),\n"
                    + "    WORLD VARCHAR(15),\n"
                    + "    DIALECT VARCHAR(15) NOT NULL,\n"
                    + "    PRIMARY KEY (DIALECT)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("HELLOWORLD", "DIALECT");
            builder.addStmtProcedure("Insert", "insert into HELLOWORLD values (?,?,?);");
            builder.addStmtProcedure("Select", "select * from HELLOWORLD;");
            builder.addProcedures(SelectStarHelloWorld.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            String response = callProcOverJSONRaw(japaneseTestVarStrings, 200);
            Response r = responseFromJSON(response);
            assertEquals(1, r.status);

        // If this line doesn't compile, right click the file in the package explorer.
            // Select the properties menu. Set the text file encoding to UTF-8.
            char[] test1 = {'こ', 'ん', 'に', 'ち', 'は'};
            String test2 = new String(test1);

            ParameterSet pset = ParameterSet.emptyParameterSet();
            response = callProcOverJSON("Select", pset, null, null, false);
            System.out.println(response);
            System.out.println(test2);
            r = responseFromJSON(response);
            assertEquals(1, r.status);

            response = callProcOverJSON("SelectStarHelloWorld", pset, null, null, false);
            r = responseFromJSON(response);
            assertEquals(1, r.status);
            assertTrue(response.contains(test2));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testJSONAuth() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE HELLOWORLD (\n"
                    + "    HELLO VARCHAR(15),\n"
                    + "    WORLD VARCHAR(20),\n"
                    + "    DIALECT VARCHAR(15) NOT NULL,\n"
                    + "    PRIMARY KEY (DIALECT)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("HELLOWORLD", "DIALECT");

            RoleInfo gi = new RoleInfo("foo", true, false, true, true, false, false);
            builder.addRoles(new RoleInfo[]{gi});

            // create 20 users, only the first one has an interesting user/pass
            UserInfo[] ui = new UserInfo[15];
            ui[0] = new UserInfo("ry@nlikesthe", "y@nkees", new String[]{"foo"});
            for (int i = 1; i < ui.length; i++) {
                ui[i] = new UserInfo("USER" + String.valueOf(i), "PASS" + String.valueOf(i), new String[]{"foo"});
            }
            builder.addUsers(ui);

            builder.setSecurityEnabled(true, true);

            ProcedureInfo[] pi = new ProcedureInfo[2];
            pi[0] = new ProcedureInfo(new String[]{"foo"}, "Insert", "insert into HELLOWORLD values (?,?,?);", null);
            pi[1] = new ProcedureInfo(new String[]{"foo"}, "Select", "select * from HELLOWORLD;", null);
            builder.addProcedures(pi);

            builder.setHTTPDPort(8095);

            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            ParameterSet pset;

            // test good auths
            for (UserInfo u : ui) {
                pset = ParameterSet.fromArrayNoCopy(u.name, u.password, u.name);
                String response = callProcOverJSON("Insert", pset, u.name, u.password, true);
                Response r = responseFromJSON(response);
                assertEquals(ClientResponse.SUCCESS, r.status);
            }
            // test re-using auths
            for (UserInfo u : ui) {
                pset = ParameterSet.fromArrayNoCopy(u.name + "-X", u.password + "-X", u.name + "-X");
                String response = callProcOverJSON("Insert", pset, u.name, u.password, false);
                Response r = responseFromJSON(response);
                assertEquals(ClientResponse.SUCCESS, r.status);
            }

            // test bad auth
            UserInfo u = ui[0];
            pset = ParameterSet.fromArrayNoCopy(u.name + "-X1", u.password + "-X1", u.name + "-X1");
            String response = callProcOverJSON("Insert", pset, u.name, "ick", true);
            Response r = responseFromJSON(response);
            assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
            response = callProcOverJSON("Insert", pset, u.name, "ick", false);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);

            // test malformed auth (too short hash)
            pset = ParameterSet.fromArrayNoCopy(u.name + "-X2", u.password + "-X2", u.name + "-X2");
            String paramsInJSON = pset.toJSONString();
            HashMap<String, String> params = new HashMap<>();
            params.put("Procedure", "Insert");
            params.put("Parameters", paramsInJSON);
            params.put("User", u.name);
            params.put("Password", Encoder.hexEncode(new byte[]{1, 2, 3}));
            String varString = getHTTPVarString(params);
            response = callProcOverJSONRaw(varString, 200);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);

            // test malformed auth (gibberish password, but good length)
            pset = ParameterSet.fromArrayNoCopy(u.name + "-X3", u.password + "-X3", u.name + "-X3");
            paramsInJSON = pset.toJSONString();
            params = new HashMap<>();
            params.put("Procedure", "Insert");
            params.put("Parameters", paramsInJSON);
            params.put("User", u.name);
            params.put("Password", "abcdefghiabcdefghiabcdefghiabcdefghi");
            varString = getHTTPVarString(params);
            response = callProcOverJSONRaw(varString, 200);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);

            // the update catalog test below is for enterprise only
            if (VoltDB.instance().getConfig().m_isEnterprise == false) {
                return;
            }

        // ENG-963 below here
            // do enough to get a new deployment file
            VoltProjectBuilder builder2 = new VoltProjectBuilder();
            builder2.addSchema(schemaPath);
            builder2.addPartitionInfo("HELLOWORLD", "DIALECT");

            // Same groups
            builder2.addRoles(new RoleInfo[]{gi});

            // create same 15 users, hack the last 14 passwords
            ui = new UserInfo[15];
            ui[0] = new UserInfo("ry@nlikesthe", "y@nkees", new String[]{"foo"});
            for (int i = 1; i < ui.length; i++) {
                ui[i] = new UserInfo("USER" + String.valueOf(i),
                        "welcomehackers" + String.valueOf(i),
                        new String[]{"foo"});
            }
            builder2.addUsers(ui);

            builder2.setSecurityEnabled(true, true);
            builder2.addProcedures(pi);
            builder2.setHTTPDPort(8095);

            success = builder2.compile(Configuration.getPathToCatalogForTest("json-update.jar"));
            assertTrue(success);

            pset = ParameterSet.fromArrayNoCopy(Encoder.hexEncode(MiscUtils.fileToBytes(new File(config.m_pathToCatalog))),
                    new String(MiscUtils.fileToBytes(new File(builder2.getPathToDeployment())), "UTF-8"));
            response = callProcOverJSON("@UpdateApplicationCatalog", pset,
                    ui[0].name, ui[0].password, true);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);

            // retest the good auths above
            for (UserInfo user : ui) {
                ParameterSet ps = ParameterSet.fromArrayNoCopy(user.name + "-X3", user.password + "-X3", user.name + "-X3");
                String respstr = callProcOverJSON("Insert", ps, user.name, user.password, false);
                Response resp = responseFromJSON(respstr);
                assertEquals(ClientResponse.SUCCESS, resp.status);
            }

            VoltProjectBuilder builder3 = new VoltProjectBuilder();
            builder3.addSchema(schemaPath);
            builder3.addPartitionInfo("HELLOWORLD", "DIALECT");

            // Same groups
            builder3.addRoles(new RoleInfo[]{gi});

            ui = new UserInfo[1];
            ui[0] = new UserInfo("ry@nlikesthe",
                    "D033E22AE348AEB5660FC2140AEC35850C4DA9978C6976E5B5410415BDE908BD4DEE15DFB167A9C873FC4BB8A81F6F2AB448A918",
                    new String[]{"foo"}, false);
            builder3.addUsers(ui);

            builder3.setSecurityEnabled(true, true);
            builder3.addProcedures(pi);
            builder3.setHTTPDPort(8095);

            success = builder3.compile(Configuration.getPathToCatalogForTest("json-update.jar"));
            assertTrue(success);

            pset = ParameterSet.fromArrayNoCopy(Encoder.hexEncode(MiscUtils.fileToBytes(new File(config.m_pathToCatalog))),
                    new String(MiscUtils.fileToBytes(new File(builder3.getPathToDeployment())), "UTF-8"));
            response = callProcOverJSON("@UpdateApplicationCatalog", pset,
                    "ry@nlikesthe", "y@nkees", true);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);

            // retest the good auths above
            ParameterSet ps = ParameterSet.fromArrayNoCopy(ui[0].name + "-X4", "admin-X4", ui[0].name + "-X4");
            String respstr = callProcOverJSON("Insert", ps, ui[0].name, "admin", false);
            Response resp = responseFromJSON(respstr);
            assertEquals(ClientResponse.SUCCESS, resp.status);
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testJSONDisabled() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE HELLOWORLD (\n"
                    + "    HELLO VARCHAR(15),\n"
                    + "    WORLD VARCHAR(15),\n"
                    + "    DIALECT VARCHAR(15) NOT NULL,\n"
                    + "    PRIMARY KEY (DIALECT)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("HELLOWORLD", "DIALECT");

            builder.addStmtProcedure("Insert", "insert into HELLOWORLD values (?,?,?);");

            builder.setHTTPDPort(8095);
            builder.setJSONAPIEnabled(false);

            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            // test not enabled
            ParameterSet pset = ParameterSet.fromArrayNoCopy("foo", "bar", "foobar");
            try {
                callProcOverJSON("Insert", pset, null, null, false, false, 403, ClientAuthScheme.HASH_SHA256); // HTTP_FORBIDDEN
            } catch (Exception e) {
                // make sure failed due to permissions on http
                assertTrue(e.getMessage().contains("403"));
            }
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testLongProc() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            ParameterSet pset = ParameterSet.fromArrayNoCopy(14_000);
            String response = callProcOverJSON("DelayProc", pset, null, null, false);
            Response r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testProcTimeout() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(InsertProc.class);
            builder.addProcedures(LongReadProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            ParameterSet pset = null;

            int batchSize = 10000;
            for (int i=0; i<10; i++) {
                pset = ParameterSet.fromArrayNoCopy(i*batchSize, batchSize);
                String response = callProcOverJSON("TestJSONInterface$InsertProc", pset, null, null, false, false, 200, ClientAuthScheme.HASH_SHA256);
                Response r = responseFromJSON(response);
                assertEquals(ClientResponse.SUCCESS, r.status);
            }

            pset = ParameterSet.fromArrayNoCopy(100000);
            String response = callProcOverJSON("TestJSONInterface$LongReadProc", pset, null, null, false, false, 200, ClientAuthScheme.HASH_SHA256, 1);
            Response r = responseFromJSON(response);
            assertEquals(ClientResponse.GRACEFUL_FAILURE, r.status);
            assertTrue(r.statusString.contains("Transaction Interrupted"));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public static class InsertProc extends VoltProcedure {

        public final SQLStmt stmt = new SQLStmt("INSERT INTO foo values (?);");

        public long run(int startValue, long numRows) {
            for (int i=0; i<numRows; i++) {
                voltQueueSQL(stmt, i+startValue);
            }
            voltExecuteSQL();
            return 1;
        }

    }

    public static class LongReadProc extends VoltProcedure {

        public final SQLStmt stmt = new SQLStmt("SELECT * FROM foo");

        public long run(long numLoops) {
            voltQueueSQL(stmt);
            voltExecuteSQL();
            return 1;
        }

    }

    public void testLongQuerySTring() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //create a large query string
            final StringBuilder s = new StringBuilder();
            s.append("Procedure=@Statistics&Parameters=[TABLE]&jsonpxx=");
            for (int i = 0; i < 450000; i++) {
                s.append(i);
            }
            String query = s.toString();
            //call multiple times.
            for (int i = 0; i < 500; i++) {
                String response = callProcOverJSONRaw(query, 200);
                System.out.println(response);
                Response r = responseFromJSON(response);
                assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
                //make sure good queries can still work.
                ParameterSet pset = ParameterSet.fromArrayNoCopy("select * from foo");
                String responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
                System.out.println(responseJSON);
                r = responseFromJSON(responseJSON);
                assertEquals(ClientResponse.SUCCESS, r.status);
            }
            //make sure good queries can still work after.
            ParameterSet pset = ParameterSet.fromArrayNoCopy("select * from foo");
            String responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
            System.out.println(responseJSON);
            Response response = responseFromJSON(responseJSON);
            System.out.println(response.statusString);
            assertEquals(ClientResponse.SUCCESS, response.status);

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testBinaryProc() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    b VARBINARY(256) DEFAULT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.addPartitionInfo("foo", "bar");
            builder.addStmtProcedure("Insert", "insert into foo values (?, ?);");
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            // try a good insert
            String varString = "Procedure=Insert&Parameters=[5,\"aa\"]";
            String response = callProcOverJSONRaw(varString, 200);
            System.out.println(response);
            Response r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);

            // try two poorly hex-encoded inserts
            varString = "Procedure=Insert&Parameters=[6,\"aaa\"]";
            response = callProcOverJSONRaw(varString, 200);
            System.out.println(response);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.GRACEFUL_FAILURE, r.status);
            varString = "Procedure=Insert&Parameters=[7,\"aaay\"]";
            response = callProcOverJSONRaw(varString, 200);
            System.out.println(response);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.GRACEFUL_FAILURE, r.status);

            // try null binary inserts
            varString = "Procedure=Insert&Parameters=[8,NULL]";
            response = callProcOverJSONRaw(varString, 200);
            System.out.println(response);
            r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testGarbageProcs() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            callProcOverJSONRaw(getHTTPURL(null, "api/1.0/Tim"), 400);
            callProcOverJSONRaw(getHTTPURL(null, "api/1.0/Tim?Procedure=foo&Parameters=[x4{]"), 400);
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testDeployment() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment
            String jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            assertTrue(jdep.contains("cluster"));
            //Download deployment
            String xdep = getUrlOverJSON("http://localhost:8095/deployment/download", null, null, null, 200, "text/xml");
            assertTrue(xdep.contains("<deployment>"));
            assertTrue(xdep.contains("</deployment>"));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testUpdateDeployment() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment
            String jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            assertTrue(jdep.contains("cluster"));
            //POST deployment with no content
            String pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", null);
            assertTrue(pdep.contains("Failed"));
            Map<String,String> params = new HashMap<>();
            params.put("deployment", URLEncoder.encode(jdep, "UTF-8"));
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            assertTrue(pdep.contains("Deployment Updated"));

            //POST deployment in admin mode
            params.put("admin", "true");
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            assertTrue(pdep.contains("Deployment Updated"));

            ObjectMapper mapper = new ObjectMapper();
            DeploymentType deptype = mapper.readValue(jdep, DeploymentType.class);

            //Test change heartbeat.
            if (deptype.getHeartbeat() == null) {
                HeartbeatType hb = new HeartbeatType();
                hb.setTimeout(99);
                deptype.setHeartbeat(hb);
            } else {
                deptype.getHeartbeat().setTimeout(99);
            }
            String ndeptype = mapper.writeValueAsString(deptype);
            params.put("deployment", URLEncoder.encode(ndeptype, "UTF-8"));
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            System.out.println("POST result is: " + pdep);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            assertTrue(jdep.contains("cluster"));
            deptype = mapper.readValue(jdep, DeploymentType.class);
            int nto = deptype.getHeartbeat().getTimeout();
            assertEquals(99, nto);

            //Test change Query timeout
            SystemSettingsType ss = deptype.getSystemsettings();
            if (ss == null) {
                ss = new SystemSettingsType();
                deptype.setSystemsettings(ss);
            }
            Query qv = ss.getQuery();
            if (qv == null) {
                qv = new Query();
                qv.setTimeout(99);
            } else {
                qv.setTimeout(99);
            }
            ss.setQuery(qv);
            deptype.setSystemsettings(ss);
            ndeptype = mapper.writeValueAsString(deptype);
            params.put("deployment", URLEncoder.encode(ndeptype, "UTF-8"));
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            System.out.println("POST result is: " + pdep);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            assertTrue(jdep.contains("cluster"));
            deptype = mapper.readValue(jdep, DeploymentType.class);
            nto = deptype.getSystemsettings().getQuery().getTimeout();
            assertEquals(99, nto);

            qv.setTimeout(88);
            ss.setQuery(qv);
            deptype.setSystemsettings(ss);
            ndeptype = mapper.writeValueAsString(deptype);
            params.put("deployment", URLEncoder.encode(ndeptype, "UTF-8"));
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            System.out.println("POST result is: " + pdep);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            assertTrue(jdep.contains("cluster"));
            deptype = mapper.readValue(jdep, DeploymentType.class);
            nto = deptype.getSystemsettings().getQuery().getTimeout();
            assertEquals(88, nto);

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testUpdateDeploymentMemoryLimit() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment
            String jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            Map<String,String> params = new HashMap<>();

            ObjectMapper mapper = new ObjectMapper();
            DeploymentType deptype = mapper.readValue(jdep, DeploymentType.class);

            SystemSettingsType ss = deptype.getSystemsettings();
            if (ss == null) {
                ss = new SystemSettingsType();
                deptype.setSystemsettings(ss);
            }

            ResourceMonitorType resourceMonitor = new ResourceMonitorType();
            Memorylimit memLimit = new Memorylimit();
            memLimit.setSize("10");
            memLimit.setAlert("5");
            resourceMonitor.setMemorylimit(memLimit);
            ss.setResourcemonitor(resourceMonitor);
            String ndeptype = mapper.writeValueAsString(deptype);
            params.put("deployment", ndeptype);
            String pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            DeploymentType gotValue = mapper.readValue(jdep, DeploymentType.class);
            assertEquals("10", gotValue.getSystemsettings().getResourcemonitor().getMemorylimit().getSize());

            memLimit.setSize("90%25");
            ndeptype = mapper.writeValueAsString(deptype);
            params.put("deployment", ndeptype);
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            assertTrue(pdep.contains("Deployment Updated"));
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            gotValue = mapper.readValue(jdep, DeploymentType.class);
            assertEquals("90%", gotValue.getSystemsettings().getResourcemonitor().getMemorylimit().getSize());

            // decimal values not allowed for percentages
            memLimit.setSize("90.5%25");
            ndeptype = mapper.writeValueAsString(deptype);
            params.put("deployment", ndeptype);
            pdep = postUrlOverJSON("http://localhost:8095/deployment/", null, null, null, 200, "application/json", params);
            jdep = getUrlOverJSON("http://localhost:8095/deployment", null, null, null, 200,  "application/json");
            gotValue = mapper.readValue(jdep, DeploymentType.class);
            // must be still the old value
            assertEquals("90%", gotValue.getSystemsettings().getResourcemonitor().getMemorylimit().getSize());

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testDeploymentSecurity() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            UserInfo users[] = new UserInfo[] {
                    new UserInfo("user1", "admin", new String[] {"user"}),
                    new UserInfo("user2", "admin", new String[] {"administrator"}),
                    new UserInfo("user3", "admin", new String[] {"administrator"}), //user3 used for both hash testing.
            };
            builder.addUsers(users);

            // suite defines its own ADMINISTRATOR user
            builder.setSecurityEnabled(true, false);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment with diff hashed password
            //20E3AAE7FC23385295505A6B703FD1FBA66760D5 FD19534FBF9B75DF7CD046DE3EAF93DB77367CA7C1CC017FFA6CED2F14D32E7D
            //D033E22AE348AEB5660FC2140AEC35850C4DA997 8C6976E5B5410415BDE908BD4DEE15DFB167A9C873FC4BB8A81F6F2AB448A918
            //sha-256
            String dep = getUrlOverJSON("http://localhost:8095/deployment/?User=" + "user3&" + "Hashedpassword=8C6976E5B5410415BDE908BD4DEE15DFB167A9C873FC4BB8A81F6F2AB448A918", null, null, null, 200, "application/json");
            assertTrue(dep.contains("cluster"));
            //sha-1
            dep = getUrlOverJSON("http://localhost:8095/deployment/?User=" + "user3&" + "Hashedpassword=D033E22AE348AEB5660FC2140AEC35850C4DA997", null, null, null, 200, "application/json");
            assertTrue(dep.contains("cluster"));

            //Get deployment bad user
            dep = getUrlOverJSON("http://localhost:8095/deployment/?User=" + "user1&" + "Hashedpassword=d033e22ae348aeb5660fc2140aec35850c4da997", null, null, null, 200, "application/json");
            assertTrue(dep.contains("Permission denied"));
            //good user
            dep = getUrlOverJSON("http://localhost:8095/deployment/?User=" + "user2&" + "Hashedpassword=d033e22ae348aeb5660fc2140aec35850c4da997", null, null, null, 200, "application/json");
            assertTrue(dep.contains("cluster"));
            //Download deployment bad user
            dep = getUrlOverJSON("http://localhost:8095/deployment/download?User=" + "user1&" + "Hashedpassword=d033e22ae348aeb5660fc2140aec35850c4da997", null, null, null, 200, "application/json");
            assertTrue(dep.contains("Permission denied"));
            //good user
            dep = getUrlOverJSON("http://localhost:8095/deployment/download?User=" + "user2&" + "Hashedpassword=d033e22ae348aeb5660fc2140aec35850c4da997", null, null, null, 200, "text/xml");
            assertTrue(dep.contains("<deployment>"));
            assertTrue(dep.contains("</deployment>"));
            //get with jsonp
            dep = getUrlOverJSON("http://localhost:8095/deployment/?User=" + "user2&" + "Hashedpassword=d033e22ae348aeb5660fc2140aec35850c4da997&jsonp=jackson5", null, null, null, 200, "application/json");
            assertTrue(dep.contains("cluster"));
            assertTrue(dep.contains("jackson5"));
            assertTrue(dep.matches("^jackson5(.*)"));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testDeploymentSecurityAuthorizationHashed() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            UserInfo users[] = new UserInfo[] {
                    new UserInfo("user1", "admin", new String[] {"user"}),
                    new UserInfo("user2", "admin", new String[] {"administrator"}),
            };
            builder.addUsers(users);

            // suite defines its own ADMINISTRATOR user
            builder.setSecurityEnabled(true, false);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment bad user
            String dep = getUrlOverJSON("http://localhost:8095/deployment/", "user1", "admin", "hashed", 200, "application/json");
            assertTrue(dep.contains("Permission denied"));
            //good user
            dep = getUrlOverJSON("http://localhost:8095/deployment/", "user2", "admin", "hashed", 200, "application/json");
            assertTrue(dep.contains("cluster"));
            //Download deployment bad user
            dep = getUrlOverJSON("http://localhost:8095/deployment/download", "user1", "admin", "hashed", 200, "application/json");
            assertTrue(dep.contains("Permission denied"));
            //good user
            dep = getUrlOverJSON("http://localhost:8095/deployment/download", "user2", "admin", "hashed", 200, "text/xml");
            assertTrue(dep.contains("<deployment>"));
            assertTrue(dep.contains("</deployment>"));
            dep = getUrlOverJSON("http://localhost:8095/deployment/download", "user2", "admin", "hashed256", 200, "text/xml");
            assertTrue(dep.contains("<deployment>"));
            assertTrue(dep.contains("</deployment>"));
            //Test back with sha1
            dep = getUrlOverJSON("http://localhost:8095/deployment/download", "user2", "admin", "hashed", 200, "text/xml");
            assertTrue(dep.contains("<deployment>"));
            assertTrue(dep.contains("</deployment>"));

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testDeploymentSecurityAuthorizationBasic() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            UserInfo users[] = new UserInfo[] {
                    new UserInfo("user1", "admin", new String[] {"user"}),
                    new UserInfo("user2", "admin", new String[] {"administrator"}),
            };
            builder.addUsers(users);

            // suite defines its own ADMINISTRATOR user
            builder.setSecurityEnabled(true, false);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get deployment bad user
            String dep = getUrlOverJSON("http://localhost:8095/deployment/", "user1", "admin", "basic", 200, "application/json");
            assertTrue(dep.contains("Permission denied"));
            //good user
            dep = getUrlOverJSON("http://localhost:8095/deployment/", "user2", "admin", "basic", 200, "application/json");
            assertTrue(dep.contains("cluster"));
            //Download deployment bad user
            dep = getUrlOverJSON("http://localhost:8095/deployment/download", "user1", "admin", "basic", 200, "application/json");
            assertTrue(dep.contains("Permission denied"));
            //good user
            dep = getUrlOverJSON("http://localhost:8095/deployment/download", "user2", "admin", "basic", 200, "text/xml");
            assertTrue(dep.contains("<deployment>"));
            assertTrue(dep.contains("</deployment>"));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testUsers() throws Exception {
        try {
            String simpleSchema
            = "CREATE TABLE foo (\n"
            + "    bar BIGINT NOT NULL,\n"
            + "    PRIMARY KEY (bar)\n"
            + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            builder.setUseDDLSchema(true);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get users
            String json = getUrlOverJSON("http://localhost:8095/deployment/users/", null, null, null, 200,  "application/json");
            assertEquals(json, "");
            getUrlOverJSON("http://localhost:8095/deployment/users/foo", null, null, null, 404,  "application/json");

            //Put users
            ObjectMapper mapper = new ObjectMapper();
            UsersType.User user = new UsersType.User();
            user.setName("foo");
            user.setPassword("foo");
            String map = mapper.writeValueAsString(user);
            Map<String,String> params = new HashMap<>();
            params.put("user", map);
            putUrlOverJSON("http://localhost:8095/deployment/users/foo/", null, null, null, 201,  "application/json", params);

            //Get users
            json = getUrlOverJSON("http://localhost:8095/deployment/users/", null, null, null, 200,  "application/json");
            JSONArray jarray = new JSONArray(json);
            assertEquals(jarray.length(), 1);
            JSONObject jobj = jarray.getJSONObject(0);
            assertTrue(jobj.getString("id").contains("/deployment/users/foo"));
            assertTrue(jobj.getString("roles").equalsIgnoreCase("null"));

            //Post users
            user.setRoles("foo");
            map = mapper.writeValueAsString(user);
            params.put("user", map);
            postUrlOverJSON("http://localhost:8095/deployment/users/foo/", null, null, null, 200,  "application/json", params);

            //Get users
            json = getUrlOverJSON("http://localhost:8095/deployment/users/", null, null, null, 200,  "application/json");
            jarray = new JSONArray(json);
            assertEquals(jarray.length(), 1);
            jobj = jarray.getJSONObject(0);
            assertTrue(jobj.getString("roles").equals("foo"));

            //Delete users
            deleteUrlOverJSON("http://localhost:8095/deployment/users/foo/", null, null, null, 204,  "application/json");

            //Get users
            json = getUrlOverJSON("http://localhost:8095/deployment/users/", null, null, null, 200,  "application/json");
            assertEquals(json, "");
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testExportTypes() throws Exception {
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get exportTypes
            String json = getUrlOverJSON("http://localhost:8095/deployment/export/type", null, null, null, 200,  "application/json");
            JSONObject jobj = new JSONObject(json);
            assertTrue(jobj.getString("types").contains("FILE"));
            assertTrue(jobj.getString("types").contains("JDBC"));
            assertTrue(jobj.getString("types").contains("KAFKA"));
            assertTrue(jobj.getString("types").contains("HTTP"));
            assertTrue(jobj.getString("types").contains("RABBITMQ"));
            assertTrue(jobj.getString("types").contains("CUSTOM"));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testProfile() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
            String schemaPath = schemaFile.getPath();
            schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addSchema(schemaPath);
            builder.addPartitionInfo("foo", "bar");
            builder.addProcedures(DelayProc.class);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            //Get profile
            String dep = getUrlOverJSON("http://localhost:8095/profile", null, null, null, 200, "application/json");
            assertTrue(dep.contains("\"user\""));
            assertTrue(dep.contains("\"permissions\""));
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    public void testConnectionsWithUpdateCatalog() throws Exception {
        runConnectionsWithUpdateCatalog(false);
    }

    public void testConnectionsWithUpdateCatalogWithSecurity() throws Exception {
        runConnectionsWithUpdateCatalog(true);
    }

    public void runConnectionsWithUpdateCatalog(boolean securityOn) throws Exception {
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
            builder.addProcedures(WorkerProc.class);
            UserInfo[] ui = new UserInfo[5];
            if (securityOn) {
                RoleInfo ri = new RoleInfo("role1", true, false, true, true, false, false);
                builder.addRoles(new RoleInfo[] { ri });

                for (int i = 0; i < ui.length; i++) {
                    ui[i] = new UserInfo("user" + String.valueOf(i), "password" + String.valueOf(i), new String[] { "role1" } );
                }
                builder.addUsers(ui);

                builder.setSecurityEnabled(true, true);
            }
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            TestWorker.s_insertCount = new AtomicLong(0);
            int poolSize = 25;
            ExecutorService executor = Executors.newFixedThreadPool(poolSize);
            int workCount = 200;
            for (int i=0; i<workCount; i++) {
                executor.execute(new TestWorker(i, workCount/10,
                        (securityOn ? ui[workCount%ui.length].name : null),
                        (securityOn ? ui[workCount%ui.length].password : null) ));
            }

            // wait for everything to be done and check status
            executor.shutdown();
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                fail("Workers should have finished execution by now");
            }
            assertTrue(TestWorker.s_success);
        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
        }
    }

    private static class TestWorker implements Runnable {

        public static boolean s_success = true; // this is set by worker threads on failure only
        public static AtomicLong s_insertCount = new AtomicLong(0);

        private final int m_id;
        private final int m_catUpdateCount;
        private final String m_username;
        private final String m_password;

        public TestWorker(int id, int catUpdateCount, String username, String password) {
            m_id = id;
            m_catUpdateCount = catUpdateCount;
            m_username = username;
            m_password = password;
        }

        @Override
        public void run()
        {
            try {
                if (m_id==0) { // do all deployment update from one thread to avoid version error on server side
                    for (int i=0; i<m_catUpdateCount; i++) {
                        // update deployment to force a catalog update and resetting connections
                        String jdep = getUrlOverJSON("http://localhost:8095/deployment", m_username, m_password, "hashed", 200,  "application/json");
                        ObjectMapper mapper = new ObjectMapper();
                        DeploymentType deptype = mapper.readValue(jdep, DeploymentType.class);
                        int timeout = 100 + m_id;
                        if (deptype.getHeartbeat() == null) {
                            HeartbeatType hb = new HeartbeatType();
                            hb.setTimeout(timeout);
                            deptype.setHeartbeat(hb);
                        } else {
                            deptype.getHeartbeat().setTimeout(timeout);
                        }
                        Map<String,String> params = new HashMap<>();
                        params.put("deployment", URLEncoder.encode(mapper.writeValueAsString(deptype), "UTF-8"));
                        params.put("admin", "true");
                        String responseJSON = postUrlOverJSON("http://localhost:8095/deployment/", m_username, m_password, "hashed", 200, "application/json", params);
                        if (!responseJSON.contains("Deployment Updated.")) {
                            System.out.println("Failed to update deployment");
                            s_success = false;
                        }
                    }
                } else {
                    // do a write and a read
                    ParameterSet pset = ParameterSet.fromArrayNoCopy("insert into test1 values (" + (m_id) + ")");
                    String responseJSON = callProcOverJSON("@AdHoc", pset, m_username, m_password, false, false);
                    //System.out.println("Insert response: " + responseJSON);
                    if (!responseJSON.contains("\"data\":[[1]]")) {
                        System.out.println("Insert should have returned 1. Got: " + responseJSON);
                        s_success = false;
                        return;
                    }
                    s_insertCount.incrementAndGet();
                    Thread.sleep(200);
                    pset = ParameterSet.fromArrayNoCopy("select count(*) from test1");
                    long expectedCount = s_insertCount.get();
                    responseJSON = callProcOverJSON("@AdHoc", pset, m_username, m_password, false, false);
                    int startIndex = responseJSON.indexOf(":[[");
                    int endIndex = responseJSON.indexOf("]]");
                    if (startIndex==-1 || endIndex==-1) {
                        System.out.println("Invalid response from select: " + responseJSON);
                        s_success = false;
                        return;
                    }
                    int count = Integer.parseInt(responseJSON.substring(startIndex+3, endIndex));
                    if (count < expectedCount) {
                        System.out.println("Select must have returned at least " + expectedCount + ". Got "+ count);
                        s_success = false;
                        return;
                    }
                    // do a proc cal that takes longer
                    pset = ParameterSet.fromArrayNoCopy(500);
                    responseJSON = callProcOverJSON("TestJSONInterface$WorkerProc", pset, m_username, m_password, false, false);
                    //System.out.println("WorkperProc response: " + responseJSON);
                }
            } catch(Exception e) {
                e.printStackTrace();
                s_success = false;
            }
        }
    }

    public static class WorkerProc extends VoltProcedure {

        public long run(long delay) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new VoltAbortException(e.getMessage());
            }
            return 0;
        }

    }
}
