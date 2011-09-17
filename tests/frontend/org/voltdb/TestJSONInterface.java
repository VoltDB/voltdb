/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.compiler.procedures.CrazyBlahProc;
import org.voltdb.compiler.procedures.DelayProc;
import org.voltdb.compiler.procedures.SelectStarHelloWorld;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class TestJSONInterface extends TestCase {

    static class Response {
        public byte status = 0;
        public String statusString = null;
        public byte appStatus = Byte.MIN_VALUE;
        public String appStatusString = null;
        public VoltTable[] results = new VoltTable[0];
        public String exception = null;
    }

    static String japaneseTestVarStrings = "Procedure=Insert&Parameters=%5B%22%5Cu3053%5Cu3093%5Cu306b%5Cu3061%5Cu306f%22%2C%22%5Cu4e16%5Cu754c%22%2C%22Japanese%22%5D";

    static String getHTTPVarString(Map<String,String> params) throws UnsupportedEncodingException {
        String s = "";
        for (Entry<String, String> e : params.entrySet()) {
            String encodedValue = URLEncoder.encode(e.getValue(), "UTF-8");
            s += "&"+ e.getKey() + "=" + encodedValue;
        }
        s = s.substring(1);
        return s;
    }

    public static String callProcOverJSONRaw(String varString, int expectedCode) throws Exception {
        URL jsonAPIURL = new URL("http://localhost:8095/api/1.0/");

        HttpURLConnection conn = (HttpURLConnection) jsonAPIURL.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.connect();

        OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
        out.write(varString);
        out.flush();
        out.close();
        out = null;
        conn.getOutputStream().close();

        BufferedReader in = null;
        try {
            if(conn.getInputStream()!=null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getInputStream(), "UTF-8"));
            }
        } catch(IOException e){
            if(conn.getErrorStream()!=null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getErrorStream(), "UTF-8"));
            }
        }
        if(in==null) {
            throw new Exception("Unable to read response from server");
        }

        StringBuffer decodedString = new StringBuffer();
        String line;
        while ((line = in.readLine()) != null) {
            decodedString.append(line);
        }
        in.close();
        in = null;
        // get result code
        int responseCode = conn.getResponseCode();

        String response = decodedString.toString();

        assertEquals(expectedCode, responseCode);

        try {
            conn.getInputStream().close();
            conn.disconnect();
        }
        // ignore closing problems here
        catch (Exception e) {}
        conn = null;

        //System.err.println(response);

        return response;
    }

    public static String getHashedPasswordForHTTPVar(String password) {
        assert(password != null);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
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
        assertEquals(40, retval.length());
        return retval;
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, false, 200 /* HTTP_OK */);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, admin, 200 /* HTTP_OK */);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin, int expectedCode) throws Exception {
        // Call insert
        String paramsInJSON = pset.toJSONString();
        //System.out.println(paramsInJSON);
        HashMap<String,String> params = new HashMap<String,String>();
        params.put("Procedure", procName);
        params.put("Parameters", paramsInJSON);
        if (username != null)
            params.put("User", username);
        if (password != null) {
            if (preHash)
                params.put("Hashedpassword", getHashedPasswordForHTTPVar(password));
            else
                params.put("Password", password);
        }
        if (admin) {
            params.put("admin", "true");
        }

        String varString = getHTTPVarString(params);

        varString = getHTTPVarString(params);

        return callProcOverJSONRaw(varString, expectedCode);
    }

    public static Response responseFromJSON(String jsonStr) throws JSONException, IOException {
        Response response = new Response();
        JSONObject jsonObj = new JSONObject(jsonStr);
        JSONArray resultsJson = jsonObj.getJSONArray("results");
        response.results = new VoltTable[resultsJson.length()];
        for (int i = 0; i < response.results.length; i++) {
            JSONObject tableJson = resultsJson.getJSONObject(0);
            response.results[i] =  VoltTable.fromJSONObject(tableJson);
        }
        if (jsonObj.isNull("status") == false)
            response.status = (byte) jsonObj.getInt("status");
        if (jsonObj.isNull("appstatus") == false)
            response.appStatus = (byte) jsonObj.getInt("appstatus");
        if (jsonObj.isNull("statusstring") == false)
            response.statusString = jsonObj.getString("statusstring");
        if (jsonObj.isNull("appstatusstring") == false)
            response.appStatusString = jsonObj.getString("appstatusstring");
        if (jsonObj.isNull("exception") == false)
            response.exception = jsonObj.getString("exception");

        return response;
    }

    public void testSimple() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 23 not null, " +
            "sval varchar(200) default 'foo', " +
            "dateval timestamp, " +
            "fval float, " +
            "decval decimal, " +
            "PRIMARY KEY(ival));";

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
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"), 1, 1, 0, 21213, false);
        assertTrue(success);

        config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
        config.m_pathToDeployment = builder.getPathToDeployment();


        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        ParameterSet pset = new ParameterSet();
        String responseJSON;
        Response response;

        // Call insert
        pset.setParameters(1, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
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
        pset.setParameters(1,
                           5,
                           new double[] { 1.5, 6.0, 4 },
                           new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                           new BigDecimal(5),
                           new BigDecimal[] {},
                           new TimestampType(System.currentTimeMillis()));

        responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // check the JSON itself makes sense
        JSONObject jsonObj = new JSONObject(responseJSON);
        JSONArray results = jsonObj.getJSONArray("results");
        assertEquals(3, response.results.length);
        JSONObject table = results.getJSONObject(0);
        JSONArray data = table.getJSONArray("data");
        assertEquals(1, data.length());
        JSONArray row = data.getJSONArray(0);
        assertEquals(1, row.length());
        long value = row.getLong(0);
        assertEquals(1, value);

        // try to pass a string as a date
        java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
        pset.setParameters(1,
                5,
                new double[] { 1.5, 6.0, 4 },
                new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                new BigDecimal(5),
                new BigDecimal[] {},
                ts.toString());

        responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // now try a null short value sent as a int  (param expects short)
        pset.setParameters(1,
                VoltType.NULL_SMALLINT,
                new double[] { 1.5, 6.0, 4 },
                new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                new BigDecimal(5),
                new BigDecimal[] {},
                new TimestampType(System.currentTimeMillis()));

        responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertFalse(response.status == ClientResponse.SUCCESS);

        // now try an out of range long value (param expects short)
        pset.setParameters(1,
                Long.MAX_VALUE - 100,
                new double[] { 1.5, 6.0, 4 },
                new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                new BigDecimal(5),
                new BigDecimal[] {},
                new TimestampType(System.currentTimeMillis()));

        responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertFalse(response.status == ClientResponse.SUCCESS);

        // now try bigdecimal with small value
        pset.setParameters(1,
                4,
                new double[] { 1.5, 6.0, 4 },
                new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                5,
                new BigDecimal[] {},
                new TimestampType(System.currentTimeMillis()));

        responseJSON = callProcOverJSON("CrazyBlahProc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        System.out.println(response.statusString);
        assertEquals(ClientResponse.SUCCESS, response.status);

        // now try null
        pset.setParameters(1,
                4,
                new double[] { 1.5, 6.0, 4 },
                new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                5,
                new BigDecimal[] {},
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
        pset.setParameters("select * from blah");
        responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        System.out.println(response.statusString);
        assertEquals(ClientResponse.SUCCESS, response.status);

        // now try adhoc insert with a huge bigint
        pset.setParameters("insert into blah values (974599638818488300, NULL, NULL, NULL, NULL);");
        responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        System.out.println(response.statusString);
        assertEquals(ClientResponse.SUCCESS, response.status);

        pset.setParameters("select * from blah where ival = 974599638818488300;");
        responseJSON = callProcOverJSON("@AdHoc", pset, null, null, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        System.out.println(response.statusString);
        assertEquals(ClientResponse.SUCCESS, response.status);
        assertEquals(1, response.results.length);
        assertEquals(1, response.results[0].getRowCount());

        server.shutdown();
        server.join();
    }

    public void testJapaneseNastiness() throws Exception {
        String simpleSchema =
            "CREATE TABLE HELLOWORLD (\n" +
            "    HELLO VARCHAR(15),\n" +
            "    WORLD VARCHAR(15),\n" +
            "    DIALECT VARCHAR(15) NOT NULL,\n" +
            "    PRIMARY KEY (DIALECT)\n" +
            ");";

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
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        String response = callProcOverJSONRaw(japaneseTestVarStrings, 200);
        Response r = responseFromJSON(response);
        assertEquals(1, r.status);

        // If this line doesn't compile, right click the file in the package explorer.
        // Select the properties menu. Set the text file encoding to UTF-8.
        char[] test1 = {'こ', 'ん', 'に', 'ち', 'は' };
        String test2 = new String(test1);

        ParameterSet pset = new ParameterSet();
        response = callProcOverJSON("Select", pset, null, null, false);
        System.out.println(response);
        System.out.println(test2);
        r = responseFromJSON(response);
        assertEquals(1, r.status);

        response = callProcOverJSON("SelectStarHelloWorld", pset, null, null, false);
        r = responseFromJSON(response);
        assertEquals(1, r.status);
        assertTrue(response.contains(test2));

        server.shutdown();
        server.join();
    }

    public void testJSONAuth() throws Exception {
        String simpleSchema =
            "CREATE TABLE HELLOWORLD (\n" +
            "    HELLO VARCHAR(15),\n" +
            "    WORLD VARCHAR(20),\n" +
            "    DIALECT VARCHAR(15) NOT NULL,\n" +
            "    PRIMARY KEY (DIALECT)\n" +
            ");";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("HELLOWORLD", "DIALECT");

        GroupInfo gi = new GroupInfo("foo", true, true);
        builder.addGroups(new GroupInfo[] { gi } );

        // create 20 users, only the first one has an interesting user/pass
        UserInfo[] ui = new UserInfo[15];
        ui[0] = new UserInfo("ry@nlikesthe", "y@nkees", new String[] { "foo" } );
        for (int i = 1; i < ui.length; i++) {
            ui[i] = new UserInfo("USER" + String.valueOf(i), "PASS" + String.valueOf(i), new String[] { "foo" } );
        }
        builder.addUsers(ui);

        builder.setSecurityEnabled(true);

        ProcedureInfo[] pi = new ProcedureInfo[2];
        pi[0] = new ProcedureInfo(new String[] { "foo" }, "Insert", "insert into HELLOWORLD values (?,?,?);", null);
        pi[1] = new ProcedureInfo(new String[] { "foo" }, "Select", "select * from HELLOWORLD;", null);
        builder.addProcedures(pi);

        builder.setHTTPDPort(8095);

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
        assertTrue(success);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
        config.m_pathToDeployment = builder.getPathToDeployment();
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        // test good auths
        for (UserInfo u : ui) {
            ParameterSet pset = new ParameterSet();
            pset.setParameters(u.name, u.password, u.name);
            String response = callProcOverJSON("Insert", pset, u.name, u.password, true);
            Response r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);
        }
        // test re-using auths
        for (UserInfo u : ui) {
            ParameterSet pset = new ParameterSet();
            pset.setParameters(u.name + "-X", u.password + "-X", u.name + "-X");
            String response = callProcOverJSON("Insert", pset, u.name, u.password, false);
            Response r = responseFromJSON(response);
            assertEquals(ClientResponse.SUCCESS, r.status);
        }

        // test bad auth
        UserInfo u = ui[0];
        ParameterSet pset = new ParameterSet();
        pset.setParameters(u.name + "-X1", u.password + "-X1", u.name + "-X1");
        String response = callProcOverJSON("Insert", pset, u.name, "ick", true);
        Response r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
        response = callProcOverJSON("Insert", pset, u.name, "ick", false);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);

        // test malformed auth (too short hash)
        pset = new ParameterSet();
        pset.setParameters(u.name + "-X2", u.password + "-X2", u.name + "-X2");
        String paramsInJSON = pset.toJSONString();
        HashMap<String,String> params = new HashMap<String,String>();
        params.put("Procedure", "Insert");
        params.put("Parameters", paramsInJSON);
        params.put("User", u.name);
        params.put("Password", Encoder.hexEncode(new byte[] {1,2,3}));
        String varString = getHTTPVarString(params);
        response = callProcOverJSONRaw(varString, 200);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);

        // test malformed auth (gibberish password, but good length)
        pset = new ParameterSet();
        pset.setParameters(u.name + "-X3", u.password + "-X3", u.name + "-X3");
        paramsInJSON = pset.toJSONString();
        params = new HashMap<String,String>();
        params.put("Procedure", "Insert");
        params.put("Parameters", paramsInJSON);
        params.put("User", u.name);
        params.put("Password", "abcdefghiabcdefghiabcdefghiabcdefghi");
        varString = getHTTPVarString(params);
        response = callProcOverJSONRaw(varString, 200);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);

        // test back-to-back rejections, unknown user
        // partial ENG-954 test (make sure the rejection timeout works)
        pset = new ParameterSet();
        pset.setParameters(u.name + "-X4", u.password + "-X4", u.name + "-X4");
        response = callProcOverJSON("Insert", pset, "rando", "ick", true);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
        assertTrue(r.statusString.contains("Authentication rejected"));
        System.out.println(r.statusString);
        response = callProcOverJSON("Insert", pset, "rando", "ick", true);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
        assertTrue(r.statusString.contains("rejected due to too many recent rejected attempts"));
        System.out.println(r.statusString);
        // Rejection timeout is 1 second, sleep for slightly more and make sure we get the first error
        Thread.sleep(1100);
        response = callProcOverJSON("Insert", pset, "rando", "ick", true);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
        assertTrue(r.statusString.contains("Authentication rejected"));
        System.out.println(r.statusString);

        // ENG-1115 test (make sure the rejection timeout works when the password is null
        // but the username is not)
        // Wait and clear the authentication failure timeout from above
        Thread.sleep(1100);
        pset = new ParameterSet();
        pset.setParameters(u.name + "-X4", u.password + "-X4", u.name + "-X4");
        response = callProcOverJSON("Insert", pset, "rando2", null, true);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
        assertTrue(r.statusString.contains("Authentication rejected"));
        System.out.println(r.statusString);
        response = callProcOverJSON("Insert", pset, "rando2", null, true);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.status);
        assertTrue(r.statusString.contains("rejected due to too many recent rejected attempts"));
        System.out.println(r.statusString);


        // ENG-963 below here
        // do enough to get a new deployment file
        VoltProjectBuilder builder2 = new VoltProjectBuilder();
        builder2.addSchema(schemaPath);
        builder2.addPartitionInfo("HELLOWORLD", "DIALECT");

        // Same groups
        builder2.addGroups(new GroupInfo[] { gi } );

        // create same 15 users, hack the last 14 passwords
        ui = new UserInfo[15];
        ui[0] = new UserInfo("ry@nlikesthe", "y@nkees", new String[] { "foo" } );
        for (int i = 1; i < ui.length; i++) {
            ui[i] = new UserInfo("USER" + String.valueOf(i),
                                 "welcomehackers" + String.valueOf(i),
                                 new String[] { "foo" } );
        }
        builder2.addUsers(ui);

        builder2.setSecurityEnabled(true);
        builder2.addProcedures(pi);
        builder2.setHTTPDPort(8095);

        success = builder2.compile(Configuration.getPathToCatalogForTest("json-update.jar"));
        assertTrue(success);

        pset = new ParameterSet();
        pset.setParameters(Encoder.hexEncode(CatalogUtil.toBytes(new File(config.m_pathToCatalog))),
                           new String(CatalogUtil.toBytes(new File(builder2.getPathToDeployment())), "UTF-8"));
        response = callProcOverJSON("@UpdateApplicationCatalog", pset,
                                    ui[0].name, ui[0].password, true);
        r = responseFromJSON(response);
        assertEquals(ClientResponse.SUCCESS, r.status);

        // retest the good auths above
        for (UserInfo user : ui) {
            ParameterSet ps = new ParameterSet();
            ps.setParameters(user.name + "-X3", user.password + "-X3", user.name + "-X3");
            String respstr = callProcOverJSON("Insert", ps, user.name, user.password, false);
            Response resp = responseFromJSON(respstr);
            assertEquals(ClientResponse.SUCCESS, resp.status);
        }

        server.shutdown();
        server.join();
    }

    public void testJSONDisabled() throws Exception {
        String simpleSchema =
            "CREATE TABLE HELLOWORLD (\n" +
            "    HELLO VARCHAR(15),\n" +
            "    WORLD VARCHAR(15),\n" +
            "    DIALECT VARCHAR(15) NOT NULL,\n" +
            "    PRIMARY KEY (DIALECT)\n" +
            ");";

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
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        // test not enabled
        ParameterSet pset = new ParameterSet();
        pset.setParameters("foo", "bar", "foobar");
        try {
            callProcOverJSON("Insert", pset, null, null, false, false, 403); // HTTP_FORBIDDEN
        }
        catch (Exception e) {
            // make sure failed due to permissions on http
            assertTrue(e.getMessage().contains("403"));
        }

        server.shutdown();
        server.join();
    }

    public void testLongProc() throws Exception {
        String simpleSchema =
            "CREATE TABLE foo (\n" +
            "    bar BIGINT NOT NULL,\n" +
            "    PRIMARY KEY (bar)\n" +
            ");";

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
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        ParameterSet pset = new ParameterSet();
        pset.setParameters(30000);
        String response = callProcOverJSON("DelayProc", pset, null, null, false);
        Response r = responseFromJSON(response);
        assertEquals(ClientResponse.SUCCESS, r.status);

        server.shutdown();
        server.join();
    }

    public void testBinaryProc() throws Exception {
        String simpleSchema =
            "CREATE TABLE foo (\n" +
            "    bar BIGINT NOT NULL,\n" +
            "    b VARBINARY(256) DEFAULT NULL,\n" +
            "    PRIMARY KEY (bar)\n" +
            ");";

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
        ServerThread server = new ServerThread(config);
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

        server.shutdown();
        server.join();
    }

    public void testGarbageProcs() throws Exception {
        String simpleSchema =
            "CREATE TABLE foo (\n" +
            "    bar BIGINT NOT NULL,\n" +
            "    PRIMARY KEY (bar)\n" +
            ");";

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
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        callProcOverJSONRaw("http://localhost:8080/api/1.0/Tim", 404);
        callProcOverJSONRaw("http://localhost:8080/api/1.0/Tim?Procedure=foo&Parameters=[x4{]", 404);

        server.shutdown();
        server.join();
    }

    public void testAdminMode() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 23 not null, " +
            "sval varchar(200) default 'foo', " +
            "dateval timestamp, " +
            "fval float, " +
            "decval decimal, " +
            "PRIMARY KEY(ival));";

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
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"), 1, 1, 0, 21213, true);
        assertTrue(success);

        config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
        config.m_pathToDeployment = builder.getPathToDeployment();

        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        ParameterSet pset = new ParameterSet();
        String responseJSON;
        Response response;

        // Call insert on admin port
        pset.setParameters(1, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
        responseJSON = callProcOverJSON("Insert", pset, null, null, false, true);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // Call insert on closed client port and expect failure
        pset.setParameters(2, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
        responseJSON = callProcOverJSON("Insert", pset, null, null, false, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SERVER_UNAVAILABLE);

        // open client port
        pset.setParameters();
        responseJSON = callProcOverJSON("@Resume", pset, null, null, false, true);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // call insert on open client port
        pset.setParameters(2, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
        responseJSON = callProcOverJSON("Insert", pset, null, null, false, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // call insert on admin port again (now that both ports are open)
        pset.setParameters(3, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
        responseJSON = callProcOverJSON("Insert", pset, null, null, false, true);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // put the system in admin mode
        pset.setParameters();
        responseJSON = callProcOverJSON("@Pause", pset, null, null, false, true);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // Call insert on admin port
        pset.setParameters(4, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
        responseJSON = callProcOverJSON("Insert", pset, null, null, false, true);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // Call insert on closed client port and expect failure
        pset.setParameters(5, "hello", new TimestampType(System.currentTimeMillis()), 5.0, "5.0");
        responseJSON = callProcOverJSON("Insert", pset, null, null, false, false);
        System.out.println(responseJSON);
        response = responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SERVER_UNAVAILABLE);

        server.shutdown();
        server.join();
    }
}
