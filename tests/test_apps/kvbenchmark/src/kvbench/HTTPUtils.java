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
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */
package kvbench;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.utils.Encoder;

public class HTTPUtils {

    static class Response {

        public byte status = 0;
        public String statusString = null;
        public byte appStatus = Byte.MIN_VALUE;
        public String appStatusString = null;
        public VoltTable[] results = new VoltTable[0];
        public String exception = null;
    }
    static String username = null;
    static String password = null;
    static boolean prehash = true;

    static void dumpResponse(Response resp) {
        System.out.println("resp.toString(): " + resp.toString());
        System.out.println("status: " + resp.status);
    }

    static String getHTTPVarString(Map<String, String> params) throws UnsupportedEncodingException {
        String s = "";
        for (Entry<String, String> e : params.entrySet()) {
            String encodedValue = URLEncoder.encode(e.getValue(), "UTF-8");
            s += "&" + e.getKey() + "=" + encodedValue;
        }
        s = s.substring(1);
        return s;
    }

    public static String callProcOverJSONRaw(String varString, int expectedCode) throws Exception {
        URL jsonAPIURL = new URL("http://localhost:8080/api/1.0/");

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

    public static String getHashedPasswordForHTTPVar(String password) {
        assert (password != null);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte hashedPassword[] = null;
        try {
            hashedPassword = md.digest(password.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("JVM doesn't support UTF-8. Please use a supported JVM", e);
        }

        String retval = Encoder.hexEncode(hashedPassword);
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
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("Procedure", procName);
        params.put("Parameters", paramsInJSON);
        if (username != null) {
            params.put("User", username);
        }
        if (password != null) {
            if (preHash) {
                params.put("Hashedpassword", getHashedPasswordForHTTPVar(password));
            } else {
                params.put("Password", password);
            }
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
            JSONObject tableJson = resultsJson.getJSONObject(i);
            response.results[i] = VoltTable.fromJSONObject(tableJson);
            System.out.println(response.results[i].toString());
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

    public static Response callProcedure(String string, String key, byte[] storeValue) throws JSONException, IOException, Exception {
        ParameterSet pset = ParameterSet.fromArrayNoCopy(key, storeValue);
        System.out.println("Call proc: " + string + ". key: " + key + ". len(storeValue): " + storeValue.length);
        String resp = callProcOverJSON(string, pset, username, password, prehash);
        System.out.println("Response KV resp: " + resp.toString());
        Response response = responseFromJSON(resp);
        System.out.println("Response KV: " + response.toString());
        return response;
    }

    public static Response callProcedure(String string, String generateRandomKeyForRetrieval) throws JSONException, IOException, Exception {
        ParameterSet pset = ParameterSet.fromArrayNoCopy(generateRandomKeyForRetrieval);
        System.out.println("Call proc: " + string + ". key: " + generateRandomKeyForRetrieval);
        String resp = callProcOverJSON(string, pset, username, password, prehash);
        System.out.println("Response K resp: " + resp.toString());
        Response response = responseFromJSON(resp);
        System.out.println("Response K: " + response.toString());
        return response;
    }

}
