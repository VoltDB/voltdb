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

package voltkvqa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
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
    static Random rand = new Random();
    static String[] servers;

    static void dumpResponse(Response resp) {
        System.out.println("resp.toString(): " + resp.toString());
        System.out.println("status: " + resp.status);
    }

    public static String callProcOverJSONRaw(List<NameValuePair> vals, CloseableHttpClient httpclient,
            HttpPost httppost) throws Exception {

        HttpEntity entity = null;
        String entityStr = null;

        httppost.setEntity(new UrlEncodedFormEntity(vals));
        CloseableHttpResponse httpResponse = httpclient.execute(httppost);

        BufferedReader reader = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()));

        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = reader.readLine()) != null) {
            response.append(inputLine);
        }

        return response.toString();
    }

    public static String getHashedPasswordForHTTPVar(String password) {
        assert(password != null);

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

    public static String callProcOverJSON(String procName, ParameterSet pset,
            String username, String password, boolean preHash, CloseableHttpClient httpclient,
            HttpPost httppost) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, false, httpclient,  httppost);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset,
            String username, String password, boolean preHash, boolean admin,
            CloseableHttpClient httpclient, HttpPost httppost) throws Exception {
        // Call insert
        String paramsInJSON = pset.toJSONString();

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("Procedure", procName));
        params.add(new BasicNameValuePair("Parameters", paramsInJSON));
        if (username != null) {
            params.add(new BasicNameValuePair("User", username));
        }
        if (password != null) {
            if (preHash) {
                params.add(new BasicNameValuePair("Hashedpassword", getHashedPasswordForHTTPVar(password)));
            } else {
                params.add(new BasicNameValuePair("Password", password));
            }
        }
        if (admin) {
            params.add(new BasicNameValuePair("admin", "true"));
        }

        return callProcOverJSONRaw(params, httpclient, httppost);
    }

    public static Response responseFromJSON(String jsonStr) throws JSONException, IOException {
        Response response = new Response();
        JSONObject jsonObj = new JSONObject(jsonStr);
        JSONArray resultsJson = jsonObj.getJSONArray("results");
        response.results = new VoltTable[resultsJson.length()];
        for (int i = 0; i < response.results.length; i++) {
            JSONObject tableJson = resultsJson.getJSONObject(i);
            response.results[i] =  VoltTable.fromJSONObject(tableJson);
            //System.out.println(response.results[i].toString());
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

    public static Response callProcedure(String string, String key, byte[] storeValue,
            CloseableHttpClient httpclient, HttpPost httppost)
                    throws JSONException, IOException, Exception {
        String hexval = Encoder.hexEncode(storeValue);
        ParameterSet pset = ParameterSet.fromArrayNoCopy(key, hexval);
        String resp = callProcOverJSON(string, pset, username, password, prehash, httpclient, httppost);
        Response response = responseFromJSON(resp);
        return response;
    }

    public static Response  callProcedure(String string, String generateRandomKeyForRetrieval,
            CloseableHttpClient httpclient, HttpPost httppost)
                    throws JSONException, IOException, Exception {
        ParameterSet pset = ParameterSet.fromArrayNoCopy(generateRandomKeyForRetrieval);
        String resp = callProcOverJSON(string, pset, username, password, prehash, httpclient, httppost);
        Response response = responseFromJSON(resp);
        return response;
    }

}
