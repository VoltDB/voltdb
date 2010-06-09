/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.json.JSONArray;
import org.json.JSONObject;
import org.voltdb.compiler.CrazyBlahProc;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

public class TestJSONInterface extends TestCase {

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

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?,?,?,?,?);");
        builder.addProcedures(CrazyBlahProc.class);
        boolean success = builder.compile("json.jar");
        assertTrue(success);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_httpAdminPort = 8095;
        config.m_pathToCatalog = "json.jar";
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        ParameterSet pset = new ParameterSet();
        pset.setParameters(1,
                           new double[] { 1.5, 6.0, 4 },
                           new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT)),
                           new BigDecimal[] {},
                           new TimestampType(System.currentTimeMillis()));
        String paramsInJSON = pset.toJSONString();

        //debug
        JSONObject jobj = new JSONObject("{ params: " + paramsInJSON + " }");
        System.out.println(jobj.toString(2));

        HashMap<String,String> params = new HashMap<String,String>();
        params.put("Procedure", "CrazyBlahProc");
        params.put("Parameters", paramsInJSON);

        String varString = getHTTPVarString(params);

        URL jsonAPIURL = new URL("http://localhost:8095/api/");
        HttpURLConnection conn = (HttpURLConnection) jsonAPIURL.openConnection();
        conn.setDoOutput(true);

        conn.setRequestMethod("POST");
        OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
        out.write(varString);
        out.close();

        BufferedReader in = null;
        try {
            if(conn.getInputStream()!=null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getInputStream()));
            }
        } catch(IOException e){
            if(conn.getErrorStream()!=null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getErrorStream()));
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
        // get result code
        int responseCode = conn.getResponseCode();

        String response = decodedString.toString();

        assertEquals(200, responseCode);
        System.out.println(response);

        JSONObject jsonObj = new JSONObject(response);
        JSONArray results = jsonObj.getJSONArray("results");
        assertEquals(3, results.length());
        JSONObject table = results.getJSONObject(0);
        JSONArray data = table.getJSONArray("data");
        assertEquals(1, data.length());
        JSONArray row = data.getJSONArray(0);
        assertEquals(1, row.length());
        long value = row.getLong(0);
        assertEquals(1, value);

        System.out.println(responseCode);
        System.out.println(decodedString.toString());

        server.shutdown();
        server.join();
    }


    String getHTTPVarString(Map<String,String> params) throws UnsupportedEncodingException {
        String s = "";
        for (Entry<String, String> e : params.entrySet())
        s += "&"+ e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8");
        s = s.substring(1);
        return s;
    }
}
