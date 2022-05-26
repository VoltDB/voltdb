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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URLEncoder;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.TestJSONInterface.Response;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSimpleCJK {
    public static final String POORLY_TRANSLATED_CHINESE =
        "两条路分叉在黄色的树林，\n" +
        "可惜我不能到处都\n" +
        "而且是一个旅客，我站在\n" +
        "向下看去，尽量令我\n" +
        "它的地方在丛林深处;\n";

    public static final String POORLY_TRANSLATED_JAPANESE =
        "2つの道路は黄色の木では、分岐\n" +
        "そして、残念私は両方の旅行でした" +
        "そして、私は立って1つの旅行者、長さ" +
        "そして、1つ下の限りに見えた私はできる限り" +
        "下草のどこが曲がっすること。";

    public static final String POORLY_TRANSLATED_KOREAN =
        "두 도로는 노란색 나무에서 갈라" +
        "그리고 미안하다 둘 다 여행할 수 없습니다" +
        "그리고 내가 서 한 여행자, 오래" +
        "아래로 하나까지 보였다 내가 할 수처럼" +
        "덤불에 어디로 휘어한다";

    /*public static final String POORLY_TRANSLATED_CHINESE = "两条";

    public static final String POORLY_TRANSLATED_JAPANESE = "2つ";

    public static final String POORLY_TRANSLATED_KOREAN = "두도";*/

    ServerThread m_server;

    @Before
    public void startup() throws Exception {
        String simpleSchema =
            "create table cjk (" +
            "sval1 varchar(1024) not null, " +
            "sval2 varchar(1024) default 'foo', " +
            "sval3 varchar(1024) default 'bar', " +
            "PRIMARY KEY(sval1));";

        /*String simpleSchema =
            "create table cjk (" +
            "sval1 varchar(20) not null, " +
            "sval2 varchar(20) default 'foo', " +
            "sval3 varchar(20) default 'bar', " +
            "PRIMARY KEY(sval1));";*/

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("cjk", "sval1");
        builder.addStmtProcedure("Insert", "insert into cjk values (?,?,?);");
        builder.addStmtProcedure("Select", "select * from cjk;");
        builder.setHTTPDPort(8095);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("cjk.jar"), 1, 1, 0);
        assertTrue(success);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("cjk.jar");
        config.m_pathToDeployment = builder.getPathToDeployment();
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        m_server =  server;
    }

    @Test
    public void testRoundTripCJKWithRegularInsert() throws Exception {
        ServerThread server = m_server;

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponse response1;
        response1 = client.callProcedure("Insert",
                POORLY_TRANSLATED_CHINESE, POORLY_TRANSLATED_JAPANESE, POORLY_TRANSLATED_KOREAN);
        assertEquals(response1.getStatus(), ClientResponse.SUCCESS);
        response1 = client.callProcedure("Select");
        assertEquals(response1.getStatus(), ClientResponse.SUCCESS);

        VoltTable[] results = response1.getResults();
        assertEquals(1, results.length);
        VoltTable result = results[0];
        assertEquals(1, result.getRowCount());
        assertEquals(3, result.getColumnCount());
        result.advanceRow();

        String c = result.getString(0);
        String j = result.getString(1);
        String k = result.getString(2);

        assertEquals(0, c.compareTo(POORLY_TRANSLATED_CHINESE));
        assertEquals(0, j.compareTo(POORLY_TRANSLATED_JAPANESE));
        assertEquals(0, k.compareTo(POORLY_TRANSLATED_KOREAN));

        ParameterSet pset = ParameterSet.emptyParameterSet();
        String responseJSON;
        Response response2;

        responseJSON = TestJSONInterface.callProcOverJSON("Select", pset, null, null, false);
        System.out.println(responseJSON);
        response2 = TestJSONInterface.responseFromJSON(responseJSON);
        assertTrue(response2.status == ClientResponse.SUCCESS);

        results = response2.results;
        assertEquals(1, results.length);
        result = results[0];
        assertEquals(1, result.getRowCount());
        assertEquals(3, result.getColumnCount());
        result.advanceRow();

        c = result.getString(0);
        j = result.getString(1);
        k = result.getString(2);

        System.out.printf("c: %s\nj: %s\nk: %s\n", c, j, k);

        assertEquals(0, c.compareTo(POORLY_TRANSLATED_CHINESE));
        assertEquals(0, j.compareTo(POORLY_TRANSLATED_JAPANESE));
        assertEquals(0, k.compareTo(POORLY_TRANSLATED_KOREAN));

        server.shutdown();
        server.join();
    }

    @Test
    public void testRoundTripCJKWithJSONInsert() throws Exception {
        ServerThread server = m_server;

        ParameterSet pset;
        String responseJSON;
        Response response;
        VoltTable[] results;
        VoltTable result;
        String c,j,k;

        // Call insert
        pset = ParameterSet.fromArrayNoCopy(POORLY_TRANSLATED_CHINESE, POORLY_TRANSLATED_JAPANESE, POORLY_TRANSLATED_KOREAN);
        responseJSON = TestJSONInterface.callProcOverJSON("Insert", pset, null, null, false);
        System.out.println(responseJSON);
        response = TestJSONInterface.responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        // Call select
        pset = ParameterSet.emptyParameterSet();
        responseJSON = TestJSONInterface.callProcOverJSON("Select", pset, null, null, false);
        System.out.println(responseJSON);
        response = TestJSONInterface.responseFromJSON(responseJSON);
        assertTrue(response.status == ClientResponse.SUCCESS);

        results = response.results;
        assertEquals(1, results.length);
        result = results[0];
        assertEquals(1, result.getRowCount());
        assertEquals(3, result.getColumnCount());
        result.advanceRow();

        c = result.getString(0);
        j = result.getString(1);
        k = result.getString(2);

        System.out.printf("c: %s\nj: %s\nk: %s\n", c, j, k);

        assertEquals(0, c.compareTo(POORLY_TRANSLATED_CHINESE));
        assertEquals(0, j.compareTo(POORLY_TRANSLATED_JAPANESE));
        assertEquals(0, k.compareTo(POORLY_TRANSLATED_KOREAN));

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponse response1;
        response1 = client.callProcedure("Select");
        assertEquals(response1.getStatus(), ClientResponse.SUCCESS);

        results = response1.getResults();
        assertEquals(1, results.length);
        result = results[0];
        assertEquals(1, result.getRowCount());
        assertEquals(3, result.getColumnCount());
        result.advanceRow();

        c = result.getString(0);
        j = result.getString(1);
        k = result.getString(2);

        System.out.printf("c: %s\nj: %s\nk: %s\n", c, j, k);

        assertEquals(0, c.compareTo(POORLY_TRANSLATED_CHINESE));
        assertEquals(0, j.compareTo(POORLY_TRANSLATED_JAPANESE));
        assertEquals(0, k.compareTo(POORLY_TRANSLATED_KOREAN));

        server.shutdown();
        server.join();
    }
}
