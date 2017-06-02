package org.voltdb;

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


import java.io.File;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.TestCase;

public class TestSimplestCharsetAdHocQuery extends TestCase {
    public static class TestProc extends VoltProcedure {

        public long run() {
            return 0;
        }
    }

    final String SCHEMA1 =
            "create table blah (" +
            	"clm_integer integer not null," +
            	"clm_tinyint tinyint default 0," +
            	"clm_smallint smallint default 0," +
            	"clm_bigint bigint default 0," +
            	"clm_string varchar(20) default null," +
            	"clm_decimal decimal default null," +
            	"clm_float float default null," +
            	"clm_timestamp timestamp default null," +
            	"clm_point geography_point default null," +
            	"clm_geography geography default null," +
            	"PRIMARY KEY(clm_integer)" +
            	"partition table blah on column clm_integer;\n" ;

//            	"create table blah2 ("+
//            	"pkey integer not null, " +
//            	"strval varchar(200), " +
//            	"PRIMARY KEY(pkey));\n" +
//            	"partition table blah2 on column pkey;\n";

    public void testSimple() throws Exception {

        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addProcedures(TestProc.class);
        pb.addLiteralSchema(SCHEMA1);
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("simpleUC.jar")));

        InProcessVoltDBServer server = new InProcessVoltDBServer();
        server.configPartitionCount(1);
        server.start();
        server.runDDLFromString(SCHEMA1);

        Client client = server.getClient();

        //client.callProcedure("@AdHoc", "create table replicated_blah (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));");
        client.callProcedure("@AdHoc", "select * from blah;");

        client.updateClasses(new File(Configuration.getPathToCatalogForTest("simpleUC.jar")), null);

        server.runDDLFromString("create procedure from class org.voltdb.TestSimplestCharsetAdHocQuery$TestProc;");

        ClientResponse cr = client.callProcedure("@Explain", "select * from blah;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable t = cr.getResults()[0];
        System.out.println(t.toFormattedString());

        cr = client.callProcedure("@ExplainProc", "BLAH.insert");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        t = cr.getResults()[0];
        System.out.println(t.toFormattedString());

        cr = client.callProcedure("@ExplainProc", "TestSimplestAdHocQuery$TestProc");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        t = cr.getResults()[0];
        System.out.println(t.toFormattedString());

        server.runDDLFromString("create view foo as select pkey, count(*) from blah group by pkey;");

        cr = client.callProcedure("@ExplainView", "foo");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        t = cr.getResults()[0];
        System.out.println(t.toFormattedString());

        cr = client.callProcedure("@SwapTables", "blah", "blah2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //t = cr.getResults()[0];
        //System.out.println(t.toFormattedString());



        server.shutdown();
    }
}
