/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package mytest;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

import mytest.procedures.*;

public class MyTest {
    public static void main(String[] args) throws Exception {
        Client client = null;
        ClientConfig config = null;
        try {
            //config = new ClientConfig("advent","xyzzy");
            client = ClientFactory.createClient();
            client.createConnection("localhost");
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        client.createConnection("localhost");


        VoltTable[] results;
        try {
            results = client.callProcedure("Initialize").getResults();
            System.out.println("after init");
            //for(int i=1; i<100;i++)
            results = client.callProcedure("InsertTwoDuplicate", 1).getResults();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
