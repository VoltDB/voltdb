/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package com.${package};

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

public class Client
{

    static final String[] tables = {
${table_list}
    };

    public static void main(String[] args)
    {
        org.voltdb.client.Client client = null;
        ClientConfig config = new ClientConfig("", "");
        client = ClientFactory.createClient(config);
        int sleep = 1000;
        while(true) {
            try
            {
                client.createConnection("localhost");
                break;
            }
            catch (Exception e) {
                System.out.println("Connection failed - retrying in " + (sleep/1000) + " second(s).");
                try {
                    Thread.sleep(sleep);
                }
                catch(Exception tie)
                {}
                if (sleep < 8000) {
                    sleep += sleep;
                }
            }
        }

        System.out.println("===== Table Counts =====\n");
        for (String table : tables) {
            try {
                String query = String.format("SELECT COUNT(*) FROM %s", table);
                VoltTable results = client.callProcedure("@AdHoc", query).getResults()[0];
                while( results.advanceRow() ){
                    Integer count = (Integer) results.get(0, VoltType.INTEGER);
                    System.out.printf("%s: %d\n", table, count);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            client.close();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
