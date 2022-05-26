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

package benchmark;

import org.voltdb.*;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.*;

import benchmark.Benchmark.InsertDeleteConfig;

public class SeedTables {

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class SeedConfig extends CLIConfig {
        @Option(desc = "Comma-separated list of the form server[:port] to connect to database for queries.")
        String servers = "localhost";

        @Override
        public void validate() {
            if (servers.length() == 0) servers = "localhost";
        }
    }

    public static void seedTables(String serverList) throws Exception {

        /*
         * Instantiate a client and connect to the database.
         */
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        String firstServer = serverList.split(",")[0];
        myApp.createConnection(firstServer);


        VoltTable p = myApp.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];
        int i = 0;
        while (p.advanceRow()) {
            i++;
            long id = p.getLong(0);
            myApp.callProcedure("TMP_0.insert",0,id,0);
            myApp.callProcedure("TMP_1.insert",0,id,0);
            myApp.callProcedure("TMP_2.insert",0,id,0);
            myApp.callProcedure("TMP_3.insert",0,id,0);
            myApp.callProcedure("TMP_4.insert",0,id,0);
            myApp.callProcedure("TMP_5.insert",0,id,0);
            myApp.callProcedure("TMP_6.insert",0,id,0);
            myApp.callProcedure("TMP_7.insert",0,id,0);
            myApp.callProcedure("TMP_8.insert",0,id,0);
            myApp.callProcedure("TMP_9.insert",0,id,0);

            myApp.callProcedure("TMP_s0.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s1.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s2.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s3.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s4.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s5.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s6.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s7.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s8.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s9.insert",0,id,0,"FOO");

        }
        System.out.println("Finished seeding " + i + " partitions.");
    }

    public static void main(String[] args) throws Exception {
        SeedConfig config = new SeedConfig();
        config.parse(SeedTables.class.getName(), args);
        seedTables(config.servers);
    }
}
