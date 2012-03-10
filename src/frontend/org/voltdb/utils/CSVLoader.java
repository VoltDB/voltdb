/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.*;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import org.voltdb.client.*;
import au.com.bytecode.opencsv_voltpatches.CSVReader;

/**
 * CSVLoader is a simple utility to load CSV to a table that matches
 * the CSV column ordering.
 *
 * TODO:
 *   - Nulls are not handled (or at least I didn't test them).
 *   - Assumes localhost
 *   - Assumes no username/password
 *   - Does not provide usage help: needs args {filename, insertProc}
 *   - No associated test suite.
 *   - Forces JVM into UTC. All input date TZs assumed to be GMT+0
 *   - Requires canonical timestamp format
 *   - Doesn't understand header rows.
 */
class CSVLoader {

    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
    }

    private static final AtomicLong count = new AtomicLong(0);

    private static final class MyCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.err.println("Line " + count.get());
                System.err.println(response.getStatusString());
                System.exit(1);
            }

            long currentCount = count.incrementAndGet();
            if (currentCount % 5000 == 0) {
                System.out.println("Inserted " + currentCount + " rows");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.exit(1);
        }

        final String filename = args[0];
        final String insertProcedure = args[1];

        try {
            final CSVReader reader = new CSVReader(new FileReader(filename));
            final ProcedureCallback cb = new MyCallback();
            final Client client = ClientFactory.createClient();
            client.createConnection("localhost");

            String line[] = null;
            while ((line = reader.readNext()) != null) {
                boolean queued = false;
                while (queued == false) {
                    queued = client.callProcedure(cb, insertProcedure, (Object[])line);
                    if (!queued) {
                        Thread.sleep(10);
                    }
                }
            }

            reader.close();
            client.drain();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Inserted " + count.get() + " rows");
    }

}
