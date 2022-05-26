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

package log4jsocketimporter;

import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;



/**
 */
public class LogAnalyzer
{
    private final Client m_voltClient;

    public LogAnalyzer(String voltHostPort)
    {
            m_voltClient = ClientFactory.createClient();
            connectToOneServerWithRetry(voltHostPort);
    }

    public void analyzeOperation(String name)
    {
        int seconds = 10;
        long minuteBeforeNow = System.currentTimeMillis() - seconds*1000;
        ClientResponse response = null;
        try {
            response = m_voltClient.callProcedure("FetchLogRowsProcedure", minuteBeforeNow, name);
        } catch(ProcCallException | IOException e) {
            System.out.println("Error executing analyzer stmt: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.out.println("Procedure execution failed with status " + response.getStatus());
            return;
        }

        VoltTable[] results = response.getResults();
        if (results.length==0 || results[0].getRowCount()==0) {
            System.out.println("No entries found for " + name + " in the last " + seconds + " seconds");
            return;
        }

        int count = 0;
        long totalTime = 0;
        int min = 0;
        int max = 0;
        System.out.println("rowCount=" + results[0].getRowCount());
        for (int i=0; i<results[0].getRowCount(); i++) {
            VoltTableRow row = results[0].fetchRow(i);
            int time = getTimeFromLogMesg((String) row.get(0, VoltType.STRING));
            if (time>=0) {
                min = Math.min(min, time);
                max = Math.max(max, time);
                totalTime += time;
                count++;
            }
        }

        if (count==0) {
            System.out.println("No good log entries found for " + name + " in the last " + seconds + " seconds");
        } else {
            System.out.println(String.format("Operation time for %s in the last %d seconds: min=%d, max=%d, avg=%f",
                    name, seconds, min, max, totalTime*1.0/count));
        }
    }

    private int getTimeFromLogMesg(String mesg)
    {
        try {
            return Integer.parseInt(mesg);
        } catch(NumberFormatException e) {
            System.err.println("Ignoring log in wrong format " + mesg);
            return -1;
        }
    }

    // Copied from voter example
    private void connectToOneServerWithRetry(String server)
    {
        int sleep = 1000;
        while (true) {
            try {
                m_voltClient.createConnection(server);
                break;
            } catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000)
                    sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }
}
