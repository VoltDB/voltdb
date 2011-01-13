/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.benchmark.workloads;

import java.io.IOException;

import java.net.ConnectException;
import java.net.UnknownHostException;

import org.voltdb.benchmark.workloads.paramgen.RandomValues;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.Client;

import java.math.BigDecimal;
import org.voltdb.types.TimestampType;

public class RandomLoader// implements Loader
{
    private static class GenericCallback implements ProcedureCallback
    {
        public void clientCallback(ClientResponse clientResponse)
        {
            //do error checking
            //param type matching
            //invalid inserts (primary key)
            //invalid deletes/selects (no such rows)
        }
    }

    public static void run(Client client)
    {
        int numInserts = 100000;
        try
        {
            int randInt;
            byte randByte;
            short randShort;
            long randLong;
            double randDouble;
            BigDecimal randBigDecimal;
            TimestampType randTimestamp;
            String randString;
            for (int i = 0; i < numInserts; i++)
            {
                randInt = RandomValues.getInt();
                randByte = RandomValues.getByte();
                randShort = RandomValues.getShort();
                randLong = RandomValues.getLong();
                randDouble = RandomValues.getDouble();
                randBigDecimal = RandomValues.getBigDecimal();
                randTimestamp = RandomValues.getTimestamp();
                randString = RandomValues.getString(5);

                client.callProcedure(   new GenericCallback(),
                                        "Insert",
                                        randInt,
                                        randByte,
                                        randShort,
                                        randLong,
                                        randDouble,
                                        randBigDecimal,
                                        randTimestamp,
                                        randString);
            }
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (ConnectException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}