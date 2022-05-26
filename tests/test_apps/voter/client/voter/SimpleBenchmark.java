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

package voter;

import java.io.IOException;

import java.util.Random;

import org.voltdb.client.*;
import org.voltdb.client.Client;

public class SimpleBenchmark
{
    private final static int TXNS = 10000;

    public static void main(String[] args)
    {
        System.out.println("Running Simple Benchmark");
        try {
            final Client client = ClientFactory.createClient();
            final Random rng = new Random();

            for (String s : args) {
                client.createConnection(s, Client.VOLTDB_SERVER_PORT);
            }

            for (int i=0; i < SimpleBenchmark.TXNS; i++) {
                ClientResponse response =
                    client.callProcedure("VOTES.insert", rng.nextLong(), "MA", Integer.valueOf(i));

                if (response.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException(response.getStatusString());
                }

                if (i % 1000 == 0) {
                    System.out.printf(".");
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (ProcCallException e) {
            throw new RuntimeException(e);
        }

        System.out.println(" completed " + SimpleBenchmark.TXNS + " transactions.");
    }
}
