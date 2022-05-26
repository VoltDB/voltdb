/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class OrderClient {

    final static String procName = "OrderProc";
    final Client2 client;

    OrderClient(Client2 client) throws IOException {
        this.client = client;
    }

    // Very simple 'order' code. We execute a single (compound)
    // procedure with the specified arguments, and print the
    // result.
    long order(String customerName, String partName, int quantity) throws IOException {
        long orderId = 0;
        try {
            ClientResponse resp = client.callProcedureSync(procName, customerName, partName, quantity);
            if (resp.getStatus() == ClientResponse.SUCCESS) {
                VoltTable vt = resp.getResults()[0];
                vt.advanceRow();
                orderId = vt.getLong("ORDERID");
            }
        }
        catch (ProcCallException ex) {
            System.err.println("Procedure call failed: " + ex);
        }
        return orderId;
    }

    // Main routine creates an instance of the order client
    // and then issues one order.
    public static void main(String... arg) throws Exception {
        if (arg.length == 0) { // convenience for demo
            arg = new String[] { "Mr. Customer", "Left-Handed Widget", "3" };
        }
        if (arg.length != 3) {
            System.err.println("OrderClient needs 3 arguments:\n" +
                               "  customer name, part name, quantity\n");
            System.exit(1);
        }

        // For this simple example, we use all default
        // configuration settings.
        try (Client2 client = ClientFactory.createClient(new Client2Config())) {
            client.connectSync("localhost");

            OrderClient oc = new OrderClient(client);
            long order = oc.order(arg[0], arg[1], Integer.parseInt(arg[2]));
            if (order != 0) {
                System.out.printf("Order placed; number %d%n", order);
            }
            else {
                System.out.println("Order could not be placed");
            }
        }

        catch (IOException ex) {
            System.err.println("I/O error: " + ex);
            System.exit(2);
        }
    }
}
