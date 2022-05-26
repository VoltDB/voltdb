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

import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.VoltCompoundProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

/**
 * This is an intentionally-simplistic example of a compound
 * procedure, to demonstrate use of the interface. All business
 * logic apart from what we need to show the structure has been
 * eliminated, which of course means that this is not a model
 * for an actual business application.
 *
 * A client application is assumed to be ordering some quantity
 * of a named part on behalf of a named quantity. We look up
 * the customer name and part name in tables in order to
 * determine some internal identifiers for these things, and
 * then update an "orders" table accordingly.
 */
public class OrderProc extends VoltCompoundProcedure {

    private static final AtomicInteger nextId = new AtomicInteger();

    private String customerName, partName;
    private int quantity;
    private long orderId;

    public long run(String customerName, String partName, int quantity) {
        this.customerName = customerName;
        this.partName = partName;
        this.quantity = quantity;

        newStageList(this::getData)
            .then(this::processData)
            .then(this::confirmOrder)
            .build();

        return 0;
    }

    // Execution stage 1 - look up customer and part names
    private void getData(ClientResponse[] nil) {
        queueProcedureCall("CUSTOMERS.select", customerName);
        queueProcedureCall("PARTS.select", partName);
    }

    // Execution stage 2 - process returned data, and then
    // insert an order into the ORDERS table.
    private void processData(ClientResponse[] resp) {
        VoltTable custRec = getResult(resp[0]);
        if (custRec == null) {
            abortProcedure("No such customer as " + customerName);
            return;
        }
        long custId = custRec.getLong("ID");

        VoltTable partRec = getResult(resp[1]);
        if (partRec == null) {
            abortProcedure("Cannot find part name " + partName);
            return;
        }
        long partNum = partRec.getLong("PARTNUM");
        int available = (int)partRec.getLong("AVAILABLE");

        if (quantity > available) { // not a reservation!
            abortProcedure("Only " + available + " parts available");
            return;
        }

        orderId = System.currentTimeMillis(); // unrealistic order id generator
        queueProcedureCall("ORDERS.insert", orderId, custId, partNum, quantity);
    }

    // Execution stage 3 - check result of order insert, and
    // return response to the client with order id.
    private void confirmOrder(ClientResponse[] resp) {
        VoltTable ordered = getResult(resp[0]);
        long count = (ordered == null ? 0 : ordered.getLong("modified_tuples"));
        if (count != 1) {
            abortProcedure("Failed to place order: " + resp[0].getStatusString());
            return;
        }

        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("ORDERID", VoltType.BIGINT));
        result.addRow(orderId);
        completeProcedure(result);
    }

    // Utility routine to return first row from response
    private static VoltTable getResult(ClientResponse resp) {
        VoltTable vt = null;
        if (resp.getStatus() == ClientResponse.SUCCESS) {
            vt = resp.getResults()[0];
            if (!vt.advanceRow()) {
                vt = null;
            }
        }
        return vt;
    }
}
