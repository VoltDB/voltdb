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

package org.voltdb.benchmark.overhead;

import java.io.IOException;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.BulkClient;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.benchmark.overhead.OverheadClient.Transaction;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.client.ProcedureCallback;

public class BulkOverheadClient extends BulkClient {

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = OverheadProjectBuilder.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends ClientMain> m_loaderClass = null;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final String m_jarFileName = "measureoverhead.jar";

    private static Transaction transactionToInvoke = Transaction.MEASURE_OVERHEAD;

    public static void main(String[] args) {
        BulkClient.main(BulkOverheadClient.class, args);
    }

    public BulkOverheadClient(String args[]) {
        super(args);
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            } else if (parts[0].equals("transaction")) {
                Transaction transaction = Transaction.parse(parts[1]);
                if (transaction != null) {
                    System.err.println("Switching to using " + transaction + " as overhead transaction");
                    transactionToInvoke = transaction;
                }
            }
        }
    }

    private class MeasureOverheadCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[transactionToInvoke.ordinal()].incrementAndGet();
        }

    }

    private class OverheadInputHandler extends BulkClient.VoltProtocolHandler {
        private long invocations = (long)(Math.random() * 10000);

        @Override
        public void generateInvocation(Connection connection)
        throws IOException {
            if ((transactionToInvoke == Transaction.MEASURE_OVERHEAD) ||
                    (transactionToInvoke == Transaction.MEASURE_OVERHEAD_MULTIPARTITION)) {
                invokeProcedure( connection,
                        new MeasureOverheadCallback(),
                        transactionToInvoke.callName,
                        new Object[] { new Long(invocations++) });
            } else {
                Object argObjects[] = new Object[42];
                argObjects[0] = invocations++;
                switch (transactionToInvoke.ordinal()) {
                case 3:
                    for (int ii = 1; ii < argObjects.length; ii++) {
                        argObjects[ii] = new Long(ii);
                    }
                    break;
                case 5:
                    for (int ii = 1; ii < argObjects.length; ii++) {
                        argObjects[ii] = new String("1234567");
                    }
                    break;
                case 1:
                    for (int ii = 1; ii < argObjects.length; ii++) {
                        argObjects[ii] = new Byte((byte)1);
                    }
                    break;
                default:
                    assert(false);
                }
                invokeProcedure( connection,
                        new MeasureOverheadCallback(),
                        transactionToInvoke.callName, argObjects);
            }
        }

    }

    @Override
    protected VoltProtocolHandler getNewInputHandler() {
        return new OverheadInputHandler();
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction transaction : Transaction.values()) {
            names[ii++] = transaction.displayName;
        }
        return names;
    }

}
