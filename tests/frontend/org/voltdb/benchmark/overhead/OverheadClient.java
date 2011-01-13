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

import org.voltdb.benchmark.*;

import java.util.logging.Logger;
import org.voltdb.client.*;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.client.ClientResponse;

/** TPC-C client load generator. */
public class OverheadClient extends ClientMain {
    @SuppressWarnings("unused")
    private static final Logger LOG = Logger.getLogger(OverheadClient.class.getName());

    private static Transaction transactionToInvoke = Transaction.MEASURE_OVERHEAD;

    public static enum Transaction {
        MEASURE_OVERHEAD                            ("Measure Overhead",                "measureOverhead"),
        MEASURE_OVERHEAD_42_LONGS                   ("MOverhead 42 longs",              "measureOverhead42Longs"),
        MEASURE_OVERHEAD_42_STRINGS                 ("MOverhead 42 strings",            "measureOverhead42Strings"),
        MEASURE_OVERHEAD_MULTIPARTITION             ("MOverhead Multipart",             "measureOverheadMultipartition"),
        MEASURE_OVERHEAD_MULTIPARTITION_BATCHED     ("MOverhead Multipart Batched",     "measureOverheadMultipartitionBatched"),
        MEASURE_OVERHEAD_MULTIPARTITION_NOFINAL     ("MOverhead Multipart No Final",    "measureOverheadMultipartitionNoFinal"),
        MEASURE_OVERHEAD_MULTIPARTITION_TWOSTMTS    ("MOverhead Multipart Two Stmts",   "measureOverheadMultipartitionTwoStatements"),
        MEASURE_OVERHEAD_MULTIPARTITION_42_STRINGS  ("MOverhead Multipart 42 strings",  "measureOverheadMultipartition42Strings");

        private Transaction(String displayName, String callName) { this.displayName = displayName; this.callName = callName; }
        public final String displayName;
        public final String callName;

        public static Transaction parse(String value) {
            for (Transaction t : values()) {
                if (value.equalsIgnoreCase(t.name()) ||
                        value.equalsIgnoreCase(t.displayName) ||
                        value.equalsIgnoreCase(t.callName)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("No measure overhead transaction found.");
        }
    }

    public OverheadClient(String args[]) {
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

        for (Transaction transaction : Transaction.values()) {
            addConstraint(transaction.callName, 0, new MeasureOverheadConstraint());
        }
    }

    public static void main(String[] args) {
        ClientMain.main(OverheadClient.class, args, false);
    }

    /**
     * This constraint checks if the result set is empty. If not, this
     * constraint will be evaluated.
     */
    class MeasureOverheadConstraint implements Verification.Expression {
        @Override
        public <T> Object evaluate(T tuple) {
            return false;
        }

        @Override
        public <T> String toString(T tuple) {
            return "";
        }

    }

    class MeasureOverheadCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (checkTransaction(transactionToInvoke.callName, clientResponse, false, false))
                m_counts[transactionToInvoke.ordinal()].incrementAndGet();
        }

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

    int invocations = 0;

    @Override
    protected boolean runOnce() throws IOException {
     // always execute the same boring transaction.
        try {
            return invokeOverhead();
        } catch (NoConnectionsException e) {
            throw e;
        }
    }

    @Override
    protected void runLoop() throws IOException {
        try {
            while (true) {
                // always execute the same boring transaction.
                m_voltClient.backpressureBarrier();
                invokeOverhead();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean invokeOverhead() throws IOException {
        if ((transactionToInvoke == Transaction.MEASURE_OVERHEAD) ||
                (transactionToInvoke == Transaction.MEASURE_OVERHEAD_MULTIPARTITION) ||
                (transactionToInvoke == Transaction.MEASURE_OVERHEAD_MULTIPARTITION_BATCHED) ||
                (transactionToInvoke == Transaction.MEASURE_OVERHEAD_MULTIPARTITION_NOFINAL) ||
                (transactionToInvoke == Transaction.MEASURE_OVERHEAD_MULTIPARTITION_TWOSTMTS)) {

                return m_voltClient.callProcedure( new MeasureOverheadCallback(),
                        transactionToInvoke.callName, new Object[] { new Integer(invocations++) });
            } else {
                Object argObjects[] = new Object[42];
                argObjects[0] = invocations++;
                if (transactionToInvoke == Transaction.MEASURE_OVERHEAD_42_LONGS)
                    for (int ii = 1; ii < argObjects.length; ii++) {
                        argObjects[ii] = new Long(ii);
                    }
                if ((transactionToInvoke == Transaction.MEASURE_OVERHEAD_42_STRINGS) ||
                   (transactionToInvoke == Transaction.MEASURE_OVERHEAD_MULTIPARTITION_42_STRINGS))
                    for (int ii = 1; ii < argObjects.length; ii++) {
                        argObjects[ii] = new String("1234567890");
                    }
                else
                    assert(false);

                return m_voltClient.callProcedure( new MeasureOverheadCallback(),
                        transactionToInvoke.callName, argObjects);
        }
    }

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = OverheadProjectBuilder.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends ClientMain> m_loaderClass = null;

    @Override
    protected String getApplicationName() {
        return "Measure Overhead";
    }

    @Override
    protected String getSubApplicationName() {
        return "Client";
    }
}
