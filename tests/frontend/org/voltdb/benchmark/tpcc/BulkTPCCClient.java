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

package org.voltdb.benchmark.tpcc;

import java.io.IOException;

import org.voltdb.types.TimestampType;
import org.voltdb.client.ClientResponse;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.benchmark.Clock;
import org.voltdb.client.BulkClient;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class BulkTPCCClient extends BulkClient {
    private final ScaleParameters m_scaleParams;

    private final double skewfactor;

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass =
        TPCCProjectBuilder.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends ClientMain> m_loaderClass =
        org.voltdb.benchmark.tpcc.MultiLoader.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final String m_jarFileName = "tpcc.jar";

    /** Complies with our benchmark client remote controller scheme */
    public static void main(String args[]) {
        org.voltdb.client.BulkClient.main(BulkTPCCClient.class, args);
    }

    /** Complies with our benchmark client remote controller scheme */
    public BulkTPCCClient(String args[]) {
        super(args);
        /*
         * Input parameters:
         *   warehouses=#
         *   scalefactor=#
         *   skewfactor=#
         */

        // default values
        int warehouses = 1;
        double scalefactor = 1.0;
        double skewfactorTemp = 0.0;

        // scan the inputs once to read everything but host names
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            }
            else if (parts[1].startsWith("${")) {
                continue;
            }
            else if (parts[0].equals("warehouses")) {
                warehouses = Integer.parseInt(parts[1]);
            }
            else if (parts[0].equals("scalefactor")) {
                scalefactor = Double.parseDouble(parts[1]);
            }
            else if (parts[0].equals("skewfactor")) {
                skewfactorTemp = Double.parseDouble(parts[1]);
            }
        }
        skewfactor = skewfactorTemp;
        m_scaleParams =
            ScaleParameters.makeWithScaleFactor(warehouses, scalefactor);
    }

    // Delivery
    class DeliveryCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            assert clientResponse.getStatus() == 1;
//            if (clientResponse.getResults()[0].getRowCount()
//                    != m_scaleParams.districtsPerWarehouse) {
//                System.err.println(
//                        "Only delivered from "
//                        + clientResponse.getResults()[0].getRowCount()
//                        + " districts. Expected " + m_scaleParams.districtsPerWarehouse);
//            }
            m_counts[TPCCSimulation.Transaction.DELIVERY.ordinal()].incrementAndGet();
        }
    }

    // NewOrder

    class NewOrderCallback implements ProcedureCallback {

        public NewOrderCallback(boolean rollback) {
            super();
            this.cbRollback = rollback;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            assert this.cbRollback || (clientResponse.getResults() != null);
            m_counts[TPCCSimulation.Transaction.NEW_ORDER.ordinal()].incrementAndGet();
        }

        private boolean cbRollback;
    }

    // Order status

    class VerifyBasicCallback implements ProcedureCallback {
        private final TPCCSimulation.Transaction m_transactionType;

        /**
         * A generic callback that does not credit a transaction. Some transactions
         * use two procedure calls - this counts as one transaction not two.
         */
        VerifyBasicCallback() {
            m_transactionType = null;
        }

        /** A generic callback that credits for the transaction type passed. */
        VerifyBasicCallback(TPCCSimulation.Transaction transaction) {
            m_transactionType = transaction;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            assert clientResponse.getResults() != null : "Results were null";
            assert clientResponse.getStatus() == 1 : " Result status code was not 1";
            if (m_transactionType != null) {
                m_counts[m_transactionType.ordinal()].incrementAndGet();
            }
        }
    }

    class ResetWarehouseCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[TPCCSimulation.Transaction.RESET_WAREHOUSE.ordinal()].incrementAndGet();
        }
    }

    // StockLevel
    class StockLevelCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            assert clientResponse.getStatus() == 1;
            assert clientResponse.getResults().length == 1;
            assert clientResponse.getResults()[0].asScalarLong() >= 0;
            m_counts[TPCCSimulation.Transaction.STOCK_LEVEL.ordinal()].incrementAndGet();
        }
    }

    private class TPCCInputHandler extends BulkClient.VoltProtocolHandler implements TPCCSimulation.ProcCaller {
        final TPCCSimulation m_tpccSim;
        private BulkClient.Connection c;
        private TPCCInputHandler() {
            super();
            // makeForRun requires the value cLast from the load generator in
            // order to produce a valid generator for the run. Thus the sort
            // of weird eat-your-own ctor pattern.
            RandomGenerator.NURandC base_loadC = new RandomGenerator.NURandC(0,0,0);
            RandomGenerator.NURandC base_runC = RandomGenerator.NURandC.makeForRun(
                    new RandomGenerator.Implementation(0), base_loadC);
            RandomGenerator rng = new RandomGenerator.Implementation(0);
            rng.setC(base_runC);
            m_tpccSim =
                new TPCCSimulation(this, rng, new Clock.RealTime(), m_scaleParams, false, skewfactor);
        }
        @Override
        protected void generateInvocation(Connection c) throws IOException {
            this.c = c;
            m_tpccSim.doOne();
        }

        @Override
        public void callDelivery(short w_id, int carrier, TimestampType date)
                throws IOException {
            invokeProcedure( c, new DeliveryCallback(),
                    Constants.DELIVERY, w_id, carrier, date);
        }

        @Override
        public void callNewOrder(boolean rollback, Object... paramlist)
                throws IOException {
             invokeProcedure( c, new NewOrderCallback(rollback),
                    Constants.NEWORDER, paramlist);
        }
        @Override
        public void callOrderStatus(String proc, Object... paramlist)
                throws IOException {
            invokeProcedure( c, new VerifyBasicCallback(TPCCSimulation.Transaction.ORDER_STATUS),
                    proc, paramlist);
        }
        @Override
        public void callPaymentById(short w_id, byte d_id, double h_amount,
                short c_w_id, byte c_d_id, int c_id, TimestampType now) throws IOException {
            if (m_scaleParams.warehouses > 1) {
                invokeProcedure( c, new VerifyBasicCallback(),
                        Constants.PAYMENT_BY_ID_W,
                        w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now);
                invokeProcedure( c, new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT),
                        Constants.PAYMENT_BY_ID_C,
                        w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now);
            }
            else {
                invokeProcedure( c, new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT),
                        Constants.PAYMENT_BY_ID,
                        w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now);
            }
        }
        @Override
        public void callPaymentByName(short w_id, byte d_id, double h_amount,
                short c_w_id, byte c_d_id, byte[] c_last, TimestampType now) throws IOException {
            if ((m_scaleParams.warehouses > 1) || (c_last != null)) {
                invokeProcedure( c, new VerifyBasicCallback(),
                        Constants.PAYMENT_BY_NAME_W, w_id, d_id, h_amount,
                        c_w_id, c_d_id, c_last, now);
                invokeProcedure( c, new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT),
                        Constants.PAYMENT_BY_NAME_C, w_id, d_id, h_amount,
                        c_w_id, c_d_id, c_last, now);
            }
            else {
                invokeProcedure( c, new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT),
                        Constants.PAYMENT_BY_ID, w_id,
                        d_id, h_amount, c_w_id, c_d_id, c_last, now);
            }
        }
        @Override
        public void callResetWarehouse(long w_id, long districtsPerWarehouse,
                long customersPerDistrict, long newOrdersPerDistrict)
        throws IOException {
           invokeProcedure( c, new ResetWarehouseCallback(),
                  Constants.RESET_WAREHOUSE, w_id, districtsPerWarehouse,
                  customersPerDistrict, newOrdersPerDistrict);
        }
        @Override
        public void callStockLevel(short w_id, byte d_id, int threshold) throws IOException {
            invokeProcedure( c, new StockLevelCallback(), Constants.STOCK_LEVEL,
                     w_id, d_id, threshold);
        }

    }

    @Override
    protected VoltProtocolHandler getNewInputHandler() {
        return new TPCCInputHandler();
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        String countDisplayNames[] = new String[TPCCSimulation.Transaction.values().length];
        for (int ii = 0; ii < TPCCSimulation.Transaction.values().length; ii++) {
            countDisplayNames[ii] = TPCCSimulation.Transaction.values()[ii].displayName;
        }
        return countDisplayNames;
    }
}
