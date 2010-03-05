/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.types.TimestampType;
import org.voltdb.client.ClientResponse;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.benchmark.Clock;
import org.voltdb.benchmark.Verification;
import org.voltdb.benchmark.Verification.Expression;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.types.ExpressionType;

import java.io.IOException;

public class TPCCClient extends org.voltdb.benchmark.ClientMain
implements TPCCSimulation.ProcCaller {
    final TPCCSimulation m_tpccSim;
    final TPCCSimulation m_tpccSim2;
    private final ScaleParameters m_scaleParams;

    /** Complies with our benchmark client remote controller scheme */
    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(TPCCClient.class, args, false);
    }

    public TPCCClient(
            Client client,
            RandomGenerator generator,
            Clock clock,
            ScaleParameters params)
    {
        this(client, generator, clock, params, 0.0d);
    }

    public TPCCClient(
            Client client,
            RandomGenerator generator,
            Clock clock,
            ScaleParameters params,
            double skewFactor)
    {
        super(client);
        m_scaleParams = params;
        m_tpccSim = new TPCCSimulation(this, generator, clock, m_scaleParams, false, skewFactor);
        m_tpccSim2 = new TPCCSimulation(this, generator, clock, m_scaleParams, false, skewFactor);
    }

    /** Complies with our benchmark client remote controller scheme */
    public TPCCClient(String args[]) {
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
        double skewfactor = 0.0;

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
                skewfactor = Double.parseDouble(parts[1]);
            }
        }

        // makeForRun requires the value cLast from the load generator in
        // order to produce a valid generator for the run. Thus the sort
        // of weird eat-your-own ctor pattern.
        RandomGenerator.NURandC base_loadC = new RandomGenerator.NURandC(0,0,0);
        RandomGenerator.NURandC base_runC = RandomGenerator.NURandC.makeForRun(
                new RandomGenerator.Implementation(0), base_loadC);
        RandomGenerator rng = new RandomGenerator.Implementation(0);
        rng.setC(base_runC);

        RandomGenerator.NURandC base_loadC2 = new RandomGenerator.NURandC(0,0,0);
        RandomGenerator.NURandC base_runC2 = RandomGenerator.NURandC.makeForRun(
                new RandomGenerator.Implementation(0), base_loadC2);
        RandomGenerator rng2 = new RandomGenerator.Implementation(0);
        rng.setC(base_runC2);

        m_scaleParams =
            ScaleParameters.makeWithScaleFactor(warehouses, scalefactor);
        m_tpccSim =
            new TPCCSimulation(this, rng, new Clock.RealTime(), m_scaleParams, false, skewfactor);
        m_tpccSim2 =
            new TPCCSimulation(this, rng2, new Clock.RealTime(), m_scaleParams, false, skewfactor);

        // Set up checking
        buildConstraints();

        //m_sampler = new VoltSampler(20, "tpcc-cliet-sampling");
    }

    @Override
    protected boolean useHeavyweightClient() {
        return true;
    }

    protected void buildConstraints() {
        Expression constraint = null;
        Expression constraint1 = null;
        Expression constraint2 = null;
        Expression constraint3 = null;
        Expression constraint4 = null;
        Expression constraint5 = null;

        // Delivery (no need to check 'd_id', it's systematically generated)
        constraint = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                      "o_id",
                                                      0);
        addConstraint(Constants.DELIVERY, 0, constraint);

        // New Order table 0
        constraint1 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "C_DISCOUNT",
                                                       0.0);
        constraint2 = Verification.compareWithConstant(ExpressionType.COMPARE_LESSTHANOREQUALTO,
                                                       "C_DISCOUNT",
                                                       1.0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              constraint1,
                                              constraint2);
        addConstraint(Constants.NEWORDER, 0, constraint);
        // New Order table 1
        constraint1 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "w_tax",
                                                       0.0);
        constraint2 = Verification.compareWithConstant(ExpressionType.COMPARE_LESSTHANOREQUALTO,
                                                       "w_tax",
                                                       1.0);
        constraint3 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "d_tax",
                                                       0.0);
        constraint4 = Verification.compareWithConstant(ExpressionType.COMPARE_LESSTHANOREQUALTO,
                                                       "d_tax",
                                                       1.0);
        constraint5 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHAN,
                                                       "total",
                                                       0.0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              constraint1,
                                              constraint2,
                                              constraint3,
                                              constraint4,
                                              constraint5);
        addConstraint(Constants.NEWORDER, 1, constraint);
        // New Order table 2
        constraint1 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "s_quantity",
                                                       0);
        constraint2 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "i_price",
                                                       0.0);
        constraint3 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHAN,
                                                       "ol_amount",
                                                       0.0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              constraint1,
                                              constraint2,
                                              constraint3);
        addConstraint(Constants.NEWORDER, 2, constraint);

        // Order Status table 0
        constraint = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                      "C_ID",
                                                      0);
        addConstraint(Constants.ORDER_STATUS_BY_ID, 0, constraint);
        addConstraint(Constants.ORDER_STATUS_BY_NAME, 0, constraint);
        // Order Status table 1
        constraint1 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "O_ID",
                                                       0);
        constraint2 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "O_CARRIER_ID",
                                                       0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              constraint1,
                                              constraint2);
        addConstraint(Constants.ORDER_STATUS_BY_ID, 1, constraint);
        addConstraint(Constants.ORDER_STATUS_BY_NAME, 1, constraint);
        // Order Status table 2
        constraint1 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHAN,
                                                       "OL_SUPPLY_W_ID",
                                                       (short) 0);
        constraint2 = Verification.compareWithConstant(ExpressionType.COMPARE_LESSTHANOREQUALTO,
                                                       "OL_SUPPLY_W_ID",
                                                       (short) m_scaleParams.warehouses);
        constraint3 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "OL_I_ID",
                                                       0);
        constraint4 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "OL_QUANTITY",
                                                       0);
        constraint5 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "OL_AMOUNT",
                                                       0.0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              constraint1,
                                              constraint2,
                                              constraint3,
                                              constraint4,
                                              constraint5);
        addConstraint(Constants.ORDER_STATUS_BY_ID, 2, constraint);
        addConstraint(Constants.ORDER_STATUS_BY_NAME, 1, constraint);

        // Payment
        constraint1 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "c_id",
                                                       0);
        constraint2 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "c_credit_lim",
                                                       0.0);
        constraint3 = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                       "c_discount",
                                                       0.0);
        constraint4 = Verification.compareWithConstant(ExpressionType.COMPARE_LESSTHANOREQUALTO,
                                                       "c_discount",
                                                       1.0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              constraint1,
                                              constraint2,
                                              constraint3,
                                              constraint4);
        addConstraint(Constants.PAYMENT_BY_ID, 2, constraint);
        addConstraint(Constants.PAYMENT_BY_ID_C, 0, constraint);
        addConstraint(Constants.PAYMENT_BY_NAME, 2, constraint);
        addConstraint(Constants.PAYMENT_BY_NAME_C, 0, constraint);

        // slev
        constraint = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                      "C1",
                                                      0L);
        addConstraint(Constants.STOCK_LEVEL, 0, constraint);
    }

    /**
     * Whether a message was queued when attempting the last invocation.
     */
    private boolean m_queuedMessage = false;

    /*
     * callXXX methods should spin on backpressure barrier until they successfully queue rather then
     * setting m_queuedMessage and returning immediately.
     */
    private boolean m_blockOnBackpressure = true;

    @Override
    protected boolean runOnce() throws NoConnectionsException {
        m_blockOnBackpressure = false;
        // will send procedures to first connection w/o backpressure
        // if all connections have backpressure, will round robin across
        // busy servers (but the loop will spend more time running the
        // network below.)
        try {
            m_tpccSim.doOne();
            return m_queuedMessage;
        } catch (IOException e) {
            throw (NoConnectionsException)e;
        }
    }

    /**
     * Hint used when constructing the Client to control the size of buffers allocated for message
     * serialization
     * Set to 512 because neworder tops out around that size
     * @return
     */
    @Override
    protected int getExpectedOutgoingMessageSize() {
        return 256;
    }

    @Override
    protected void runLoop() throws NoConnectionsException {
        m_blockOnBackpressure = true;
        if (Runtime.getRuntime().availableProcessors() > 4) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            m_tpccSim2.doOne();
                        }
                    } catch (IOException e) {
                    }
                }
            }.start();
        }
        try {
            while (true) {
                // will send procedures to first connection w/o backpressure
                // if all connections have backpressure, will round robin across
                // busy servers (but the loop will spend more time running the
                // network below.)
                m_tpccSim.doOne();
            }
        } catch (IOException e) {
            throw (NoConnectionsException)e;
        }
    }

    // Delivery
    class DeliveryCallback extends ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            boolean status = checkTransaction(Constants.DELIVERY, clientResponse, false);
            assert status;
            if (status && clientResponse.getResults()[0].getRowCount()
                    != m_scaleParams.districtsPerWarehouse) {
                System.err.println(
                        "Only delivered from "
                        + clientResponse.getResults()[0].getRowCount()
                        + " districts.");
            }
            m_counts[TPCCSimulation.Transaction.DELIVERY.ordinal()].incrementAndGet();
        }
    }

    @Override
    public void callDelivery(short w_id, int carrier, TimestampType date) throws IOException {
        if (m_blockOnBackpressure) {
            final DeliveryCallback cb = new DeliveryCallback();
            while (!m_voltClient.callProcedure(cb,
                Constants.DELIVERY, w_id, carrier, date)) {
                try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } else {
            m_queuedMessage = m_voltClient.callProcedure(new DeliveryCallback(),
                    Constants.DELIVERY, w_id, carrier, date);
        }
    }


    // NewOrder

    class NewOrderCallback extends ProcedureCallback {

        public NewOrderCallback(boolean rollback) {
            super();
            this.cbRollback = rollback;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            boolean status = checkTransaction(Constants.NEWORDER, clientResponse, cbRollback);
            assert this.cbRollback || status;
            m_counts[TPCCSimulation.Transaction.NEW_ORDER.ordinal()].incrementAndGet();
        }

        private boolean cbRollback;
    }

    int randomIndex = 0;
    @Override
    public void callNewOrder(boolean rollback, Object... paramlist) throws IOException {
        if (m_blockOnBackpressure) {
            final NewOrderCallback cb = new NewOrderCallback(rollback);
            while (!m_voltClient.callProcedure( cb,
                Constants.NEWORDER, paramlist)) {
                try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } else {
            m_queuedMessage = m_voltClient.callProcedure(new NewOrderCallback(rollback),
                    Constants.NEWORDER, paramlist);
        }
    }

    // Order status

    class VerifyBasicCallback extends ProcedureCallback {
        private final TPCCSimulation.Transaction m_transactionType;
        private final String m_procedureName;

        /**
         * A generic callback that does not credit a transaction. Some transactions
         * use two procedure calls - this counts as one transaction not two.
         */
        VerifyBasicCallback() {
            m_transactionType = null;
            m_procedureName = null;
        }

        /** A generic callback that credits for the transaction type passed. */
        VerifyBasicCallback(TPCCSimulation.Transaction transaction, String procName) {
            m_transactionType = transaction;
            m_procedureName = procName;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            boolean errorExpected = false;
            if (m_procedureName != null && (m_procedureName.equals(Constants.ORDER_STATUS_BY_NAME)
                || m_procedureName.equals(Constants.ORDER_STATUS_BY_ID)))
                errorExpected = true;
            boolean status = checkTransaction(m_procedureName,
                                              clientResponse,
                                              errorExpected);
            assert status;
            if (m_transactionType != null) {
                m_counts[m_transactionType.ordinal()].incrementAndGet();
            }
        }
    }

    @Override
    public void callOrderStatus(String proc, Object... paramlist) throws IOException {
        if (m_blockOnBackpressure) {
            final VerifyBasicCallback cb = new VerifyBasicCallback(TPCCSimulation.Transaction.ORDER_STATUS,
                    proc);
            while (!m_voltClient.callProcedure( cb,
                                                                             proc, paramlist)) {
                try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } else {
            m_queuedMessage = m_voltClient.callProcedure(new VerifyBasicCallback(TPCCSimulation.Transaction.ORDER_STATUS,
                                                                             proc),
                proc, paramlist);
        }
    }


    // Payment


    @Override
    public void callPaymentById(short w_id, byte d_id, double h_amount,
            short c_w_id, byte c_d_id, int c_id, TimestampType now) throws IOException {
       if (m_blockOnBackpressure) {
           if (m_scaleParams.warehouses > 1) {
               final VerifyBasicCallback cb = new VerifyBasicCallback();
               while(!m_voltClient.callProcedure( cb,
                      Constants.PAYMENT_BY_ID_W,
                      w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now)) {
                   try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
               }
               final VerifyBasicCallback cb2 = new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                       Constants.PAYMENT_BY_ID_C);
               while (!m_voltClient.callProcedure( cb2,
                      Constants.PAYMENT_BY_ID_C,
                      w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now)) {
                   try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
               }
          }
          else {
              final VerifyBasicCallback cb = new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                      Constants.PAYMENT_BY_ID);
               while (!m_voltClient.callProcedure( cb,
                      Constants.PAYMENT_BY_ID,
                      w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now)) {
                   try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
               }
          }
       } else {
           if (m_scaleParams.warehouses > 1) {
                m_voltClient.callProcedure(new VerifyBasicCallback(),
                       Constants.PAYMENT_BY_ID_W,
                       w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now);
                m_queuedMessage = m_voltClient.callProcedure(new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                                                                                     Constants.PAYMENT_BY_ID_C),
                       Constants.PAYMENT_BY_ID_C,
                       w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now);
           }
           else {
                m_queuedMessage = m_voltClient.callProcedure(new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                                                                                     Constants.PAYMENT_BY_ID),
                       Constants.PAYMENT_BY_ID,
                       w_id, d_id, h_amount, c_w_id, c_d_id, c_id, now);
           }
       }
    }

    @Override
    public void callPaymentByName(short w_id, byte d_id, double h_amount,
            short c_w_id, byte c_d_id, byte[] c_last, TimestampType now) throws IOException {
        if (m_blockOnBackpressure) {
            if ((m_scaleParams.warehouses > 1) || (c_last != null)) {
                final VerifyBasicCallback cb = new VerifyBasicCallback();
                while(!m_voltClient.callProcedure(cb,
                        Constants.PAYMENT_BY_NAME_W, w_id, d_id, h_amount,
                        c_w_id, c_d_id, c_last, now)) {
                    try {
                        m_voltClient.backpressureBarrier();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                final VerifyBasicCallback cb2 = new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                        Constants.PAYMENT_BY_NAME_C);
                while(!m_voltClient.callProcedure( cb2,
                        Constants.PAYMENT_BY_NAME_C, w_id, d_id, h_amount,
                        c_w_id, c_d_id, c_last, now)) {
                    try {
                        m_voltClient.backpressureBarrier();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            else {
                final VerifyBasicCallback cb = new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                        Constants.PAYMENT_BY_ID);
                while(!m_voltClient.callProcedure( cb,
                        Constants.PAYMENT_BY_ID, w_id,
                        d_id, h_amount, c_w_id, c_d_id, c_last, now)) {
                    try {
                        m_voltClient.backpressureBarrier();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        } else {
            if ((m_scaleParams.warehouses > 1) || (c_last != null)) {
                m_voltClient.callProcedure(new VerifyBasicCallback(),
                        Constants.PAYMENT_BY_NAME_W, w_id, d_id, h_amount,
                        c_w_id, c_d_id, c_last, now);
                m_queuedMessage = m_voltClient.callProcedure(new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                                                                                     Constants.PAYMENT_BY_NAME_C),
                        Constants.PAYMENT_BY_NAME_C, w_id, d_id, h_amount,
                        c_w_id, c_d_id, c_last, now);
            }
            else {
                m_queuedMessage = m_voltClient.callProcedure(new VerifyBasicCallback(TPCCSimulation.Transaction.PAYMENT,
                                                                                     Constants.PAYMENT_BY_ID),
                        Constants.PAYMENT_BY_ID, w_id,
                        d_id, h_amount, c_w_id, c_d_id, c_last, now);
            }
        }
    }


    // StockLevel
    class StockLevelCallback extends ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            boolean status = checkTransaction(Constants.STOCK_LEVEL,
                                              clientResponse,
                                              false);
            assert status;
            m_counts[TPCCSimulation.Transaction.STOCK_LEVEL.ordinal()].incrementAndGet();
        }
      }

    @Override
    public void callStockLevel(short w_id, byte d_id, int threshold) throws IOException {
        final StockLevelCallback cb = new StockLevelCallback();
        while (!m_voltClient.callProcedure( cb, Constants.STOCK_LEVEL,
                w_id, d_id, threshold)) {
            try {
                m_voltClient.backpressureBarrier();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    class DumpStatisticsCallback extends ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (checkTransaction(null, clientResponse, false))
                System.err.println(clientResponse.getResults()[0]);
        }
    }

    public void dumpStatistics() throws IOException {
        m_queuedMessage = m_voltClient.callProcedure(new DumpStatisticsCallback(),
                "@Statistics", "procedure");
    }

    class ResetWarehouseCallback extends ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (checkTransaction(null, clientResponse, false))
                m_counts[TPCCSimulation.Transaction.RESET_WAREHOUSE.ordinal()].incrementAndGet();
        }
    }

    @Override
    public void callResetWarehouse(long w_id, long districtsPerWarehouse,
            long customersPerDistrict, long newOrdersPerDistrict)
    throws IOException {
        m_queuedMessage = m_voltClient.callProcedure(new ResetWarehouseCallback(),
              Constants.RESET_WAREHOUSE, w_id, districtsPerWarehouse,
              customersPerDistrict, newOrdersPerDistrict);
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        String countDisplayNames[] = new String[TPCCSimulation.Transaction.values().length];
        for (int ii = 0; ii < TPCCSimulation.Transaction.values().length; ii++) {
            countDisplayNames[ii] = TPCCSimulation.Transaction.values()[ii].displayName;
        }
        return countDisplayNames;
    }

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
}
