/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;

import java.util.concurrent.Semaphore;

/** TPC-C database loader. Note: The methods order id parameters from "top level" to "low level"
parameters. However, the insert stored procedures use the defined TPC-C table order, which goes from
"low level" to "top level" (except in the case of order line, which is inconsistent). As as example,
this class uses (o_w_id, o_d_id, o_id), whereas the order table is defined as (o_id, o_d_id,
o_w_id). */
public class MyLoader
{
    private AppHelper m_helpah;
    private final ClientConnection m_voltClient;
    /**
     * Number of threads to create to do the loading.
     */
    private final LoadThread m_loadThreads[];
    private final int m_warehouses;

    private static final VoltTable.ColumnInfo CUSTOMER_TABLE_COLUMN_INFO[] =
        new VoltTable.ColumnInfo[] {
            new VoltTable.ColumnInfo("C_ID", VoltType.INTEGER),
            new VoltTable.ColumnInfo("C_D_ID", VoltType.TINYINT),
            new VoltTable.ColumnInfo("C_W_ID", VoltType.SMALLINT),
            new VoltTable.ColumnInfo("C_FIRST", VoltType.STRING),
            new VoltTable.ColumnInfo("C_LAST", VoltType.STRING)
        };

    private static final LinkedList<VoltTable> s_customerNamesTables = new LinkedList<VoltTable>();
    private static final Semaphore m_finishedLoadThreads = new Semaphore(0);

    public MyLoader(String args[], ClientConnection voltClient)
    {
        m_helpah = new AppHelper(MyTPCC.class.getCanonicalName());
        m_helpah.add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 180);
        m_helpah.add("warehouses", "number_of_warehouses", "Number of warehouses", 256);
        m_helpah.add("scalefactor", "scale_factor", "Reduces per-warehouse data by warehouses/scalefactor", 22.0);
        m_helpah.add("skew-factor", "skew_factor", "Skew factor", 0.0);
        m_helpah.add("load-threads", "number_of_load_threads", "Number of load threads", 4);
        m_helpah.add("ratelimit", "rate_limit", "Rate limit to start from (tps)", 200000);
        m_helpah.add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10);
        m_helpah.add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost");
        m_helpah.setArguments(args);

        initTableNames();
        int warehouses = m_helpah.intValue("warehouses");
        double scaleFactor = m_helpah.doubleValue("scalefactor");
        int loadThreads = m_helpah.intValue("load-threads");

        if (loadThreads > warehouses)
        {
            System.out.println("Specified number of load threads exceeds number of warehouses. Setting former equal to latter.");
            loadThreads = warehouses;
        }

        m_warehouses = warehouses;
        m_loadThreads = new LoadThread[loadThreads];

        for (int ii = 0; ii < loadThreads; ii++) {
            ScaleParameters parameters = ScaleParameters.makeWithScaleFactor(warehouses, scaleFactor);
            assert parameters != null;

            RandomGenerator generator = new RandomGenerator.Implementation(0);
            TimestampType generationDateTime = new TimestampType();
            RandomGenerator.NURandC loadC = RandomGenerator.NURandC.makeForLoad(generator);
            generator.setC(loadC);

            m_loadThreads[ii] = new LoadThread(
                    generator,
                    generationDateTime,
                    parameters,
                    ii);
        }

        m_voltClient = voltClient;
    }

    private String[] m_tableNames = new String[8];
    private final static int IDX_WAREHOUSES = 0;
    private final static int IDX_DISTRICTS = 1;
    private final static int IDX_CUSTOMERS = 2;
    private final static int IDX_STOCKS = 3;
    private final static int IDX_ORDERS = 4;
    private final static int IDX_NEWORDERS = 5;
    private final static int IDX_ORDERLINES = 6;
    private final static int IDX_HISTORIES = 7;

    private void initTableNames() {
        m_tableNames[IDX_WAREHOUSES] = "warehouse";
        m_tableNames[IDX_DISTRICTS] = "district";
        m_tableNames[IDX_CUSTOMERS] = "customer";
        m_tableNames[IDX_STOCKS] = "stock";
        m_tableNames[IDX_ORDERS] = "orders";
        m_tableNames[IDX_NEWORDERS] = "new_order";
        m_tableNames[IDX_ORDERLINES] = "order_line";
        m_tableNames[IDX_HISTORIES] = "history";
    }

    /**
     * Hint used when constructing the Client to control the size of buffers allocated for message
     * serialization
     * @return
     */
    protected int getExpectedOutgoingMessageSize() {
        return 10485760;
    }

    private void rethrowExceptionLoad(String procedureName, Object... parameters) {
        try {
            VoltTable ret[] = m_voltClient.execute(procedureName, parameters).getResults();
            assert ret.length == 0;
        } catch (ProcCallException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    class LoadThread extends Thread {

        private final RandomGenerator m_generator;
        private final TimestampType m_generationDateTime;
        private final ScaleParameters m_parameters;

        /** table data FOR CURRENT WAREHOUSE (LoadWarehouse is partitioned on WID).*/
        private final VoltTable[] m_dataTables = new VoltTable[8]; // non replicated tables
        private volatile boolean m_doMakeReplicated = false;

        public LoadThread(
                RandomGenerator generator,
                TimestampType generationDateTime,
                ScaleParameters parameters,
                int index) {
            super("Load Thread " + index);
            m_generator = generator;
            this.m_generationDateTime = generationDateTime;
            this.m_parameters = parameters;
        }

        @Override
        public void run() {
            Integer warehouseId = null;
            while ((warehouseId = m_availableWarehouseIds.poll()) != null) {
                System.err.println("Loading warehouse " + warehouseId);
                makeStock(warehouseId); // STOCK is made separately to reduce memory consumption
                createDataTables();
                makeWarehouse(warehouseId);
                for (int i = 0; i < m_dataTables.length; ++i) m_dataTables[i] = null;
            }
            if (m_doMakeReplicated) {
                try {
                    m_finishedLoadThreads.acquire(m_loadThreads.length - 1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                makeReplicated();
                m_doMakeReplicated = false;
            } else {
                m_finishedLoadThreads.release();
            }
        }

        public void start(boolean doMakeReplicated) {
            m_doMakeReplicated = doMakeReplicated;
            super.start();
        }

        private double makeTax() {
            return m_generator.fixedPoint(Constants.TAX_DECIMALS, Constants.MIN_TAX, Constants.MAX_TAX);
        }

        private String makeZip() {
            int length = Constants.ZIP_LENGTH - Constants.ZIP_SUFFIX.length();
            return m_generator.nstring(length, length) + Constants.ZIP_SUFFIX;
        }

        private HashSet<Integer> selectUniqueIds(int numUnique, int minimum, int maximum) {
            HashSet<Integer> rows = new HashSet<Integer>(numUnique * 2);
            for (int i = 0; i < numUnique; ++i) {
                int index;
                do {
                    index = m_generator.number(minimum, maximum);
                } while (rows.contains(index));
                assert !rows.contains(index);
                rows.add(index);
            }
            assert rows.size() == numUnique;
            return rows;
        }

        /** @returns a string with ORIGINAL_STRING at a random position. */
        private String fillOriginal(String data) {
            int originalLength = Constants.ORIGINAL_STRING.length();
            int position = m_generator.number(0, data.length() - originalLength);
            String out = data.substring(0, position) + Constants.ORIGINAL_STRING +
                    data.substring(position + originalLength);
            assert out.length() == data.length();
            return out;
        }

        /** Inserts a new item with id, generated according to the TPC-C specification 4.3.3.1.
         * @param items
         * @param id
         * @param original
         */
        public void generateItem(VoltTable items, long id, boolean original) {
            long i_id = id;
            long i_im_id = m_generator.number(Constants.MIN_IM, Constants.MAX_IM);
            String i_name = m_generator.astring(Constants.MIN_I_NAME, Constants.MAX_I_NAME);
            double i_price = m_generator.fixedPoint(
                    Constants.MONEY_DECIMALS, Constants.MIN_PRICE, Constants.MAX_PRICE);
            String i_data = m_generator.astring(Constants.MIN_I_DATA, Constants.MAX_I_DATA);

            if (original) {
                i_data = fillOriginal(i_data);
            }

            items.addRow(i_id, i_im_id, i_name, i_price, i_data);
        }

        public void generateWarehouse(long w_id) {
            double w_tax = makeTax();
            double w_ytd = Constants.INITIAL_W_YTD;

            ArrayList<Object> insertParameters = new ArrayList<Object>();
            insertParameters.add(w_id);
            addAddress(insertParameters);
            insertParameters.addAll(Arrays.asList(w_tax, w_ytd));

            m_dataTables[IDX_WAREHOUSES].addRow(insertParameters.toArray());
        }

        public void generateDistrict(long d_w_id, long d_id) {
            double d_tax = makeTax();
            double d_ytd = Constants.INITIAL_D_YTD;
            long d_next_o_id = m_parameters.customersPerDistrict + 1;

            ArrayList<Object> insertParameters = new ArrayList<Object>(Arrays.asList(d_id, d_w_id));
            addAddress(insertParameters);
            insertParameters.addAll(Arrays.asList(new Object[]{d_tax, d_ytd, d_next_o_id}));

            m_dataTables[IDX_DISTRICTS].addRow(insertParameters.toArray());
        }

        public void generateCustomer(long c_w_id, long c_d_id, long c_id, boolean badCredit,
                boolean doesReplicateName) {
            final Object[] containerCustomer = new Object[6 + 5 + 10];

            String c_first = m_generator.astring(Constants.MIN_FIRST, Constants.MAX_FIRST);
            String c_middle = Constants.MIDDLE;

            assert 1 <= c_id && c_id <= Constants.CUSTOMERS_PER_DISTRICT;
            String c_last;
            if (c_id <= 1000) {
                c_last = m_generator.makeLastName((int) c_id-1);
            } else {
                c_last = m_generator.makeRandomLastName(m_parameters.customersPerDistrict);
            }

            String c_phone = m_generator.nstring(Constants.PHONE, Constants.PHONE);
            TimestampType c_since = m_generationDateTime;
            String c_credit = badCredit ? Constants.BAD_CREDIT : Constants.GOOD_CREDIT;
            double c_credit_lim = Constants.INITIAL_CREDIT_LIM;
            double c_discount = m_generator.fixedPoint(
                    Constants.DISCOUNT_DECIMALS, Constants.MIN_DISCOUNT, Constants.MAX_DISCOUNT);
            double c_balance = Constants.INITIAL_BALANCE;
            double c_ytd_payment = Constants.INITIAL_YTD_PAYMENT;
            long c_payment_cnt = Constants.INITIAL_PAYMENT_CNT;
            long c_delivery_cnt = Constants.INITIAL_DELIVERY_CNT;
            String c_data = m_generator.astring(Constants.MIN_C_DATA, Constants.MAX_C_DATA);

            int ind = 0;
            containerCustomer[ind++] = c_id;
            containerCustomer[ind++] = c_d_id;
            containerCustomer[ind++] = c_w_id;
            containerCustomer[ind++] = c_first;
            containerCustomer[ind++] = c_middle;
            containerCustomer[ind++] = c_last;

            String street1 = m_generator.astring(Constants.MIN_STREET, Constants.MAX_STREET);
            String street2 = m_generator.astring(Constants.MIN_STREET, Constants.MAX_STREET);
            String city = m_generator.astring(Constants.MIN_CITY, Constants.MAX_CITY);
            String state = m_generator.astring(Constants.STATE, Constants.STATE);
            String zip = makeZip();
            containerCustomer[ind++] = street1;
            containerCustomer[ind++] = street2;
            containerCustomer[ind++] = city;
            containerCustomer[ind++] = state;
            containerCustomer[ind++] = zip;

            containerCustomer[ind++] = c_phone;
            containerCustomer[ind++] = c_since;
            containerCustomer[ind++] = c_credit;
            containerCustomer[ind++] = c_credit_lim;
            containerCustomer[ind++] = c_discount;
            containerCustomer[ind++] = c_balance;
            containerCustomer[ind++] = c_ytd_payment;
            containerCustomer[ind++] = c_payment_cnt;
            containerCustomer[ind++] = c_delivery_cnt;
            containerCustomer[ind++] = c_data;
            m_dataTables[IDX_CUSTOMERS].addRow(containerCustomer);
            if (doesReplicateName) {
                //replicate name and id to every site
                synchronized (s_customerNamesTables) {
                    VoltTable customerNames = s_customerNamesTables.peekFirst();
                    if (customerNames == null || customerNames.getRowCount() > 32760) {
                        customerNames = new VoltTable(CUSTOMER_TABLE_COLUMN_INFO);
                        s_customerNamesTables.push(customerNames);
                    }
                    customerNames.addRow(c_id, c_d_id, c_w_id, c_first, c_last);
                }
            }
        }

        /** Appends the name and address fields to insertParameters. Used by both generateWarehouse and
        generateDistrict. */
        private void addAddress(ArrayList<Object> insertParameters) {
            String name = m_generator.astring(Constants.MIN_NAME, Constants.MAX_NAME);
            insertParameters.add(name);
            addStreetAddress(insertParameters);
        }

        /** Appends the street address fields to insertParameters. Used for warehouses, districts and
        customers. */
        private void addStreetAddress(ArrayList<Object> insertParameters) {
            String street1 = m_generator.astring(Constants.MIN_STREET, Constants.MAX_STREET);
            String street2 = m_generator.astring(Constants.MIN_STREET, Constants.MAX_STREET);
            String city = m_generator.astring(Constants.MIN_CITY, Constants.MAX_CITY);
            String state = m_generator.astring(Constants.STATE, Constants.STATE);
            String zip = makeZip();

            insertParameters.addAll(Arrays.asList(street1, street2, city, state, zip));
        }

        public void generateStock(long s_w_id, long s_i_id, boolean original) {
            final Object[] containerStock = new Object[3 + Constants.DISTRICTS_PER_WAREHOUSE + 4];
            long s_quantity = m_generator.number(Constants.MIN_QUANTITY, Constants.MAX_QUANTITY);
            long s_ytd = 0;
            long s_order_cnt = 0;
            long s_remote_cnt = 0;

            String s_data = m_generator.astring(Constants.MIN_I_DATA, Constants.MAX_I_DATA);
            if (original) {
                s_data = fillOriginal(s_data);
            }
            int ind = 0;
            containerStock[ind++] = s_i_id;
            containerStock[ind++] = s_w_id;
            containerStock[ind++] = s_quantity;
            for (int i = 0; i < Constants.DISTRICTS_PER_WAREHOUSE; ++i) {
                String s_dist_x = m_generator.astring(Constants.DIST, Constants.DIST);
                containerStock[ind++] = s_dist_x;
            }
            containerStock[ind++] = s_ytd;
            containerStock[ind++] = s_order_cnt;
            containerStock[ind++] = s_remote_cnt;
            containerStock[ind++] = s_data;

            m_dataTables[IDX_STOCKS].addRow(containerStock);
        }

        /* returns the generated o_ol_cnt value. */
        public long generateOrder(long o_w_id, long o_d_id, long o_id, long o_c_id, boolean newOrder) {
            TimestampType o_entry_d = m_generationDateTime;
            long o_carrier_id;
            if (!newOrder) {
                o_carrier_id = m_generator.number(Constants.MIN_CARRIER_ID, Constants.MAX_CARRIER_ID);
            } else {
                o_carrier_id = Constants.NULL_CARRIER_ID;
            }
            long o_ol_cnt = m_generator.number(Constants.MIN_OL_CNT, Constants.MAX_OL_CNT);
            long o_all_local = Constants.INITIAL_ALL_LOCAL;

            m_dataTables[IDX_ORDERS].addRow(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local);
            return o_ol_cnt;
        }

        public void generateOrderLine(long ol_w_id, long ol_d_id, long ol_o_id, long ol_number,
                boolean newOrder) {
            long ol_i_id = m_generator.number(1, m_parameters.items);
            long ol_supply_w_id = ol_w_id;
            TimestampType ol_delivery_d = m_generationDateTime;
            long ol_quantity = Constants.INITIAL_QUANTITY;

            double ol_amount;
            if (!newOrder) {
                ol_amount = 0.00;
            } else {
                ol_amount = m_generator.fixedPoint(Constants.MONEY_DECIMALS, Constants.MIN_AMOUNT,
                        Constants.MAX_PRICE * Constants.MAX_OL_QUANTITY);
                ol_delivery_d = null;
            }
            String ol_dist_info = m_generator.astring(Constants.DIST, Constants.DIST);

            m_dataTables[IDX_ORDERLINES].addRow(ol_o_id, ol_d_id, ol_w_id, ol_number,
                    ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info);
        }
        //private long max_hid = 0;
        public void generateHistory(long h_c_w_id, long h_c_d_id, long h_c_id) {
            long h_w_id = h_c_w_id;
            long h_d_id = h_c_d_id;
            TimestampType h_date = m_generationDateTime;
            double h_amount = Constants.INITIAL_AMOUNT;
            String h_data = m_generator.astring(Constants.MIN_DATA, Constants.MAX_DATA);

            m_dataTables[IDX_HISTORIES].addRow(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data);
        }

        /** STOCK is made in different method to reduce memory consumption. */
        public void makeStock(int w_id) {

            // Select 10% of the stock to be marked "original"

            final int BATCH = 5;
            final int BATCH_SIZE = (m_parameters.items / BATCH);
            m_dataTables[IDX_STOCKS] = new VoltTable(
                    new VoltTable.ColumnInfo("S_I_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("S_QUANTITY", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_DIST_01", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_02", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_03", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_04", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_05", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_06", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_07", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_08", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_09", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_10", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_YTD", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_ORDER_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_REMOTE_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_DATA", VoltType.STRING)
            );
            //t.ensureRowCapacity(parameters.items / BATCH);
            //t.ensureStringCapacity(parameters.items * (32 * 10 + 64) / BATCH);

            HashSet<Integer> selectedRows = selectUniqueIds(m_parameters.items/10, 1, m_parameters.items);

            for (int i_id = 1; i_id <= m_parameters.items; ++i_id) {
                boolean original = selectedRows.contains(i_id);
                generateStock(w_id, i_id, original);
                if (i_id % BATCH_SIZE == 0) {
                    commitDataTables(w_id);
                    //System.err.printf("%d/%d\n", i_id, m_parameters.items);
                }
            }
            if (m_dataTables[IDX_STOCKS].getRowCount() != 0) {
                commitDataTables(w_id);
            }
            m_dataTables[IDX_STOCKS] = null;


        }


        // TODO(evanj): The C++ version has tests  for this code that could be ported over, but it would
        // need a fair bit of hacking in the unit test to make them work, since it requires storing all
        // the inserted tuples.
        public void makeWarehouse(long w_id) {
            generateWarehouse(w_id);

            for (int d_id = 1; d_id <= m_parameters.districtsPerWarehouse; ++d_id) {
                //System.err.printf("Beginning District: %d\n", d_id);
                generateDistrict(w_id, d_id);

                // Select 10% of the customers to have bad credit
                HashSet<Integer> selectedRows = selectUniqueIds(m_parameters.customersPerDistrict/10, 1,
                        m_parameters.customersPerDistrict);
                // long[] c_ids = new long[customersPerDistrict];
                for (int c_id = 1; c_id <= m_parameters.customersPerDistrict; ++c_id) {
                    boolean badCredit = selectedRows.contains(c_id);
                    generateCustomer(w_id, d_id, c_id, badCredit, true);
                    generateHistory(w_id, d_id, c_id);
                }

                // TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
                // is a c_id field, it seems to make sense to have it be a permutation of the
                // customers. For the "real" thing this will be equivalent
                int[] cIdPermutation = new int[m_parameters.customersPerDistrict];
                for (int i = 0; i < m_parameters.customersPerDistrict; ++i) {
                    cIdPermutation[i] = i+1;
                }
                assert cIdPermutation[0] == 1;
                assert cIdPermutation[m_parameters.customersPerDistrict-1] ==
                        m_parameters.customersPerDistrict;
                Collections.shuffle(Arrays.asList(cIdPermutation));

                for (int o_id = 1; o_id <= m_parameters.customersPerDistrict; ++o_id) {
                    // The last newOrdersPerDistrict are new orders
                    boolean newOrder =
                            m_parameters.customersPerDistrict - m_parameters.newOrdersPerDistrict < o_id;
                    long o_ol_cnt = generateOrder(w_id, d_id, o_id, cIdPermutation[o_id-1], newOrder);

                    // Generate each OrderLine for the order
                    for (int ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
                        generateOrderLine(w_id, d_id, o_id, ol_number, newOrder);
                    }

                    if (newOrder) {
                        // This is a new order: make one for it
                        m_dataTables[IDX_NEWORDERS].addRow((long) o_id, (long) d_id, w_id);
                    }
                }
                commitDataTables(w_id); // flushout current data to avoid outofmemory
            }
        }
        /** generate replicated tables, ITEM and CUSTOMER_NAME. */
        public void makeReplicated() {
            // create ITEMS here to reduce memory consumption
            VoltTable items = new VoltTable(
                    new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                    new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("I_DATA", VoltType.STRING)
            );
            //items.ensureRowCapacity(parameters.items);
            //items.ensureStringCapacity(parameters.items * 96);
            // Select 10% of the rows to be marked "original"
            HashSet<Integer> originalRows = selectUniqueIds(m_parameters.items/10, 1, m_parameters.items);
            for (int i = 1; i <= m_parameters.items; ++i) {
                // if we're on a 10% boundary, print out some nice status info
                //if (i % (m_parameters.items / 10) == 0)
                //    System.err.printf("   %d%%\n", (i * 100) / m_parameters.items);

                boolean original = originalRows.contains(i);
                generateItem(items, i, original);
            }

            final int numPermits = 48;
            final Semaphore maxOutstandingInvocations = new Semaphore(numPermits);
            final int totalInvocations = s_customerNamesTables.size() * m_parameters.warehouses;
            final ProcedureCallback callback = new ProcedureCallback() {
                private int invocationsCompleted = 0;

                private double lastPercentCompleted = 0.0;
                @Override
                public synchronized void clientCallback(ClientResponse clientResponse) {
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                        System.err.println(clientResponse.getStatusString());
                        System.exit(-1);
                    }
                    invocationsCompleted++;
                    final double percentCompleted = invocationsCompleted / (double)totalInvocations;
                    if (percentCompleted > lastPercentCompleted + .1) {
                        lastPercentCompleted = percentCompleted;
                        System.err.println("Finished " + invocationsCompleted + "/" +
                                totalInvocations + " replicated load work");
                    }
                    maxOutstandingInvocations.release();
                }

            };

            System.err.println("Customer names tables broken into " + s_customerNamesTables.size() + " chunks");

            for (VoltTable customerNameChunk : s_customerNamesTables) {
                /*
                 * Only supply item table the first time.
                 */
                try {
                    boolean queued = false;
                    while (!queued) {
                        maxOutstandingInvocations.acquire();
                        queued = m_voltClient.executeAsync(callback, Constants.LOAD_WAREHOUSE_REPLICATED,
                                items, customerNameChunk);
                        items = null;
                        m_voltClient.backpressureBarrier();
                    }
                }
                catch (Exception e) {
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }
            }

            try {
                maxOutstandingInvocations.acquire(numPermits);
                System.err.println("Finished all replicated load work");
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        /** Send to data to VoltDB and/or to the jdbc connection */
        private void commitDataTables(long w_id) {
            if (m_voltClient != null) {
                commitDataTables_VoltDB(w_id);
            }
            for (int i = 0; i < m_dataTables.length; ++i) {
                if (m_dataTables[i] != null) {
                    m_dataTables[i].clearRowData();
                }
            }
        }

        private void commitDataTables_VoltDB(long w_id) {
            Object[] params = new Object[m_dataTables.length + 1];
            params[0] = (short)w_id;
            for (int i = 0; i < m_dataTables.length; ++i) {
                if (m_dataTables[i] != null && m_dataTables[i].getRowCount() > 0) {
                    params[i + 1] = m_dataTables[i];
                }
            }
            rethrowExceptionLoad(Constants.LOAD_WAREHOUSE, params);
        }

        private void createDataTables() {
            //customerNames.ensureRowCapacity(parameters.warehouses * parameters.districtsPerWarehouse * parameters.customersPerDistrict);
            //customerNames.ensureStringCapacity(parameters.warehouses * parameters.districtsPerWarehouse * parameters.customersPerDistrict * (64));

            //non replicated tables
            for (int i = 0; i < m_dataTables.length; ++i) m_dataTables[i] = null;
            m_dataTables[IDX_WAREHOUSES] = new VoltTable(
                    new VoltTable.ColumnInfo("W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("W_NAME", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_STREET_1", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_STREET_2", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_CITY", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_STATE", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_ZIP", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_TAX", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("W_YTD", VoltType.FLOAT)
            );
            //t.ensureRowCapacity(1);
            //t.ensureStringCapacity(200);

            m_dataTables[IDX_DISTRICTS] = new VoltTable(
                    new VoltTable.ColumnInfo("D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("D_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("D_NAME", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_STREET_1", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_STREET_2", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_CITY", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_STATE", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_ZIP", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_TAX", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("D_YTD", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("D_NEXT_O_ID", VoltType.INTEGER)
            );
            //t.ensureRowCapacity(1);
            //t.ensureStringCapacity(1 * (16 + 96 + 2 + 9));

            m_dataTables[IDX_CUSTOMERS] = new VoltTable(
                    new VoltTable.ColumnInfo("C_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("C_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("C_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("C_FIRST", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_MIDDLE", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_LAST", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_STREET_1", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_STREET_2", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_CITY", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_STATE", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_ZIP", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_PHONE", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_SINCE", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("C_CREDIT", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_CREDIT_LIM", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_DISCOUNT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_BALANCE", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_YTD_PAYMENT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_PAYMENT_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("C_DELIVERY_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("C_DATA", VoltType.STRING)
            );
            //t.ensureRowCapacity(parameters.customersPerDistrict);
            //t.ensureStringCapacity(parameters.customersPerDistrict * (32 * 6 + 2 * 3 + 9 + 500));

            m_dataTables[IDX_ORDERS] = new VoltTable(
                    new VoltTable.ColumnInfo("O_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("O_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("O_C_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_ENTRY_D", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("O_CARRIER_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_OL_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_ALL_LOCAL", VoltType.INTEGER)
            );
            //t.ensureRowCapacity(parameters.customersPerDistrict);

            m_dataTables[IDX_NEWORDERS] = new VoltTable(
                    new VoltTable.ColumnInfo("NO_O_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("NO_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("NO_W_ID", VoltType.SMALLINT)
            );
            //t.ensureRowCapacity(parameters.customersPerDistrict);

            m_dataTables[IDX_ORDERLINES] = new VoltTable(
                    new VoltTable.ColumnInfo("OL_O_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("OL_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("OL_NUMBER", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_I_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_SUPPLY_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("OL_DELIVERY_D", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("OL_QUANTITY", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_AMOUNT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("OL_DIST_INFO", VoltType.STRING)
            );
            //t.ensureRowCapacity(parameters.customersPerDistrict * Constants.MAX_OL_CNT);
            //t.ensureStringCapacity(parameters.customersPerDistrict * Constants.MAX_OL_CNT * (32));

            m_dataTables[IDX_HISTORIES] = new VoltTable(
                    new VoltTable.ColumnInfo("H_C_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("H_C_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("H_C_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("H_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("H_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("H_DATE", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("H_AMOUNT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("H_DATA", VoltType.STRING)
            );
            //t.ensureRowCapacity(parameters.customersPerDistrict);
            //t.ensureStringCapacity(parameters.customersPerDistrict * (32));
        }
    }

    private ConcurrentLinkedQueue<Integer> m_availableWarehouseIds = new ConcurrentLinkedQueue<Integer>();

    public void run() throws NoConnectionsException {
        ArrayList<Integer> warehouseIds = new ArrayList<Integer>();
        for (int ii = 1; ii <= m_warehouses; ii++) {
            warehouseIds.add(ii);
        }
        //Shuffling spreads the loading out across physical hosts better
        Collections.shuffle(warehouseIds);
        m_availableWarehouseIds.addAll(warehouseIds);

        boolean doMakeReplicated = true;
        for (LoadThread loadThread : m_loadThreads) {
            loadThread.start(doMakeReplicated);
            doMakeReplicated = false;
        }

        for (int ii = 0; ii < m_loadThreads.length; ii++) {
            try {
                m_loadThreads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        try {
            m_voltClient.drain();
        } catch (InterruptedException e) {
            return;
        }
    }
}
