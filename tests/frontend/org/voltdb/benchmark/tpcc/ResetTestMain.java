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

import java.io.File;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.voltdb.BackendTarget;
import org.voltdb.ClientInterface;
import org.voltdb.ExecutionSite;
import org.voltdb.ServerThread;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched;
import org.voltdb.benchmark.tpcc.procedures.ResetWarehouse;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.BuildDirectoryUtils;

public class ResetTestMain {
    public static void main(final String[] args) throws Exception {
        //noisy!
        //Logger.getLogger(VoltProcedure.class.getName()).setLevel(Level.SEVERE);
        Logger.getLogger(Client.class.getName()).setLevel(Level.SEVERE);
        Logger.getLogger(ClientInterface.class.getName()).setLevel(Level.SEVERE);
        Logger.getLogger(ExecutionEngine.class.getName()).setLevel(Level.SEVERE);
        Logger.getLogger(ExecutionSite.class.getName()).setLevel(Level.SEVERE);
        Logger.getLogger(VoltProcedure.class.getName()).setLevel(Level.SEVERE);

        ScaleParameters parameters = ScaleParameters.makeWithScaleFactor(1, 1);
        RandomGenerator generator = new RandomGenerator.Implementation(0);

        String catalog = BuildDirectoryUtils.getBuildDirectoryPath() +
           File.pathSeparator + "tpcc.jar";


        TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(InsertOrderLineBatched.class, ResetWarehouse.class);
        pb.compile(catalog);

        ServerThread server = new ServerThread(catalog,  BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
        ClientConfig clientConfig = new ClientConfig("program", "none");
        Client client = ClientFactory.createClient(clientConfig);
        client.createConnection("localhost");

        Date generationDateTime = new Date();
        long tm = System.currentTimeMillis();

        System.out.println("making order line table...");
        tm = System.currentTimeMillis();
        // int BATCH_SIZE = parameters.districtsPerWarehouse * (parameters.customersPerDistrict / 30);
        int BATCH_SIZE = 1000;
        long[] b_ol_o_id = new long[BATCH_SIZE];
        long[] b_ol_d_id = new long[BATCH_SIZE];
        long[] b_ol_number = new long[BATCH_SIZE];
        long[] b_ol_i_id = new long[BATCH_SIZE];
        long[] b_ol_supply_w_id = new long[BATCH_SIZE];
        Date[] b_ol_delivery_d = new Date[BATCH_SIZE];
        long[] b_ol_quantity = new long[BATCH_SIZE];
        double[] b_ol_amount = new double[BATCH_SIZE];
        String[] b_ol_dist_info = new String[BATCH_SIZE];
        int total = 0;
        int batch_cnt = 0;
        int w_id = 1;
        int customersPerDistrictAfterInsertion = (int) (parameters.customersPerDistrict * 3.0);
        for (int d_id = 1; d_id <= parameters.districtsPerWarehouse; ++d_id) {
            for (int o_id = 1; o_id <= customersPerDistrictAfterInsertion; ++o_id) { //10% more
                // Generate each OrderLine for the order
                long o_ol_cnt = generator.number(Constants.MIN_OL_CNT, Constants.MAX_OL_CNT);
                boolean newOrder =
                    parameters.customersPerDistrict - parameters.newOrdersPerDistrict < o_id;
                for (int ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
                    //generateOrderLine(w_id, d_id, o_id, ol_number, newOrder);
                    //(long ol_w_id, long ol_d_id, long ol_o_id, long ol_number, boolean newOrder)
                    b_ol_o_id[batch_cnt] = o_id;
                    b_ol_d_id[batch_cnt] = d_id;
                    b_ol_number[batch_cnt] = ol_number;
                    b_ol_i_id[batch_cnt] = generator.number(1, parameters.items);
                    b_ol_supply_w_id[batch_cnt] = w_id;
                    b_ol_delivery_d[batch_cnt] = generationDateTime;
                    b_ol_quantity[batch_cnt] = Constants.INITIAL_QUANTITY;

                    if (!newOrder) {
                        b_ol_amount[batch_cnt] = 0.00;
                    } else {
                        b_ol_amount[batch_cnt] = generator.fixedPoint(Constants.MONEY_DECIMALS, Constants.MIN_AMOUNT,
                                Constants.MAX_PRICE * Constants.MAX_OL_QUANTITY);
                        b_ol_delivery_d[batch_cnt] = null;
                    }
                    b_ol_dist_info[batch_cnt] = generator.astring(Constants.DIST, Constants.DIST);
                    ++batch_cnt;
                    if (batch_cnt == BATCH_SIZE) {
                        total += BATCH_SIZE;
                        System.out.println ("loading: " + total + "/" + (parameters.districtsPerWarehouse * customersPerDistrictAfterInsertion * (Constants.MAX_OL_CNT - Constants.MIN_OL_CNT)));
                        client.callProcedure(InsertOrderLineBatched.class.getSimpleName(),
                            b_ol_o_id, b_ol_d_id, w_id, b_ol_number, b_ol_i_id,
                            b_ol_supply_w_id, b_ol_delivery_d, b_ol_quantity, b_ol_amount, b_ol_dist_info);
                        batch_cnt = 0;
                    }
                }
            }
        }

        System.out.println("created " + (System.currentTimeMillis() - tm) + "ms");
        tm = System.currentTimeMillis();

        //delete the 10% orderline
        client.callProcedure(ResetWarehouse.class.getSimpleName(),
                1L,
                (long) parameters.districtsPerWarehouse,
                (long) parameters.customersPerDistrict,
                (long) parameters.newOrdersPerDistrict);
        System.out.println("deleted " + (System.currentTimeMillis() - tm) + "ms");
        tm = System.currentTimeMillis();
        server.shutdown();
    }
}
