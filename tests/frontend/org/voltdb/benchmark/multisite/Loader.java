/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

package org.voltdb.benchmark.multisite;

import java.io.IOException;
import java.util.Random;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.SyncCallback;
import org.voltdb.benchmark.ClientMain;


public class Loader extends ClientMain {

    // @244 bytes per row, approximately 24MB chunks
    private static int kCustomerBatchSize = 100000;
    public static int kMaxCustomers = 50000000;

    // @134 bytes per row, less than 10MB chunks
    // This table is replicated and copied NODE times in the msg layer.
    private static int kFlightBatchSize = 50000;
    public static int kMaxFlights = 1000000;

    // @52 bytes per row, approximately 24MB chunks
    private static int kReservationBatchSize = 1000000;

    // scale all table cardinalities by this factor
    private int m_scalefactor = 1;

    // used internally
    private final Random m_rng;

    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(Loader.class, args, true);
    }

    public Loader(String[] args) {
        super(args);
        m_rng = new Random();

        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1)
                continue;

            if (parts[1].startsWith("${"))
                continue;

            if (parts[0].equals("sf"))
                m_scalefactor = Integer.parseInt(parts[1]);
        }
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    @Override
    protected void runLoop() throws NoConnectionsException {
        Thread C = new Thread() {
            @Override
            public void run() {
                generateCustomers();
            }
        };

        Thread F = new Thread() {
            @Override
            public void run() {
                generateFlights();
            }
        };

        Thread R = new Thread() {
            @Override
            public void run() {
                generateReservations();
            }
        };

        C.start();
        F.start();
        R.start();

        try {
            C.join();
            F.join();
            R.join();
            m_voltClient.drain();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.err.println("Multisite loader done.");
    }


    // taken from tpcc.RandomGenerator
    /** @returns a random alphabetic string with length in range [minimum_length, maximum_length].
     */
     public String astring(int minimum_length, int maximum_length) {
         return randomString(minimum_length, maximum_length, 'a', 26);
     }

     // taken from tpcc.RandomGenerator
     /** @returns a random numeric string with length in range [minimum_length, maximum_length].
     */
     public String nstring(int minimum_length, int maximum_length) {
         return randomString(minimum_length, maximum_length, '0', 10);
     }

     // taken from tpcc.RandomGenerator
     public String randomString(int minimum_length, int maximum_length, char base,
             int numCharacters) {
         int length = number(minimum_length, maximum_length);
         byte baseByte = (byte) base;
         byte[] bytes = new byte[length];
         for (int i = 0; i < length; ++i) {
             bytes[i] = (byte)(baseByte + number(0, numCharacters-1));
         }
         return new String(bytes);
     }

     // taken from tpcc.RandomGenerator
     public int number(int minimum, int maximum) {
         assert minimum <= maximum;
         int range_size = maximum - minimum + 1;
         int value = m_rng.nextInt(range_size);
         value += minimum;
         assert minimum <= value && value <= maximum;
         return value;
     }

    /**
     *   Define the internal tables that will be populated and sent to
     *   VoltDB.
     */
    VoltTable initializeCustomerDataTable() {
       VoltTable tbl = new VoltTable(
               new VoltTable.ColumnInfo("CID", VoltType.INTEGER),
               new VoltTable.ColumnInfo("SATTR00", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR01", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR02", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR03", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR04", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR05", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR06", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR07", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR08", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR09", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR10", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR11", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR12", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR13", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR14", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR15", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR16", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR17", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR18", VoltType.STRING),
               new VoltTable.ColumnInfo("SATTR19", VoltType.STRING),
               new VoltTable.ColumnInfo("IATTR00", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR01", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR02", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR03", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR04", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR05", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR06", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR07", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR08", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR09", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR10", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR11", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR12", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR13", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR14", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR15", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR16", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR17", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR18", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR19", VoltType.INTEGER)
       );
      return tbl;
    }

    VoltTable initializeFlightDataTable() {
       VoltTable tbl = new VoltTable(
               new VoltTable.ColumnInfo("FID", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR00", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR01", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR02", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR03", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR04", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR05", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR06", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR07", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR08", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR09", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR10", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR11", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR12", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR13", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR14", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR15", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR16", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR17", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR18", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR19", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR20", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR21", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR22", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR23", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR24", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR25", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR26", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR27", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR28", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR29", VoltType.INTEGER)
       );
       return tbl;
    }

    VoltTable initializeReservationDataTable() {
       VoltTable tbl = new VoltTable(
               new VoltTable.ColumnInfo("RID", VoltType.INTEGER),
               new VoltTable.ColumnInfo("CID", VoltType.INTEGER),
               new VoltTable.ColumnInfo("FID", VoltType.INTEGER),
               new VoltTable.ColumnInfo("SEAT", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR00", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR01", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR02", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR03", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR04", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR05", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR06", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR07", VoltType.INTEGER),
               new VoltTable.ColumnInfo("IATTR08", VoltType.INTEGER)
       );
       return tbl;
    }

    /**
     *   Populate customers table per benchmark spec.
     */
    void generateCustomers() {
        int cid = 0;
        VoltTable customerTbl = initializeCustomerDataTable();
        Object row[] = new Object[customerTbl.getColumnCount()];

        while (cid < kMaxCustomers / m_scalefactor) {
            int col = 0;
            row[col++] = new Integer(cid++);
            for (int j=0; j < 20; ++j) {
                row[col++] = astring(6,8);
            }
            for (int j=0; j < 20; ++j) {
                row[col++] = number(0, 1<<30);
            }
            assert (col == customerTbl.getColumnCount());
            customerTbl.addRow(row);

            if (customerTbl.getRowCount() >= kCustomerBatchSize) {
                System.err.printf("CUSTOMERS: loading %d rows (cid %d of %d)\n",
                                  customerTbl.getRowCount(), cid, (kMaxCustomers / m_scalefactor));
                loadTable("CUSTOMERS", customerTbl);
                 customerTbl.clearRowData();
            }
        }
        System.err.println("CUSTOMERS: loading final " + customerTbl.getRowCount() + " rows.");
        loadTable("CUSTOMERS", customerTbl);
        customerTbl.clearRowData();
    }

    /**
     * Populate FLIGHTS per benchmark spec
     */
    void generateFlights() {
        int fid = 0;
        VoltTable flightTbl = initializeFlightDataTable();
        Object row[] = new Object[flightTbl.getColumnCount()];

        while (fid < kMaxFlights / m_scalefactor) {
            int col = 0;
            row[col++] = new Integer(fid++);
            for (int j=0; j < 30; ++j) {
                row[col++] = number(0, 1<<30);
            }
            assert (col == flightTbl.getColumnCount());
            flightTbl.addRow(row);

            if (flightTbl.getRowCount() >= kFlightBatchSize) {
                System.err.printf("FLIGHTS: loading %d rows (fid %d of %d)\n",
                                  flightTbl.getRowCount(), fid, (kMaxFlights / m_scalefactor));
                loadTable("FLIGHTS", flightTbl);
                flightTbl.clearRowData();
            }
        }
        System.err.println("FLIGHTS: loading final " + flightTbl.getRowCount() + " rows.");
        loadTable("FLIGHTS", flightTbl);
        flightTbl.clearRowData();
    }

    /**
     * Populate RESERVATIONS per benchmark spec
     */
    void generateReservations() {
        // iterate through the flights.
        // pick 70-90% of the 150 seats in the flight.
        // pick a customer (who is not booked for this flight).
        // create a reservation row.

        // assign cids to fids in a deterministic way so
        // that (cid, fid) pairs can be generated by the
        // benchmark for the ChangeSeat procedure.

        /* Creates a table of cids like this (using 6
         * seats per aircraft, 10 total customers)

                 SEAT
                 1  2   3   4   5   6
           FID
            1    1   4   7  10  3   6
            2    2   5   8  1   4   7
            3    3   6   9  2   5   8

          cid = f(fid,seat) = (maxfid * (seat -1) + fid) % maxcust
          is in range to be on the flight.

          (Actually, I think this gives a wrong answer (0 v. max) for
          the maximum customer ID. 0 is an invalid cid so this can be
          caught and fixed...)

         */

        int maxcids = kMaxCustomers / m_scalefactor;
        int maxfids = kMaxFlights / m_scalefactor;

        int cid = 1;
        int rid = 1;

        VoltTable reservationTbl = initializeReservationDataTable();
        Object row[] = new Object[reservationTbl.getColumnCount()];

        for (int seatnum = 1; seatnum < 151; ++seatnum) {
            for (int fid = 1; fid < maxfids + 1; ++fid) {
                // always advance cid, even if seat is empty
                cid = (cid++ % maxcids);

                if (seatIsOccupied()) {
                    int col = 0;
                    row[col++] = new Integer(rid++);
                    row[col++] = new Integer(cid);
                    row[col++] = new Integer(fid);
                    row[col++] = new Integer(seatnum);
                    for (int j=0; j < 9; ++j) {
                        row[col++] = number(1, 1<<30);
                    }
                    reservationTbl.addRow(row);
                    if (reservationTbl.getRowCount() >= kReservationBatchSize) {
                        System.err.printf("RESERVATIONS: loading %d rows (fid %d of %d)\n",
                                          reservationTbl.getRowCount(), fid, maxfids);
                        loadTable("RESERVATIONS", reservationTbl);
                        reservationTbl.clearRowData();
                    }
                }

            }
        }
        System.err.println("RESERVATIONS: loading final " + reservationTbl.getRowCount() + " rows.");
        loadTable("RESERVATIONS", reservationTbl);
        reservationTbl.clearRowData();
    }

    /**
     * Give each seat a 70%-90% probability of being occupied
     */
    boolean seatIsOccupied() {
        if (number(1,100) < number(70,90))
            return true;
        return false;
    }

    void loadTable(String tablename, VoltTable table) {
        SyncCallback cb = new SyncCallback();
        try {
            m_voltClient.callProcedure(cb, "@LoadMultipartitionTable", tablename, table);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            cb.waitForResponse();
        } catch (InterruptedException e) {
            e.printStackTrace();
        };
    }

    @Override
    protected String getApplicationName() {
        return "Multisite Benchmark";
    }

    @Override
    protected String getSubApplicationName() {
        return "Loader";
    }
}

