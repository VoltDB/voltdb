/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.voltdb.VoltTable;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.Client;

public abstract class TPCDataPrinter {

    public static final HashMap<Integer, String> indexMap = new HashMap<Integer, String>();
    public static final HashMap<String, Integer> nameMap = new HashMap<String, Integer>();
    static {
        indexMap.put(0, "WAREHOUSE");
        indexMap.put(1, "DISTRICT");
        indexMap.put(2, "ITEM");
        indexMap.put(3, "CUSTOMER");
        indexMap.put(4, "HISTORY");
        indexMap.put(5, "STOCK");
        indexMap.put(6, "ORDERS");
        indexMap.put(7, "NEW_ORDER");
        indexMap.put(8, "ORDER_LINE");

        // Invert the map
        for (Map.Entry<Integer, String> e : indexMap.entrySet()) {
            assert !nameMap.containsKey(e.getValue());
            nameMap.put(e.getValue(), e.getKey());
        }
    }

    public static final String tppcPrintout = "";   //rkallman

    public static HashMap<String, VoltTable> getAllData(Client client) {
        VoltTable[] tables = null;
        try {
            tables = client.callProcedure("SelectAll").getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HashMap<String, VoltTable> data = new HashMap<String, VoltTable>();
        for (int i : indexMap.keySet()) {
            data.put(indexMap.get(i), tables[i]);
        }
        return data;
    }

    public static void printAllData(Client client) {
        VoltTable[] tables = null;
        try {
            tables = client.callProcedure("SelectAll").getResults();
        } catch (ProcCallException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i : indexMap.keySet()) {
            VoltTable table = tables[i];
            String name = indexMap.get(i);
            System.out.println("===============================");
            System.out.println("Table " + name + " has " + table.getRowCount() + " rows.");
            printTable(table);
        }
    }

    public static void printTable(VoltTable table) {
        System.out.println(table.toString());
    }

}
