/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.lang.reflect.Field;
import java.util.HashMap;

public class AllTpccSQL {
    public HashMap<String, String> selects = new HashMap<String, String>();
    public HashMap<String, String> updates = new HashMap<String, String>();
    public HashMap<String, String> deletes = new HashMap<String, String>();
    public HashMap<String, String> inserts = new HashMap<String, String>();

    public HashMap<String, HashMap<String, String>> stmts = new HashMap<String, HashMap<String, String>>();

    Class<?>[] procs = new Class<?>[] {
        // tpcc
        org.voltdb.benchmark.tpcc.procedures.delivery.class,
        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId.class,
        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName.class,
        org.voltdb.benchmark.tpcc.procedures.neworder.class,
        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId.class,
        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName.class,
        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerNameC.class,
        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerIdC.class,
        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerNameW.class,
        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerIdW.class,
        org.voltdb.benchmark.tpcc.procedures.slev.class
    };

    public AllTpccSQL() {
        for (Class<?> cls : procs) {
            HashMap<String, String> procStmts = new HashMap<String, String>();
            stmts.put(cls.getName(), procStmts);

            VoltProcedure procInstance = null;
            try {
                procInstance = (VoltProcedure) cls.newInstance();
            } catch (InstantiationException e1) {
                e1.printStackTrace();
            } catch (IllegalAccessException e1) {
                e1.printStackTrace();
            }

            Field[] fields = cls.getFields();
            for (Field f : fields) {
                if (f.getType() == SQLStmt.class) {
                    //String fieldName = f.getName();
                    SQLStmt stmt = null;

                    try {
                        stmt = (SQLStmt) f.get(procInstance);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }

                    String sql = stmt.getText();
                    sql = sql.trim();
                    assert(sql != null);
                    String name = cls.getSimpleName() + "-" + f.getName();

                    procStmts.put(name, sql);
                    String prefix = sql.substring(0, 6);
                    if (prefix.equalsIgnoreCase("select"))
                        selects.put(name, sql);
                    else if (prefix.equalsIgnoreCase("update"))
                        updates.put(name, sql);
                    else if (prefix.equalsIgnoreCase("insert"))
                        inserts.put(name, sql);
                    else if (prefix.equalsIgnoreCase("delete"))
                        deletes.put(name, sql);
                    else
                        throw new RuntimeException("SQL Statement unrecognizable");
                }
            }
        }
    }
}
