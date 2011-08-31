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

package org.voltdb.benchmark.multisite;

import java.net.URL;

import org.voltdb.benchmark.multisite.procedures.ChangeSeat;
import org.voltdb.benchmark.multisite.procedures.FindOpenSeats;
import org.voltdb.benchmark.multisite.procedures.LoadTables;
import org.voltdb.benchmark.multisite.procedures.UpdateReservation;
import org.voltdb.compiler.VoltProjectBuilder;

public class MultisiteProjectBuilder extends VoltProjectBuilder {

    public static Class<?> PROCEDURES[] = new Class<?>[] {
        FindOpenSeats.class,
        UpdateReservation.class,
        ChangeSeat.class,
        LoadTables.class
    };

    public static String partitioning[][] =
        new String[][] {{"CUSTOMERS", "CID"},
                        {"RESERVATIONS", "FID"}};


    public static final URL ddlURL =
        MultisiteProjectBuilder.class.getResource("multisite-ddl.sql");

    private static final String m_jarFileName = "multisite.jar";

    @Override
    public String[] compileAllCatalogs(int sitesPerHost,
                                       int length,
                                       int kFactor,
                                       String voltRoot) {
        addAllDefaults();
        boolean compile = compile(m_jarFileName, sitesPerHost,
                                  length, kFactor, voltRoot);
        if (!compile) {
            throw new RuntimeException("Bingo project builder failed app compilation.");
        }
        return new String[] {m_jarFileName};
     }

    @Override
    public void addAllDefaults() {
        addProcedures(
                      LoadTables.class,
                      ChangeSeat.class,
                      UpdateReservation.class,
                      FindOpenSeats.class);
        addSchema(ddlURL);
        addPartitionInfo("CUSTOMERS", "CID");
        addPartitionInfo("RESERVATIONS", "FID");
    }

}
