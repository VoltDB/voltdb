/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */

package org.voltdb.dtxn;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class NonDeterministic_RO_MP extends VoltProcedure {
    public static final SQLStmt sql = new SQLStmt("select * from kv");

    public VoltTable run() {
        // This will fail
        System.loadLibrary("VoltDBMissingLibraryTrap");
        return null;
    }

}
