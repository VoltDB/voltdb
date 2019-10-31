/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */

package org.voltdb.dtxn;

import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;

public class NonDeterministic_RO_SP extends VoltProcedure {

    public static final SQLStmt sql = new SQLStmt("select * from kv where key = ?");

    public long run(long key) {
        return VoltDB.instance().getHostMessenger().getHostId();
    }

}
