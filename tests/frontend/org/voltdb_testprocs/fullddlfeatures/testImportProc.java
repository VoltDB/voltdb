package org.voltdb_testprocs.fullddlfeatures;

import org.voltdb.VoltProcedure;

public class testImportProc extends VoltProcedure {

    public long run()
    {
        NoMeaningClass nmc = new NoMeaningClass();
        return nmc.returnTen();
    }
}
