package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

public class RenameCatalogJar extends VoltSystemProcedure {

    @Override
    public long[] getPlanFragmentIds() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    public VoltTable[] run() {
        return null;
    }

}
