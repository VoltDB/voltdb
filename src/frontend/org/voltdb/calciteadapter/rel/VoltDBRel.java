package org.voltdb.calciteadapter.rel;

import org.apache.calcite.rel.RelNode;
import org.voltdb.plannodes.AbstractPlanNode;

public interface VoltDBRel extends RelNode  {
    public AbstractPlanNode toPlanNode();
}
