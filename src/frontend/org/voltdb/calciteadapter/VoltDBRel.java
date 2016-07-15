package org.voltdb.calciteadapter;

import org.apache.calcite.rel.RelNode;
import org.voltdb.plannodes.AbstractPlanNode;

interface VoltDBRel extends RelNode  {
    public AbstractPlanNode toPlanNode();
}
