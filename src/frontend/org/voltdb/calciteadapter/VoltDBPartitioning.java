package org.voltdb.calciteadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexNode;

public class VoltDBPartitioning {

    private List<VoltDBTable> m_scans = new ArrayList<>();
    private List<RexNode> m_equivalenceExprs = new ArrayList<>();

    public VoltDBPartitioning() {}

    public VoltDBPartitioning(VoltDBTable scan) {
        addScan(scan);
    }

    public void addScan(VoltDBTable scan) {
        assert(scan != null);
        m_scans.add(scan);
    }

    public boolean isCompartible(VoltDBPartitioning other, RexNode equivalenceExpr) {
        return true;
    }

    public VoltDBPartitioning mergeWith(VoltDBPartitioning other, RexNode equivalenceExpr) {
        m_scans.addAll(other.m_scans);
        m_equivalenceExprs.addAll(other.m_equivalenceExprs);
        return this;
    }
}
