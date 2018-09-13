/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.rel;

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
