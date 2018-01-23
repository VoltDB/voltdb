/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rules.convert;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.VoltDBPartitioning;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;
import org.voltdb.calciteadapter.rel.VoltDBSend;

public class VoltDBScanRule extends ConverterRule {

        public static final VoltDBScanRule INSTANCE = new VoltDBScanRule();

        VoltDBScanRule() {
            super(
                    AbstractVoltDBTableScan.class,
                    Convention.NONE,
                    VoltDBConvention.INSTANCE,
                    "VoltDBScanRule");
        }

        @Override public RelNode convert(RelNode rel) {
            AbstractVoltDBTableScan scan = (AbstractVoltDBTableScan) rel;
            final RelTraitSet traitSet = scan.getTraitSet().replace(VoltDBConvention.INSTANCE);

            RelNode newScan = AbstractVoltDBTableScan.copy(scan, traitSet);
            if (scan.getVoltDBTable().getCatTable().getIsreplicated()) {
                return newScan;
            } else {
                VoltDBPartitioning partitioning = new VoltDBPartitioning(scan.getVoltDBTable());
                VoltDBSend send = VoltDBSend.create(
                        scan.getCluster(),
                        traitSet,
                        newScan,
                        partitioning);
                return send;
            }
      }
}