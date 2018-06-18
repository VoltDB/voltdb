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

package org.voltdb.calciteadapter.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

import com.google.common.collect.ImmutableList;

public class VoltDBDefaultRelMetadataProvider {

    private VoltDBDefaultRelMetadataProvider() {
    }

    public static final RelMetadataProvider INSTANCE = ChainedRelMetadataProvider.of(
            ImmutableList
            .of(VoltDBRelMdParallelism.SOURCE,
                DefaultRelMetadataProvider.INSTANCE));

    public static class MetaDataProviderModifier extends RelShuttleImpl {
        private final RelMetadataProvider metadataProvider;

        public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
          this.metadataProvider = metadataProvider;
        }

//        @Override
//        public RelNode visit(TableScan scan) {
//          scan.getCluster().setMetadataProvider(metadataProvider);
//          return super.visit(scan);
//        }
//
//        @Override
//        public RelNode visit(TableFunctionScan scan) {
//          scan.getCluster().setMetadataProvider(metadataProvider);
//          return super.visit(scan);
//        }
//
//        @Override
//        public RelNode visit(LogicalValues values) {
//          values.getCluster().setMetadataProvider(metadataProvider);
//          return super.visit(values);
//        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
          child.accept(this);
          parent.getCluster().setMetadataProvider(metadataProvider);
          return parent;
        }
      }

}
