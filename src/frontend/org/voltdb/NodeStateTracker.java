/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb;

import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.common.NodeState;

import com.google_voltpatches.common.base.Supplier;

/**
 * Class that aides in the tracking of a VoltDB node state.
 */
public class NodeStateTracker {

    private final AtomicReference<NodeState> nodeState = new AtomicReference<>(NodeState.INITIALIZING);

    public NodeStateTracker() {
    }

    static class NodeStateSupplier implements Supplier<NodeState> {
        private final AtomicReference<NodeState> ref;
        private NodeStateSupplier(AtomicReference<NodeState> ref) {
            this.ref = ref;
        }
        @Override
        public NodeState get() {
            return ref.get();
        }
    }

    public Supplier<NodeState> getNodeStateSupplier() {
        return new NodeStateSupplier(nodeState);
    }

    public boolean setNodeState(NodeState update) {
        return compareAndSetNodeState(nodeState.get(), update);
    }

    public boolean compareAndSetNodeState(NodeState expect, NodeState update) {
        return nodeState.compareAndSet(expect, update);
    }

    public NodeState getNodeState() {
        return nodeState.get();
    }
}
