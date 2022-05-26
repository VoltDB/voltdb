/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltdb.common.NodeState;

import com.google_voltpatches.common.base.Supplier;

/**
 * Class that aids in the tracking of a VoltDB node state.
 * <p>
 * As well as the actual node state, this class supports
 * progress indication (represented as 'N things complete
 * out of a total of M') and a set-once flag that marks
 * the point at which VoltDB considers startup to be
 * complete.
 * <p>
 * These are both for the convenience of status reporing
 * via an API that calls StatusServlet. It is the case
 * that a node state of UP does not always indicate that
 * we are ready to service client requests; and we have
 * both a progress indicator and an explicit completion
 * flag because of the somewhat ad-hoc way we track our
 * progress. The flag has the final say as to whether
 * startup has completed.
 * <p>
 * Warning: since some code paths are used for startup
 * and during normal operations, the 'progress' data
 * may not reflect anything useful after the startup
 * completion flag is set.
 */
public class NodeStateTracker {

    private static final VoltLogger logger = new VoltLogger("NODESTATE");

    private final AtomicReference<NodeState> nodeState =
        new AtomicReference<>(NodeState.INITIALIZING);

    private final AtomicBoolean startupComplete =
        new AtomicBoolean(false);

    private int complete, total; // progress indicators

    public NodeState set(NodeState newState) {
        NodeState prevState = nodeState.getAndSet(newState);
        logger.info(String.format("State change, %s => %s", prevState, newState));
        return prevState;
    }

    public NodeState get() {
        return nodeState.get();
    }

    public Supplier<NodeState> getSupplier() {
        return nodeState::get;
    }

    public void reportProgress(int complete, int total) {
        int prevCmp, prevTot;
        synchronized (this) {
            prevCmp = this.complete;
            prevTot = this.total;
            this.complete = complete;
            this.total = total;
        }
        String warn = "";
        if (complete != 0 && (complete < prevCmp || total < prevTot)) {
            warn = " [retrograde]";
        }
        logger.info(String.format("Progress, %d/%d => %d/%d%s",
                                  prevCmp, prevTot, complete, total,
                                  warn));
    }

    public int[] getProgress() {
        int[] out = new int[2];
        synchronized (this) {
            out[0] = this.complete;
            out[1] = this.total;
        }
        return out;
    }

    public void setStartupComplete() {
        boolean prev = startupComplete.getAndSet(true);
        if (!prev) {
            logger.info("Server is now fully started");
        }
    }

    public boolean getStartupComplete() {
        return startupComplete.get();
    }
}
