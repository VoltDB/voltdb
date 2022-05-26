/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.iv2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.client.Priority;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.PriorityPolicyType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.utils.AdaptivePriorityQueue;
import org.voltdb.utils.BlockingAdaptivePriorityQueue;

/**
 * A class managing the priority policy on a running system.
 * <p>
 * The range of priorities allowed is currently determined in source code
 * (see org.voltdb.client.Priority).
 * <p>
 * The policy defines the types of queues used by the SP and MP sites based
 * on the attributes of the {@link PriorityPolicyType} object from the deployment.
 */
public class PriorityPolicy {
    // Defined as volatile for weak synchronization
    private static volatile PriorityPolicyType s_activePolicy = null;
    static final VoltLogger hostLog = new VoltLogger("HOST");

    // Backward compatibility: old snapshot priority as a delay factor, will be deprecated
    private static volatile int s_snapshotDelayFactor;

    /**
     * Get a queue for SP tasks
     * <p>
     * Note: package-private
     *
     * @return the {@link BlockingQueue} of {@link SiteTasker} objects used by the SP site.
     */
    static BlockingQueue<SiteTasker> getSpTaskQueue() {
        PriorityPolicyType pp = getActivePolicy();
        if (!pp.isEnabled()) {
            return new LinkedTransferQueue<SiteTasker>();
        }

        int timeout = pp.getMaxwait();
        int batchSize = pp.getBatchsize();
        AdaptivePriorityQueue.OrderingPolicy policy = timeout > 0 ?
                AdaptivePriorityQueue.OrderingPolicy.MAX_WAIT_POLICY
                : AdaptivePriorityQueue.OrderingPolicy.USER_DEFINED_POLICY;

        return new BlockingAdaptivePriorityQueue<SiteTasker>(
                policy, timeout,
                Priority.SYSTEM_PRIORITY, batchSize);
    }

    /**
     * Get a queue for MP tasks
     * <p>
     * Note: package-private
     *
     * @return the {@link BlockingQueue} of {@link MpInitiatorMailbox.MpiTask} objects used by the MP site.
     */
    static BlockingQueue<MpInitiatorMailbox.MpiTask> getMpTaskQueue() {
        PriorityPolicyType pp = getActivePolicy();
        if (!pp.isEnabled()) {
            return new LinkedBlockingQueue<MpInitiatorMailbox.MpiTask>();
        }

        int timeout = pp.getMaxwait();
        int batchSize = pp.getBatchsize();
        AdaptivePriorityQueue.OrderingPolicy policy = timeout > 0 ?
                AdaptivePriorityQueue.OrderingPolicy.MAX_WAIT_POLICY
                : AdaptivePriorityQueue.OrderingPolicy.USER_DEFINED_POLICY;

        return new BlockingAdaptivePriorityQueue<MpInitiatorMailbox.MpiTask>(
                policy, timeout,
                Priority.SYSTEM_PRIORITY, batchSize);
    }

    /**
     * @return {@code true} if priorities are enabled.
     */
    static public boolean isEnabled() {
        PriorityPolicyType pp = getActivePolicy();
        return pp.isEnabled();
    }

    /**
     * Get the highest priority (not including system
     * priority, which might be higher). Currently fixed.
     *
     * @return highest priority
     */
    static public int getHighestPriority() {
        return Priority.HIGHEST_PRIORITY;
    }

     /**
     * Get the lowest priority, currently fixed.
     *
     * @return lowest priority
     */
    static public int getLowestPriority() {
        return Priority.LOWEST_PRIORITY;
    }

    /**
     * @return default priority
     */
    static public int getDefaultPriority() {
        return Priority.DEFAULT_PRIORITY;
    }

    /**
     * Get the DR priority
     *
     * @return DR priority, or {@link Priority.SYSTEM_PRIORITY} if priorities are disabled
     */
    static public int getDRPriority() {
        PriorityPolicyType pp = getActivePolicy();
        return pp.isEnabled() ? pp.getDr().getPriority() : Priority.SYSTEM_PRIORITY;
    }

    /**
     * Get the snapshot priority
     *
     * @return snapshot priority, or {@link Priority.SYSTEM_PRIORITY} if priorities are disabled
     */
    static public int getSnapshotPriority() {
        PriorityPolicyType pp = getActivePolicy();
        return pp.isEnabled() ? pp.getSnapshot().getPriority() : Priority.SYSTEM_PRIORITY;
    }

    /**
     * Return the old snapshot priority
     */
    static public int getSnapshotDelayFactor() {
        getActivePolicy();
        return s_snapshotDelayFactor;
    }

    /**
     * Clip a priority to be within the supported range.
     *
     * @param priority input priority
     * @return input priority or priority clipped to supported range.
     */
    static public int clipPriority(int priority) {
        int highestPriority = getHighestPriority();
        int lowestPriority = getLowestPriority();
        if (priority < highestPriority) {
            priority = highestPriority;
        } else if (priority > lowestPriority) {
            priority = lowestPriority;
        }
        return priority;
    }

    /**
     * Check if priority is valid
     *
     * @param priority
     * @return {@code true} if priority is valid
     */
    static public boolean isValidPriority(int priority) {
        int effective = clipPriority(priority);
        return effective == priority;
    }

    /**
     * Lazily cache the active transaction policy and return it
     *
     * @return {@link PriorityPolicyType}, the active transaction policy
     */
    public static PriorityPolicyType getActivePolicy() {
        if (s_activePolicy == null) {
            // Don't care if this is done concurrently by multiple threads
            CatalogContext cata = VoltDB.instance().getCatalogContext();
            DeploymentType depl = cata != null ? cata.getDeploymentSafely() : null;
            s_activePolicy = getPolicyFromDeployment(depl);

            // Initialize old snapshot priority (will be deprecated)
            s_snapshotDelayFactor = 6;
            if (depl != null) {
                SystemSettingsType sys = depl.getSystemsettings();
                SystemSettingsType.Snapshot snap = sys.getSnapshot();
                if (snap != null) {
                    s_snapshotDelayFactor = snap.getPriority();
                }
            }
        }
        return s_activePolicy;
    }

    /**
     * Get a transaction policy from a deployment
     *
     * @param depl {@link DeploymentType} or (@code null} if mocked environment
     * @return {@link PriorityPolicyType}, initialized with proper defaults
     */
    public static PriorityPolicyType getPolicyFromDeployment(DeploymentType depl) {
        PriorityPolicyType pp = null;
        SystemSettingsType sys = depl == null ? null : depl.getSystemsettings();
        pp = sys == null ? null : sys.getPriorities();
        if (pp == null) {
            pp = new PriorityPolicyType();
        }
        if (pp.isEnabled()) {
            // Set a fully initialized transaction with defaults
            PriorityPolicyType.Dr dr = pp.getDr();
            if (dr == null) {
                dr = new PriorityPolicyType.Dr();
                pp.setDr(dr);
            }
            PriorityPolicyType.Snapshot snap = pp.getSnapshot();
            if (snap == null) {
                snap = new PriorityPolicyType.Snapshot();
                pp.setSnapshot(snap);
            }
        }
        return pp;
    }

    /**
     * Returns a visible string, assumes object fully initialized
     *
     * @param pp
     * @return
     */
    public static String toString(PriorityPolicyType pp, String prefix) {
        return String.format(String.format(
                "%s: enabled %b, default priority %d, max wait time %d, batch size %d, dr priority %d, snapshot priority %d",
                prefix, pp.isEnabled(), Priority.DEFAULT_PRIORITY, pp.getMaxwait(), pp.getBatchsize(),
                pp.getDr().getPriority(), pp.getSnapshot().getPriority()));
    }
}
