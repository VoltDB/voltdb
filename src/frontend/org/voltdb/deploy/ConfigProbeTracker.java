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

package org.voltdb.deploy;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.utils.EstTime;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

public class ConfigProbeTracker {

    public final static long CONFIG_PROBE_TIMEOUT = Long.getLong(
            "CONFIG_PROBE_TIMEOUT", TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES));

    protected final ConfigProbeResponse initialProbeResponse;
    protected final AtomicLong meshTimeout;
    protected final ConcurrentMap<UUID,Long> receivedProbes =new ConcurrentHashMap<>();
    protected final SettableFuture<Map<UUID,Long>> done = SettableFuture.create();
    protected final SettableFuture<LeaderAllClear> allClear = SettableFuture.create();
    protected final int nodeCount;

    public ConfigProbeTracker(
            int nodeCoount,
            UUID configHash,
            UUID meshHash,
            UUID startUuid,
            String internalInterface,
            boolean admin
    ) {
        checkArgument(nodeCoount > 0, "node count must be greater then 0");
        this.nodeCount = nodeCoount;
        this.initialProbeResponse =
                new ConfigProbeResponse(configHash, meshHash, startUuid, internalInterface, admin)
                ;
        this.meshTimeout = new AtomicLong(System.currentTimeMillis() + CONFIG_PROBE_TIMEOUT);
    }

    public ConfigProbeResponse getInitialConfigProbeResponse() {
        return initialProbeResponse;
    }

    public ConfigProbeResponse getConfigProbeResponse(UUID configHash) {
        return new ConfigProbeResponse(
                checkNotNull(configHash, "config hash is null"),
                initialProbeResponse.getMeshHash(),
                initialProbeResponse.getStartUuid(),
                initialProbeResponse.getInternalInterface(),
                initialProbeResponse.isAdmin()
                );
    }

    public void track(UUID id, long meshTo) {
        if (receivedProbes.putIfAbsent(id, meshTo) == null) {
            long currentTo = Long.MIN_VALUE;
            do {
                currentTo = meshTimeout.get();
            } while (currentTo < meshTo && !meshTimeout.compareAndSet(currentTo, meshTo));
            if (receivedProbes.size() >= nodeCount) {
                done.set(ImmutableMap.copyOf(receivedProbes));
            }
        }
    }

    public void receivedLeaderAllClear(LeaderAllClear allClear) {
        checkArgument(allClear != null, "leader all clear is null");
        this.allClear.set(allClear);
    }

    ListenableFuture<Map<UUID,Long>> getAllProbesCompleteFuture() {
        return done;
    }

    public long getMillisToTimeout() {
        long diff = meshTimeout.get() - EstTime.currentTimeMillis();
        return diff > 0L ? diff : 0L;
    }

    public long getMeshTimeout() {
        return meshTimeout.get();
    }

    protected <T> T waitFor(SettableFuture<T> fut) throws InterruptedException, TimeoutException {
        long waitMillis = getMillisToTimeout();
        do {
            try {
                return fut.get(getMillisToTimeout(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                // cannot happen as we don't set exceptions in the settable future
            } catch (TimeoutException e) {
                waitMillis = getMillisToTimeout();
                if (waitMillis == 0) {
                    throw e;
                }
            }
        } while (waitMillis > 0L);
        return null;
    }

    public Map<UUID,Long> waitForExpectedProbes() throws InterruptedException, TimeoutException {
        return waitFor(done);
    }

    public LeaderAllClear waitForLeaderAllClear() throws InterruptedException, TimeoutException {
        return waitFor(allClear);
    }

    public Map<UUID,Long> getTracked() {
        return ImmutableMap.copyOf(receivedProbes);
    }
}
