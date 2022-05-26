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
package org.voltdb.stats;

import com.google.common.util.concurrent.Futures;
import com.sun.management.UnixOperatingSystemMXBean;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FileDescriptorsTracker {

    private static final long UPDATE_INTERVAL_MINUTES = 10;

    private static final int RECOMMENDED_MINIMUM_FILE_DESCRIPTOR_LIMIT = 1024 * 10;
    private static final int OPEN_DESCRIPTORS_LEFT_WARN_THRESHOLD = 300;

    protected static VoltLogger logger = new VoltLogger("HOST");

    private final AtomicInteger m_openFileDescriptorCount = new AtomicInteger();
    private final AtomicInteger m_openFileDescriptorLimit = new AtomicInteger();

    private final UnixOperatingSystemMXBean m_unixOperatingSystemMXBean;
    private Future<?> m_updaterFuture = Futures.immediateCancelledFuture();

    public FileDescriptorsTracker(UnixOperatingSystemMXBean unixOperatingSystemMXBean) {
        m_unixOperatingSystemMXBean = unixOperatingSystemMXBean;
        update();
    }

    public FileDescriptorsTracker() {
        this((UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean());
    }

    public void start() {
        m_updaterFuture = VoltDB.instance().scheduleWork(
                this::update,
                0,
                UPDATE_INTERVAL_MINUTES,
                TimeUnit.MINUTES
        );
    }

    public void update() {
        int openFileDescriptorCount = (int) m_unixOperatingSystemMXBean.getOpenFileDescriptorCount();
        int maxFileDescriptorCount = (int) m_unixOperatingSystemMXBean.getMaxFileDescriptorCount();

        warnIfTooManyOpenFileDescriptors(openFileDescriptorCount, maxFileDescriptorCount);
        warnIfDescriptorLimitTooLow(maxFileDescriptorCount);

        m_openFileDescriptorCount.set(openFileDescriptorCount);
        m_openFileDescriptorLimit.set(maxFileDescriptorCount);
    }

    public void shutdown() {
        m_updaterFuture.cancel(true);
    }

    public int getOpenFileDescriptorCount() {
        return m_openFileDescriptorCount.get();
    }

    public int getOpenFileDescriptorLimit() {
        return m_openFileDescriptorLimit.get();
    }

    private void warnIfDescriptorLimitTooLow(int maxFileDescriptorCount) {
        if (maxFileDescriptorCount < RECOMMENDED_MINIMUM_FILE_DESCRIPTOR_LIMIT) {
            logger.warnFmt(
                    "File descriptor limit is low. Current: %d, recommended at least: %d",
                    maxFileDescriptorCount,
                    RECOMMENDED_MINIMUM_FILE_DESCRIPTOR_LIMIT
            );
        }
    }

    private void warnIfTooManyOpenFileDescriptors(int openFileDescriptorCount, int maxFileDescriptorCount) {
        int fileDescriptorsLeft = maxFileDescriptorCount - openFileDescriptorCount;
        if (fileDescriptorsLeft < OPEN_DESCRIPTORS_LEFT_WARN_THRESHOLD) {
            logger.warnFmt(
                    "Number of used file descriptors (%d) is approaching limit (%d)",
                    openFileDescriptorCount,
                    maxFileDescriptorCount
            );
        }
    }
}
