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

package org.voltdb.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * @author rdykiel
 *
 * Export Data Source Executor Factory.
 *
 * Singleton instance allocating {@link ListeningExecutorService} for {@link ExportDataSource}.
 *
 * Each {@code ExportDataSource} instance uses a mono-thread {@code ListeningExecutorService},
 * so that executing its runnables in that unique thread ensures that the instance is implictly
 * synchronized. However using a different thread per instance results in an explosion of threads
 * (one thread per exported stream multiplied by the number of partitions).
 *
 * The design of a {@code ExportDataSource} relies on all the runnables being executed by the same
 * thread, but nothing prevents sharing the thread among multiple instances: this would still ensure
 * that each instance is touched by a unique thread, while allowing reducing the overall number of
 * threads.
 *
 * This factory implements a thread sharing policy using a fixed number of threads. The default number
 * of threads is 1: all {@code ExportDataSource} instances sharing the same thread. This limit may be
 * increased with the MAX_EXPORT_THREADS system property. Further, the thread sharing policy ensures
 * that all {@code ExportDataSource} instances for a given partition share the same thread.
 *
 * Note on synchronization: we don't need a sophisticated synchronization mechanism here, as the
 * factory is just used by the {@code ExportDataSource} constructors.
 *
 * Finally, we need to maintain a reference count on the executors in order to handle shutdown.
 */
public class ExecutorFactory {


    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    public static final String MAX_EXPORT_THREADS = "MAX_EXPORT_THREADS";

    private Integer m_maxThreads;
    private int m_nextAlloc;

    private ArrayList<ListeningExecutorService> m_executors;
    private IdentityHashMap<ListeningExecutorService, Integer> m_refCounts;

    private Map<Integer, ListeningExecutorService> m_pid2execMap;

    /**
     * Singleton accessor
     *
     * @return the {@code EDSExecutorFactory } instance
     */
    public static final ExecutorFactory instance() {
        return m_execFactory;
    }

    /**
     * Package-private constructor for JUnit tests
     */
    ExecutorFactory() {
    }

    /**
     * @return true if initialized
     */
    private boolean isInitialized() {
        return m_maxThreads != null;
    }

    /**
     * Lazy initialization on first request for an executor
     */
    private void initialize() {

        if (isInitialized()) {
            return;
        }

        // Note - this theoretically could throw on permissions
        m_maxThreads = getMaxThreads();
        if (m_maxThreads < 1) {
            exportLog.warn("Parameter \"" + MAX_EXPORT_THREADS
                    + "\" should have a positive value, forcing to default value of 1");
            m_maxThreads = 1;
        }
        else {
            int localSitesCount = getLocalSitesCount();
            if (localSitesCount == 0) {
                // FIXME: can this happen?
                exportLog.warn("Parameter \"" + MAX_EXPORT_THREADS
                        + "\" cannot be checked, forcing to default value of 1");
                m_maxThreads = 1;
            }
            else if (m_maxThreads > localSitesCount) {
                exportLog.warn("Parameter \"" + MAX_EXPORT_THREADS
                        + "\" exceeds local sites count, forcing to default value of 1");
                m_maxThreads = 1;
            }
        }
        trace("Export Data Sources running with %d executor threads", m_maxThreads);

        m_nextAlloc = 0;
        m_executors = new ArrayList<>(m_maxThreads);
        m_refCounts = new IdentityHashMap<>();
        m_pid2execMap = new HashMap<>();
    }

    /**
     * @return max threads, package private for JUnit tests
     */
    Integer getMaxThreads() {
        return Integer.getInteger(MAX_EXPORT_THREADS, 1);
    }

    /**
     * @return local sites count, or 0 if undefined, package private for JUnit tests
     */
    int getLocalSitesCount() {
        int locSitesCount = 0;
        // JUnit environment may give inconsistent answers
        VoltDBInterface volt = VoltDB.instance();
        if (volt != null &&  volt.getCatalogContext() != null
                && volt.getCatalogContext().getNodeSettings() != null) {
                    locSitesCount = volt.getCatalogContext().getNodeSettings().getLocalSitesCount();
                }
        return locSitesCount;
    }

    /**
     * Get an executor for an export data source
     *
     * @param eds {@link ExportDataSource} instance
     * @return {@link ListeningExecutorService} allocated
     */
    public synchronized ListeningExecutorService getExecutor(ExportDataSource eds) {

        initialize();
        return allocate(eds);
    }

    /**
     * Free an executor used by an export data source
     *
     * @param eds {@link ExportDataSource} instance
     */
    public synchronized void freeExecutor(ExportDataSource eds) {

        if (!isInitialized()) {
            // Paranoid check for maintenance
            throw new IllegalStateException("Export Data Source for table: " + eds.getTableName()
            + ", partition: " + eds.getPartitionId() + " frees uninitialized executor");
        }

        if (!m_pid2execMap.keySet().contains(eds.getPartitionId())) {
            // Paranoid check for maintenance
            throw new IllegalStateException("Export Data Source for table: " + eds.getTableName()
                    + ", partition: " + eds.getPartitionId() + " frees unallocated executor");
        }
        release(eds);
    }

    /**
     * Allocate executor for new export data source
     *
     * @param eds {@link ExportDataSource} instance, verified new
     * @return {@link ListeningExecutorService} allocated
     */
    private ListeningExecutorService allocate(ExportDataSource eds) {

        ListeningExecutorService les = m_pid2execMap.get(eds.getPartitionId());
        if (les != null) {
            // Existing executor for this partition
            int refCount = m_refCounts.get(les).intValue() + 1;
            m_refCounts.put(les, refCount);

            trace("Allocated existing executor %d, for partition %d",
                    les.hashCode(), eds.getPartitionId());
        }
        else if (m_executors.size() < m_maxThreads) {
            // Create new executor for partition
            les = CoreUtils.getListeningExecutorService("ExportDataSource executor", 1);
            m_executors.add(les);
            m_refCounts.put(les, 1);
            m_pid2execMap.put(eds.getPartitionId(), les);

            trace("Allocated new executor %d, for partition %d, %d executors running",
                    les.hashCode(), eds.getPartitionId(), m_executors.size());
        }
        else {
            // Re-use existing executor for this partition
            if (m_nextAlloc >= m_executors.size()) {
                m_nextAlloc = 0;
            }
            les = m_executors.get(m_nextAlloc);
            int refCount = m_refCounts.get(les).intValue() + 1;
            m_refCounts.put(les, refCount);
            m_pid2execMap.put(eds.getPartitionId(), les);

            trace("Allocated executor %d, index %d, for partition %d",
                    les.hashCode(), m_nextAlloc, eds.getPartitionId());
            m_nextAlloc += 1;
        }
        return les;
    }

    /**
     * Release executor for export data source
     *
     * @param eds {@link ExportDataSource} instance, verified mapped in factory
     */
    private void release(ExportDataSource eds) {

        ListeningExecutorService les = m_pid2execMap.get(eds.getPartitionId());
        int refCount = m_refCounts.get(les).intValue() - 1;
        if (refCount < 0) {
            throw new IllegalStateException("Invalid refCount for table: " + eds.getTableName()
                        + "partition: " + eds.getPartitionId());
        }

        if (refCount == 0) {
            m_executors.remove(les);
            m_refCounts.remove(les);
            m_pid2execMap.remove(eds.getPartitionId());

            if (m_nextAlloc > m_executors.size()) {
                m_nextAlloc = 0;
            }
            les.shutdown();
            trace("Shutdown executor %d", les.hashCode());
        }
        else {
            m_refCounts.put(les, refCount);
            trace("Defer shutdown executor %d, refCount %d", les.hashCode(), refCount);
        }
    }

    private void trace(String msg) {
        trace(msg, null);
    }

    private void trace(String format, Object... arguments) {
        if (exportLog.isTraceEnabled()) {
            if (arguments != null && arguments.length > 0) {
                exportLog.trace(String.format(format, arguments));
            } else {
                exportLog.trace(format);
            }
        }
    }

    private static final ExecutorFactory m_execFactory = new ExecutorFactory();
}
