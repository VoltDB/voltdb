/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.importer;

import java.net.URI;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.InternalConnectionContext;


/**
 * Abstract class that must be extended to create importers in VoltDB server.
 * If the importer is made available using an OSGi bundle, this class has BundleActivator start method implementation
 * to register itself as a service. The sequence of calls when the importer is started up is:
 * - Find the importer in the OSGi bundle as a service
 * - Get ImporterConfig object using importer.createImporterConfig
 * - Add configuration entries using importerConfig.addConfiguration
 * - Start the importer by calling readyForData. This starts up required number of executors for the
 *   resources used by this importer and calls readyForData for each resource in its own thread.
 *
 * <p>The importer is stopped by simply calling stop method, which will stop the executor service and
 * call stopImporter to stop required activities specific to the importer instance.
 *
 * <p>Importer implementations should do their work in a <code>while (shouldRun())</code> loop, which will
 * make sure that that the importer stops its work when stop is called.
 * TODO: Should we call interrupt as well on the importer instance.
 */
public abstract class AbstractImporter
    implements InternalConnectionContext, BundleActivator {

    private ExecutorService m_executorService;
    private final ImporterConfig m_config;
    private final VoltLogger m_logger;
    private ImporterServerAdapter m_importServerAdapter;
    private volatile boolean m_stopping;
    private AtomicInteger m_backPressureCount = new AtomicInteger(0);

    protected AbstractImporter() {
        m_logger = new VoltLogger(getName());
        m_config = createImporterConfig();
    }

    @Override
    public final void start(BundleContext context) throws Exception {
        context.registerService(this.getClass().getName(), this, null);
    }

    @Override
    public final void stop(BundleContext context) throws Exception {
        // Nothing to do for now.
    }

    public final void setImportServerAdapter(ImporterServerAdapter adapter) {
        m_importServerAdapter = adapter;
    }

    public final void configure(Properties props) {
        m_config.addConfiguration(props);
    }

    public final void readyForData() {
        if (m_executorService != null) { // Should be caused by coding error. Hence generic RuntimeException is OK
            throw new RuntimeException("Importer has already been started and is running");
        }

        Set<URI> resources = m_config.getAvailalbleResources();
        m_executorService = Executors.newFixedThreadPool(resources.size(),
                getThreadFactory(getName(), ImportHandlerProxy.MEDIUM_STACK_SIZE));
        for (final URI res : resources) {
            m_executorService.submit(new Runnable() {
                @Override
                public void run() {
                    accept(res);
                }
            });
        }
    }

    public final void stopImporter() {
        m_stopping = true;
        stop();
        if (m_executorService != null) {
            m_executorService.shutdown();
            try {
                m_executorService.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException ex) {
                //Should never come here.
                m_logger.warn("Unexpected interrupted exception waiting for " + getName() + " to shutdown", ex);
            }
        }
    }

    protected final boolean shouldRun() {
        return !m_stopping;
    }

    protected final VoltLogger getLogger() {
        return m_logger;
    }

    protected final boolean callProcedure(Invocation invocation) {
        try {
            boolean result = m_importServerAdapter.callProcedure(this, invocation.getProcedure(), invocation.getParams());
            reportStat(result, invocation.getProcedure());
            applyBackPressureAsNeeded();
            return result;
        } catch (Exception ex) {
            rateLimitedLog(Level.ERROR, ex, "%s: Error trying to import", getName());
            reportFailureStat(invocation.getProcedure());
            return false;
        }
    }

    private void applyBackPressureAsNeeded()
    {
        int count = m_backPressureCount.get();
        if (count > 0) {
            try { // increase sleep time exponentially to a max of 128ms
                if (count > 7) {
                    Thread.sleep(128);
                } else {
                    Thread.sleep(1<<count);
                }
            } catch(InterruptedException e) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Sleep for back pressure interrupted", e);
                }
            }
        }
    }

    @Override
    public void setBackPressure(boolean hasBackPressure)
    {
        if (hasBackPressure) {
            m_backPressureCount.incrementAndGet();
        } else {
            m_backPressureCount.set(0);
        }
    }

    private void reportStat(boolean result, String procName) {
        if (result) {
            m_importServerAdapter.reportQueued(getName(), procName);
        } else {
            m_importServerAdapter.reportFailure(getName(), procName, false);
        }
    }

    private void reportFailureStat(String procName) {
        m_importServerAdapter.reportFailure(getName(), procName, false);
    }

    private ThreadFactory getThreadFactory(final String groupName, final int stackSize) {
        final ThreadGroup group = new ThreadGroup(Thread.currentThread().getThreadGroup(), groupName);

        return new ThreadFactory() {
            private final AtomicLong m_createdThreadCount = new AtomicLong(0);

            @Override
            public synchronized Thread newThread(final Runnable r) {
                final String threadName = groupName + " - " + m_createdThreadCount.getAndIncrement();
                Thread t = new Thread(group, r, threadName, stackSize);
                t.setDaemon(true);
                return t;
            }
        };
    }

    protected void rateLimitedLog(Level level, Throwable cause, String format, Object... args) {
        //TODO: define suppress interval somewhere
        m_logger.rateLimitedLog(60, level, cause, format, args);
    }

    protected abstract ImporterConfig createImporterConfig();

    protected abstract void accept(URI resourceID);

    protected abstract void stop();
}
