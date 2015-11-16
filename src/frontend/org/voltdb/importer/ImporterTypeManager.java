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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;


/**
 * TODO:
 */
public class ImporterTypeManager implements ChannelChangeCallback
{
    private final VoltLogger m_logger;
    private final AbstractImporterFactory m_factory;
    private ExecutorService m_executorService;
    private Map<URI, ImporterConfig> m_configs = new HashMap<>();
    private Map<URI, AbstractImporter> m_importers = new HashMap<>();
    private volatile boolean m_stopping;

    public ImporterTypeManager(AbstractImporterFactory factory)
    {
        m_factory = factory;
        m_logger = new VoltLogger("ImporterTypeManager");
    }


    /**
     * This will be called for every importer configuration section.
     *
     * @param props Properties defined in a configuration section for this importer
     */
    public final void configure(Properties props)
    {
        m_configs.putAll(m_factory.createImporterConfigurations(props));
    }

    /**
     * This method is used by the framework to indicate to the importer that it may start its work.
     * This implementation starts the required number of threads based on the number of resources
     * configured for this importer.
     * <p>For importers that must be run on every node, this will also call
     * <code>accept(resourceID)</code> for all the available resources in its own thread.
     * For importers that should not be run on every node, this will register itself with the
     * resource distributer to be notified of the resources that this should use.
     *
     * @param distributer ChannelDistributer that is responsible for allocating resources to nodes
     */
    public final void readyForData(ChannelDistributer distributer)
    {
        if (m_stopping) return;

        if (m_executorService != null) { // Should be caused by coding error. Generic RuntimeException is OK
            throw new RuntimeException("Importer has already been started and is running");
        }

        m_executorService = Executors.newFixedThreadPool(m_configs.size(),
                getThreadFactory(m_factory.getTypeName(), ImportHandlerProxy.MEDIUM_STACK_SIZE));

        if (m_factory.isImporterRunEveryWhere()) {
            for (final ImporterConfig config : m_configs.values()) {
                AbstractImporter importer = m_factory.createImporter(config);
                m_importers.put(importer.getResourceID(), importer);
                submitAccept(importer);
            }
        } else {
            distributer.registerCallback(m_factory.getTypeName(), this);
            distributer.registerChannels(m_factory.getTypeName(), m_configs.keySet());
        }
    }

    /**
     * Callback method used by resource distributer to allocate/deallocate resources.
     * Stop will be called for resources that are removed from assignment list for this node.
     * Accept with be called in its own execution thread for resources that are added for this node.
     */
    @Override
    public final void onChange(ImporterChannelAssignment assignment)
    {
        if (m_stopping) return;

        for (URI removed: assignment.getRemoved()) {
            try {
                AbstractImporter importer = m_importers.remove(removed);
                importer.stop();
            } catch(Exception e) {
                m_logger.warn(
                        String.format("Error calling stop on %s in importer %s", removed.toString(), m_factory.getTypeName()),
                        e);
            }
        }

        for (final URI added: assignment.getAdded()) {
            AbstractImporter importer = m_factory.createImporter(m_configs.get(added));
            m_importers.put(added, importer);
            submitAccept(importer);
        }
    }

    private void submitAccept(final AbstractImporter importer)
    {
        m_executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    importer.accept();
                } catch(Exception e) {
                    m_logger.error(
                        String.format("Error calling accept for importer %s", m_factory.getTypeName()),
                        e);
                }
            }
        });
    }

    @Override
    public void onClusterStateChange(VersionedOperationMode mode) {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug(m_factory.getTypeName() + ".onChange");
        }
    }

    /**
     * This is called by the importer framework to stop an importer. Once this is called
     * <code>shouldRun()</code> will return false. All resources for this importer will be unregistered
     * from the resource distributer.
     *
     * @param distributer the resource distributer from which this importer's resources must be unregistered.
     */
    public final void stop(ChannelDistributer distributer) {
        m_stopping = true;

        if (!m_factory.isImporterRunEveryWhere()) {
            distributer.registerChannels(m_factory.getTypeName(), new HashSet<URI>());
        }
        stopImporters();

        if (m_executorService != null) {
            m_executorService.shutdown();
            try {
                m_executorService.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException ex) {
                //Should never come here.
                m_logger.warn("Unexpected interrupted exception waiting for " + m_factory.getTypeName() + " to shutdown", ex);
            }
        }
    }

    private void stopImporters()
    {
        for (AbstractImporter importer : m_importers.values()) {
            try {
                importer.stop();
            } catch(Exception e) {
                m_logger.warn("Error trying to stop importer resource ID " + importer.getResourceID(), e);
            }
        }
        m_importers.clear();
    }

    /**
     * Returns the logger that should be used by this importer.
     *
     * @return logger that should be used by this importer.
     */
    protected final VoltLogger getLogger()
    {
        return m_logger;
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
}
