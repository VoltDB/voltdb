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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;


/**
 * This class is responsible for receiving notifications from the server and
 * starting or stopping the importer instances accordingly. There will be a
 * manager instance per importer type/bundle.
 */
public class ImporterLifeCycleManager implements ChannelChangeCallback
{
    private final static VoltLogger s_logger = new VoltLogger("ImporterTypeManager");
    public static final int MEDIUM_STACK_SIZE = 1024 * 512;

    private final AbstractImporterFactory m_factory;
    private ListeningExecutorService m_executorService;
    private ImmutableMap<URI, ImporterConfig> m_configs = ImmutableMap.of();
    private ImmutableMap<URI, AbstractImporter> m_importers;
    private volatile boolean m_stopping;

    public ImporterLifeCycleManager(AbstractImporterFactory factory)
    {
        m_factory = factory;
    }


    /**
     * This will be called for every importer configuration section for this importer type.
     *
     * @param props Properties defined in a configuration section for this importer
     */
    public final void configure(Properties props)
    {
        ImmutableMap.Builder<URI, ImporterConfig> builder = new ImmutableMap.Builder<URI, ImporterConfig>().putAll(m_configs);
        builder.putAll(m_factory.createImporterConfigurations(props));
        m_configs = builder.build();
    }

    /**
     * This method is used by the framework to indicate that the importers must be started now.
     * This implementation starts the required number of threads based on the number of resources
     * configured for this importer type.
     * <p>For importers that must be run on every site, this will also call
     * <code>accept()</code>.
     * For importers that must not be run on every site, this will register itself with the
     * resource distributer.
     *
     * @param distributer ChannelDistributer that is responsible for allocating resources to nodes.
     * This will be used only if the importer must not be run on every site.
     */
    public final void readyForData(ChannelDistributer distributer)
    {
        if (m_stopping) return;

        if (m_executorService != null) { // Should be caused by coding error. Generic RuntimeException is OK
            throw new RuntimeException("Importer has already been started and is running");
        }

        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                m_configs.size(),
                m_configs.size(),
                5_000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                getThreadFactory(m_factory.getTypeName(), MEDIUM_STACK_SIZE)
                );
        tpe.allowCoreThreadTimeOut(true);

        m_executorService = MoreExecutors.listeningDecorator(tpe);

        ImmutableMap.Builder<URI, AbstractImporter> builder = new ImmutableMap.Builder<>();
        if (m_factory.isImporterRunEveryWhere()) {
            for (final ImporterConfig config : m_configs.values()) {
                AbstractImporter importer = m_factory.createImporter(config);
                builder.put(importer.getResourceID(), importer);
            }
            m_importers = builder.build();
            startImporters(m_importers.values());
        } else {
            m_importers = ImmutableMap.of();
            distributer.registerCallback(m_factory.getTypeName(), this);
            distributer.registerChannels(m_factory.getTypeName(), m_configs.keySet());
        }
    }

    private void startImporters(Collection<AbstractImporter> importers)
    {
        for (AbstractImporter importer : importers) {
            submitAccept(importer);
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

        ImmutableMap.Builder<URI, AbstractImporter> builder = new ImmutableMap.Builder<>();
        builder.putAll(Maps.filterKeys(m_importers, notUriIn(assignment.getRemoved())));
        for (URI removed: assignment.getRemoved()) {
            try {
                AbstractImporter importer = m_importers.get(removed);
                importer.stop();
            } catch(Exception e) {
                s_logger.warn(
                        String.format("Error calling stop on %s in importer %s", removed.toString(), m_factory.getTypeName()),
                        e);
            }
        }

        List<AbstractImporter> newImporters = new ArrayList<>();
        for (final URI added: assignment.getAdded()) {
            AbstractImporter importer = m_factory.createImporter(m_configs.get(added));
            newImporters.add(importer);
            builder.put(added, importer);
        }

        m_importers = builder.build();
        startImporters(newImporters);
    }

    private final static Predicate<URI> notUriIn(final Set<URI> uris) {
        return new Predicate<URI>() {
            @Override
            final public boolean apply(URI uri) {
                return !uris.contains(uri);
            }
        };
    }

    private void submitAccept(final AbstractImporter importer)
    {
        m_executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    importer.accept();
                } catch(Exception e) {
                    s_logger.error(
                        String.format("Error calling accept for importer %s", m_factory.getTypeName()),
                        e);
                }
            }
        });
    }

    @Override
    public void onClusterStateChange(VersionedOperationMode mode)
    {
        if (s_logger.isDebugEnabled()) {
            s_logger.debug(m_factory.getTypeName() + ".onChange");
        }
    }

    /**
     * This is called by the importer framework to stop importers.
     * All resources for this importer will be unregistered
     * from the resource distributer.
     *
     * @param distributer the resource distributer from which this importer's resources must be unregistered.
     */
    public final void stop(ChannelDistributer distributer)
    {
        m_stopping = true;

        if (!m_factory.isImporterRunEveryWhere()) {
            distributer.registerChannels(m_factory.getTypeName(), Collections.<URI> emptySet());
        }
        stopImporters();

        if (m_executorService != null) {
            m_executorService.shutdown();
            try {
                m_executorService.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException ex) {
                //Should never come here.
                s_logger.warn("Unexpected interrupted exception waiting for " + m_factory.getTypeName() + " to shutdown", ex);
            }
        }
    }

    private void stopImporters()
    {
        for (AbstractImporter importer : m_importers.values()) {
            try {
                importer.stopImporter();
            } catch(Exception e) {
                s_logger.warn("Error trying to stop importer resource ID " + importer.getResourceID(), e);
            }
        }
        m_importers = null;
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
