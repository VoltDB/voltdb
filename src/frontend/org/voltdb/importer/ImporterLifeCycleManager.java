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

package org.voltdb.importer;

import static com.google_voltpatches.common.base.Predicates.not;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltdb.importer.formatter.FormatterBuilder;

import com.google_voltpatches.common.base.Joiner;
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

    private final int m_priority;
    private final AbstractImporterFactory m_factory;
    private ListeningExecutorService m_executorService;
    private ImmutableMap<URI, ImporterConfig> m_configs = ImmutableMap.of();
    private AtomicReference<ImmutableMap<URI, AbstractImporter>> m_importers = new AtomicReference<>(ImmutableMap.<URI, AbstractImporter> of());
    private volatile boolean m_stopping;
    private final AtomicBoolean m_starting = new AtomicBoolean(false);
    // Safe to keep reference here as there is only and it is not susceptible to catalog changes
    private final ChannelDistributer m_distributer;
    private final String m_distributerDesignation;

    public ImporterLifeCycleManager(
            int priority,
            AbstractImporterFactory factory,
            final ChannelDistributer distributer,
            String clusterTag)
    {
        m_priority = priority;
        m_factory = factory;
        m_distributer = distributer;
        m_distributerDesignation = m_factory.getTypeName() + "_" + clusterTag;
    }


    /**
     * This will be called for every importer configuration section for this importer type.
     *
     * @param props Properties defined in a configuration section for this importer
     */
    public final void configure(Properties props, FormatterBuilder formatterBuilder)
    {
        Map<URI, ImporterConfig> configs = m_factory.createImporterConfigurations(props, formatterBuilder);
        m_configs = new ImmutableMap.Builder<URI, ImporterConfig>()
                .putAll(configs)
                .putAll(Maps.filterKeys(m_configs, not(in(configs.keySet()))))
                .build();
    }

    public final int getConfigsCount() {
        return m_configs.size();
    }

    /**
     * This method is used by the framework to indicate that the importers must be started now.
     * This implementation starts the required number of threads based on the number of resources
     * configured for this importer type.
     * <p>For importers that must be run on every site, this will also call
     * <code>accept()</code>.
     * For importers that must not be run on every site, this will register itself with the
     * resource distributer.
     */
    public final void readyForData()
    {
        m_starting.compareAndSet(false, true);

        if (m_stopping) return;

        if (m_executorService != null) { // Should be caused by coding error. Generic RuntimeException is OK
            throw new RuntimeException("Importer has already been started and is running");
        }

        if (m_configs.size()==0) {
            s_logger.info("No configured importers of " + m_factory.getTypeName() + " are ready to be started at this time");
            return;
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

        if (m_factory.isImporterRunEveryWhere()) {
            ImmutableMap.Builder<URI, AbstractImporter> builder = new ImmutableMap.Builder<>();
            for (final ImporterConfig config : m_configs.values()) {
                AbstractImporter importer = m_factory.createImporter(m_priority, config);
                builder.put(importer.getResourceID(), importer);
            }
            m_importers.set(builder.build());
            startImporters(m_importers.get().values());
        } else {
            m_importers.set(ImmutableMap.<URI, AbstractImporter> of());
            m_distributer.registerCallback(m_distributerDesignation, this);
            m_distributer.registerChannels(m_distributerDesignation, m_configs.keySet());
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
        if (m_stopping && !assignment.getAdded().isEmpty()) {
            String msg = "Received an a channel assignment when the importer is stopping: " + assignment;
            s_logger.warn(msg);
            throw new IllegalStateException(msg);
        }

        if (m_stopping) {
            return;
        }

        ImmutableMap<URI, AbstractImporter> oldReference = m_importers.get();
        Map<URI, AbstractImporter> importersMap = Maps.newHashMap();
        importersMap.putAll(oldReference);
        List<AbstractImporter> toStop = new ArrayList<>();
        List<String> missingRemovedURLs = new ArrayList<>();
        List<String> missingAddedURLs = new ArrayList<>();
        for (URI removed: assignment.getRemoved()) {
            importersMap.remove(removed);
            if (m_configs.containsKey(removed)) {
               AbstractImporter importer = oldReference.get(removed);
               if (importer != null) {
                    toStop.add(importer);
               }
            } else {
                missingRemovedURLs.add(removed.toString());
            }
        }

        List<AbstractImporter> newImporters = new ArrayList<>();
        for (final URI added: assignment.getAdded()) {
            if (m_configs.containsKey(added)) {
                //sanity check to avoid duplicated assignments
                if (importersMap.containsKey(added)) {
                    continue;
                }
                AbstractImporter importer = m_factory.createImporter(m_priority, m_configs.get(added));
                newImporters.add(importer);
                importersMap.put(added, importer);
            } else {
                missingAddedURLs.add(added.toString());
            }
        }

        if (!missingRemovedURLs.isEmpty() || !missingAddedURLs.isEmpty()) {
            s_logger.error("The source for Import has changed its configuration. Removed importer URL(s): (" +
                    Joiner.on(", ").join(missingRemovedURLs) + "), added importer URL(s): (" +
                    Joiner.on(", ").join(missingAddedURLs) + "). Pause and Resume the database to refresh the importer.");
        }

        ImmutableMap<URI, AbstractImporter> newReference = ImmutableMap.copyOf(importersMap);
        boolean success = m_importers.compareAndSet(oldReference, newReference);
        if (!m_stopping && success) { // Could fail if stop was called after we entered inside this method
            stopImporters(toStop);
            startImporters(newImporters);
        }
    }

    private void submitAccept(final AbstractImporter importer)
    {
        m_executorService.submit(() -> {
            try {
                final String thName = importer.getTaskThreadName();
                if (thName != null) {
                    Thread.currentThread().setName(thName);
                }
                importer.accept();
            } catch(Throwable e) {
                s_logger.error(
                        String.format("Error calling accept for importer %s", m_factory.getTypeName()),
                        e);
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
     */
    public final void stop()
    {
        m_stopping = true;

        ImmutableMap<URI, AbstractImporter> oldReference;
        boolean success = false;
        do { // onChange also could set m_importers. Use while loop to pick up latest ref
            oldReference = m_importers.get();
            success = m_importers.compareAndSet(oldReference, ImmutableMap.<URI, AbstractImporter> of());
        } while (!success);

        if (!m_starting.get()) return;

        stopImporters(oldReference.values());
        if (!m_factory.isImporterRunEveryWhere()) {
            m_distributer.registerChannels(m_distributerDesignation, Collections.<URI> emptySet());
            m_distributer.unregisterCallback(m_distributerDesignation);
        }

        if (m_executorService == null) {
            return;
        }

        //graceful shutdown to allow importers to properly process post shutdown tasks.
        m_executorService.shutdown();
        try {
            m_executorService.awaitTermination(60, TimeUnit.SECONDS);
            m_executorService = null;
        } catch (InterruptedException ex) {
            //Should never come here.
            s_logger.warn("Unexpected interrupted exception waiting for " + m_factory.getTypeName() + " to shutdown", ex);
        }
    }

    private void stopImporters(Collection<AbstractImporter> importers)
    {
        for (AbstractImporter importer : importers) {
            try {
                importer.stopImporter();
            } catch(Exception e) {
                s_logger.warn("Error trying to stop importer resource ID " + importer.getResourceID(), e);
            }
        }
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

    public final static <T> Predicate<T> in(final Set<T> set) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T m) {
                return set.contains(m);
            }
        };
    }

}
