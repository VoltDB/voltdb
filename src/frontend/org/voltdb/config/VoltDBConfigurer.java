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
package org.voltdb.config;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.OpsRegistrar;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.SnapshotIOAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.config.state.VoltStateManager;
import org.voltdb.config.topo.DummyEJoinTopologyProvider;
import org.voltdb.config.topo.PartitionsInformer;
import org.voltdb.config.topo.RejoinTopologyProvider;
import org.voltdb.config.topo.StartupTopologyProvider;
import org.voltdb.config.topo.TopologyProvider;
import org.voltdb.config.topo.TopologyProviderFactory;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.MiscUtils;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * @author black
 *
 */

public class VoltDBConfigurer extends AbstractModule {
    private final VoltLogger log = new VoltLogger("CONFIG");
    private final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    @Inject
    private Injector injector;

    @Provides @Inject @Singleton
    public HostMessenger hostMessenger(org.voltdb.config.Configuration m_config, Injector injector) {
        final String leaderAddress = m_config.m_leader;
        String hostname = MiscUtils.getHostnameFromHostnameColonPort(leaderAddress);
        int port = MiscUtils.getPortFromHostnameColonPort(leaderAddress, m_config.m_internalPort);

        org.voltcore.messaging.HostMessenger.Config hmconfig;

        hmconfig = new org.voltcore.messaging.HostMessenger.Config(hostname, port);
        hmconfig.internalPort = m_config.m_internalPort;
        if (m_config.m_internalPortInterface != null && m_config.m_internalPortInterface.trim().length() > 0) {
            hmconfig.internalInterface = m_config.m_internalPortInterface.trim();
        } else {
            hmconfig.internalInterface = m_config.m_internalInterface;
        }
        hmconfig.zkInterface = m_config.m_zkInterface;
        hmconfig.deadHostTimeout = m_config.m_deadHostTimeoutMS;
        hmconfig.factory = new VoltDbMessageFactory();
        hmconfig.coreBindIds = m_config.m_networkCoreBindings;

        HostMessenger hm = new org.voltcore.messaging.HostMessenger(hmconfig);
        injector.injectMembers(hm);
        return hm;
    }

    @Provides @Singleton
    @Named("computation")
    public ListeningExecutorService computationExecutionService(org.voltdb.config.Configuration m_config) {
        final int computationThreads = Math.max(2, CoreUtils.availableProcessors() / 4);
        return CoreUtils.getListeningExecutorService(
                "Computation service thread",
                computationThreads, m_config.m_computationCoreBindings);
    }


    @Provides @Singleton
    @Named("periodicVoltThread")
    public ScheduledThreadPoolExecutor periodicVoltThread(org.voltdb.config.Configuration m_config) {
        return CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);
   }
    @Provides @Singleton
    @Named("periodicPriorityVoltThread")
    public ScheduledThreadPoolExecutor periodicPriorityVoltThread(org.voltdb.config.Configuration m_config) {
        return CoreUtils.getScheduledThreadPoolExecutor("Periodic Priority Work", 1, CoreUtils.SMALL_STACK_SIZE);
   }
    @Provides @Singleton
    @Named("startActionWatcherES")
    public ListeningExecutorService startActionWatcherExecutionService(org.voltdb.config.Configuration m_config) {
        return CoreUtils.getCachedSingleThreadExecutor("StartAction ZK Watcher", 15000);
    }


    @Provides @Singleton
    public SnapshotIOAgent snapshotIOAgent (HostMessenger m_messenger) {
        Class<?> snapshotIOAgentClass = MiscUtils.loadProClass("org.voltdb.SnapshotIOAgentImpl", "Snapshot", true);
        if (snapshotIOAgentClass != null) {
            try {
                SnapshotIOAgent m_snapshotIOAgent = (SnapshotIOAgent) snapshotIOAgentClass.getConstructor(HostMessenger.class, long.class)
                        .newInstance(m_messenger, m_messenger.getHSIdForLocalSite(HostMessenger.SNAPSHOT_IO_AGENT_ID));
                m_messenger.createMailbox(m_snapshotIOAgent.getHSId(), m_snapshotIOAgent);
                return m_snapshotIOAgent;
            } catch (Exception e) {
                throw VoltDB.crashLocalVoltDB("Failed to instantiate snapshot IO agent", true, e);
            }
        } else {
            return new SnapshotIOAgent(null, 0) {
                @Override
                public void deliver(VoltMessage message) {
                }

                @Override
                public <T> ListenableFuture<T> submit(Callable<T> work) {
                    throw new UnsupportedOperationException("Snapshots are not supported");
                }

                @Override
                public void shutdown() throws InterruptedException {
                }
            };
        }

    }


    @Provides
    public List<TopologyProvider> topologyProvidersChain(
            DummyEJoinTopologyProvider joinProvider,
            RejoinTopologyProvider rejoinProvider,
            StartupTopologyProvider defaultProvider) {
        List<TopologyProvider> providers = new ArrayList<TopologyProvider>();
        providers.add(joinProvider);
        providers.add(rejoinProvider);
        providers.add(defaultProvider);
        return providers;
    }

    @Override
    protected void configure() {

        bind(DummyEJoinTopologyProvider.class).asEagerSingleton();
        bind(RejoinTopologyProvider.class).asEagerSingleton();
        bind(StartupTopologyProvider.class).asEagerSingleton();
        bind(TopologyProviderFactory.class).asEagerSingleton();

        bind(VoltStateManager.class).asEagerSingleton();
        bind(PartitionsInformer.class).asEagerSingleton();
        bind(CartographerProvider.class).asEagerSingleton();
        bind(CatalogContextProvider.class).asEagerSingleton();
        bind(ClientInterfaceProvider.class).asEagerSingleton();
        bind(DeploymentTypeProvider.class).asEagerSingleton();
        bind(OpsRegistrar.class).asEagerSingleton();
        bind(SnapshotCompletionMonitor.class).asEagerSingleton();
        bind(VoltDBInterface.class).to(RealVoltDB.class).asEagerSingleton();


        binder().bindListener(Matchers.any(), new TypeListener() {
            @Override
            public <I> void hear(final TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
                final Class<?> target = (Class<?>) typeLiteral.getType();
                if(HostMessenger.class.isAssignableFrom(target)) {
                    System.out.print("");
                }
                for(final Method m: target.getDeclaredMethods()) {
                    for(Annotation a:m.getAnnotations()) {
                        if(a instanceof PostConstruct) {
                            typeEncounter.register(new InjectionListener<I>() {
                                @Override
                                public void afterInjection(Object i) {
                                    try {
                                        m.setAccessible(true);
                                        m.invoke(i);
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error calling post-construct method " + target.getSimpleName() + "." + m.getName(), e);//TODO: specialized exception
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });
    }

}
