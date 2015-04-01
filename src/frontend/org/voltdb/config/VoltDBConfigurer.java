/**
 * 
 */
package org.voltdb.config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.inject.Named;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotIOAgent;
import org.voltdb.VoltDB;
import org.voltdb.config.topo.DummyEJoinTopologyProvider;
import org.voltdb.config.topo.RejoinTopologyProvider;
import org.voltdb.config.topo.StartupTopologyProvider;
import org.voltdb.config.topo.TopologyProvider;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * @author black
 *
 */
@Configuration
@ComponentScan({"org.voltdb", "org.voltcore"})
public class VoltDBConfigurer {
    private final VoltLogger log = new VoltLogger("CONFIG");
    private final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    @Bean
    public RealVoltDB realVoltDB() {
        return new RealVoltDB();
    }
    
    @Bean
    public HostMessenger hostMessenger(org.voltdb.config.Configuration m_config) {
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

        return new org.voltcore.messaging.HostMessenger(hmconfig);
    }

    @Bean
    @Named("computation")
    public ListeningExecutorService computationExecutionService(org.voltdb.config.Configuration m_config) {
    	final int computationThreads = Math.max(2, CoreUtils.availableProcessors() / 4);
    	return CoreUtils.getListeningExecutorService(
                "Computation service thread",
                computationThreads, m_config.m_computationCoreBindings);
    }
    

    @Bean
    @Named("periodicVoltThread")
    public ScheduledThreadPoolExecutor periodicVoltThread(org.voltdb.config.Configuration m_config) {
    	return CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);
   }
    @Bean
    @Named("periodicPriorityVoltThread")
    public ScheduledThreadPoolExecutor periodicPriorityVoltThread(org.voltdb.config.Configuration m_config) {
    	return CoreUtils.getScheduledThreadPoolExecutor("Periodic Priority Work", 1, CoreUtils.SMALL_STACK_SIZE);
   }
    @Bean
    @Named("startActionWatcherES")
    public ListeningExecutorService startActionWatcherExecutionService(org.voltdb.config.Configuration m_config) {
    	return CoreUtils.getCachedSingleThreadExecutor("StartAction ZK Watcher", 15000);
    }
    
    
    @Bean
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
    
    @Bean
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
    
}
