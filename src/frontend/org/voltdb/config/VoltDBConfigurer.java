/**
 * 
 */
package org.voltdb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.RealVoltDB;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.MiscUtils;

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
    
    
}
