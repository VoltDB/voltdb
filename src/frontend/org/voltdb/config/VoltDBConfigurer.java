/**
 * 
 */
package org.voltdb.config;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.RealVoltDB;
import org.voltdb.StartAction;
import org.voltdb.TheHashinator;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Initiator;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.rejoin.Iv2RejoinCoordinator;
import org.voltdb.rejoin.JoinCoordinator;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.MiscUtils;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
    
    
}
