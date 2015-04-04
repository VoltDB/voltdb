/**
 * 
 */
package org.voltdb.config;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import javax.inject.Inject;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltDB;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

/**
 * @author black
 *
 */

public class DeploymentTypeProvider {
    private final VoltLogger hostLog = new VoltLogger("HOST");

    @Inject
    private Configuration m_config;
    
    @Inject
    private HostMessenger m_messenger;
    
    private volatile byte[] cachedBytes;
    
    public byte[] getDeploymentBytes() throws KeeperException, InterruptedException {
    	if(cachedBytes == null) {
    		synchronized (this) {
    			if(cachedBytes == null) {
			        ZooKeeper zk = m_messenger.getZK();
			        byte deploymentBytes[] = null;
			        try {
			            deploymentBytes = org.voltcore.utils.CoreUtils.urlToBytes(m_config.m_pathToDeployment);
			        } catch (Exception ex) {
			            //Let us get bytes from ZK
			        }

			        try {
			            if (deploymentBytes != null) {
			                CatalogUtil.writeCatalogToZK(zk,
			                        // Fill in innocuous values for non-deployment stuff
			                        0,
			                        0L,
			                        0L,
			                        new byte[] {},  // spin loop in Inits.LoadCatalog.run() needs
			                                        // this to be of zero length until we have a real catalog.
			                        deploymentBytes);
			                hostLog.info("URL of deployment: " + m_config.m_pathToDeployment);
			            } else {
			                CatalogAndIds catalogStuff = CatalogUtil.getCatalogFromZK(zk);
			                deploymentBytes = catalogStuff.deploymentBytes;
			            }
			        } catch (KeeperException.NodeExistsException e) {
			            CatalogAndIds catalogStuff = CatalogUtil.getCatalogFromZK(zk);
			            byte[] deploymentBytesTemp = catalogStuff.deploymentBytes;
			            if (deploymentBytesTemp != null) {
			                //Check hash if its a supplied deployment on command line.
			                //We will ignore the supplied or default deployment anyways.
			                if (deploymentBytes != null && !m_config.m_deploymentDefault) {
			                    byte[] deploymentHashHere =
			                        CatalogUtil.makeDeploymentHash(deploymentBytes);
			                    if (!(Arrays.equals(deploymentHashHere, catalogStuff.getDeploymentHash())))
			                    {
			                        hostLog.warn("The locally provided deployment configuration did not " +
			                                " match the configuration information found in the cluster.");
			                    } else {
			                        hostLog.info("Deployment configuration pulled from other cluster node.");
			                    }
			                }
			                //Use remote deployment obtained.
			                deploymentBytes = deploymentBytesTemp;
			            } else {
			                hostLog.error("Deployment file could not be loaded locally or remotely, "
			                        + "local supplied path: " + m_config.m_pathToDeployment);
			                deploymentBytes = null;
			            }
			        }
			        if (deploymentBytes == null) {
			            hostLog.error("Deployment could not be obtained from cluster node or locally");
			            VoltDB.crashLocalVoltDB("No such deployment file: "
			                    + m_config.m_pathToDeployment, false, null);
			        }
			        cachedBytes = deploymentBytes;
    			}
    		}
    	}
        return cachedBytes;
    }
    public DeploymentType getDeploymentType(byte[] deploymentBytes) throws KeeperException, InterruptedException {

        return
            CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
    }

}
