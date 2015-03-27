/**
 * 
 */
package org.voltdb.config;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.utils.CatalogUtil;

/**
 * @author black
 *
 */
@Component
public class CatalogContextProvider {
	@Autowired
	private DeploymentTypeProvider deplTypeProvider;
	
	public CatalogContext getCatalogContext() throws KeeperException, InterruptedException {
        // create a dummy catalog to load deployment info into
        Catalog catalog = new Catalog();
        // Need these in the dummy catalog
        Cluster cluster = catalog.getClusters().add("cluster");
        @SuppressWarnings("unused")
        Database db = cluster.getDatabases().add("database");

        byte[] deploymentBytes = deplTypeProvider.getDeploymentBytes();
        DeploymentType deployment = deplTypeProvider.getDeploymentType(deploymentBytes);
        String result = CatalogUtil.compileDeployment(catalog, deployment, true);
        if (result != null) {
            // Any other non-enterprise deployment errors will be caught and handled here
            // (such as <= 0 host count)
            VoltDB.crashLocalVoltDB(result);
        }

        return new CatalogContext(
                        TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId(), //txnid
                        0, //timestamp
                        catalog,
                        new byte[] {},
                        deploymentBytes,
                        0);

	}
}
