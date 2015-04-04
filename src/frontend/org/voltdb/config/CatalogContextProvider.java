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

import javax.annotation.PostConstruct;

import org.apache.zookeeper_voltpatches.KeeperException;
import javax.inject.Inject;

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

public class CatalogContextProvider {
    @Inject
    private DeploymentTypeProvider deplTypeProvider;

    private volatile CatalogContext cached;

    private static CatalogContextProvider INSTANCE;

    @PostConstruct
    void init() {
        INSTANCE = this;
    }

    public CatalogContext getCatalogContext() throws KeeperException, InterruptedException {
        if(cached == null) {
            synchronized (this) {
                if(cached == null) {
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

                    cached = new CatalogContext(
                                    TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId(), //txnid
                                    0, //timestamp
                                    catalog,
                                    new byte[] {},
                                    deploymentBytes,
                                    0);
                }
            }

        }
        return cached;
    }

    /**
     * FIXME:this method violates incapsulation
     */
    public static void setCachedCatalog(CatalogContext m_catalogContext) {
        INSTANCE.cached = m_catalogContext;
    }
}
