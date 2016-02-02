/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.export;

import java.io.File;
import java.io.FileInputStream;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportConfigurationType;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.CUSTOM;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.ELASTICSEARCH;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.FILE;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.HTTP;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.JDBC;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.KAFKA;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.RABBITMQ;
import org.voltdb.utils.CatalogUtil;

public class PushSpecificGeneration {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // Arguments deployment, directory
        try {
            if (args.length != 2) {
                System.out.println("Usage: draingen deployment.xml dir-where-where-generations-are");
                System.exit(1);
            }
            DeploymentType dep = CatalogUtil.getDeployment(new FileInputStream(new File(args[0])));
            ExportConfigurationType exportConfiguration = dep.getExport().getConfiguration().get(0);
            String exportClientClassName = null;

            switch (exportConfiguration.getType()) {
                case FILE:
                    exportClientClassName = "org.voltdb.exportclient.ExportToFileClient";
                    break;
                case JDBC:
                    exportClientClassName = "org.voltdb.exportclient.JDBCExportClient";
                    break;
                case KAFKA:
                    exportClientClassName = "org.voltdb.exportclient.kafka.KafkaExportClient";
                    break;
                case RABBITMQ:
                    exportClientClassName = "org.voltdb.exportclient.RabbitMQExportClient";
                    break;
                case HTTP:
                    exportClientClassName = "org.voltdb.exportclient.HttpExportClient";
                    break;
                case ELASTICSEARCH:
                    exportClientClassName = "org.voltdb.exportclient.ElasticSearchHttpExportClient";
                    break;
                //Validate that we can load the class.
                case CUSTOM:
                    exportClientClassName = exportConfiguration.getExportconnectorclass();
                    if (exportConfiguration.isEnabled()) {
                        try {
                            CatalogUtil.class.getClassLoader().loadClass(exportClientClassName);
                        } catch (ClassNotFoundException ex) {
                            String msg
                                    = "Custom Export failed to configure, failed to load"
                                    + " export plugin class: " + exportConfiguration.getExportconnectorclass()
                                    + " Disabling export.";
                            throw new CatalogUtil.DeploymentCheckException(msg);
                        }
                    }
                    break;
            }
            StandaloneExportManager.initialize(0, args[1], exportClientClassName, dep.getExport().getConfiguration().get(0).getProperty());
            int maxPart = dep.getCluster().getSitesperhost();
            System.out.println("Please wait...|");
            while (true) {
                System.out.print(".");
                Thread.yield();
                Thread.sleep(1000);
                long sz = 0;
                for (int i = 0; i < maxPart; i++) {
                    sz += StandaloneExportManager.getQueuedExportBytes(i, "");
                }
                if (sz <= 0 && StandaloneExportManager.shouldExit()) {
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            StandaloneExportManager.instance().shutdown();
        }
    }

}
