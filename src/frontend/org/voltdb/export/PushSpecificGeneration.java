/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
            StandaloneExportManager.initialize(args[1], exportClientClassName, dep.getExport().getConfiguration().get(0).getProperty());
            System.out.print("Please wait...");
            while (true) {
                System.out.print(".");
                Thread.yield();
                Thread.sleep(5000);
                if (StandaloneExportManager.shouldExit()) {
                    break;
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            StandaloneExportManager.instance().shutdown(null);
        }
    }

}
