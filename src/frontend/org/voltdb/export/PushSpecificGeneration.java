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
import org.voltdb.compiler.deploymentfile.ExportType;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.CUSTOM;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.JDBC;
import static org.voltdb.compiler.deploymentfile.ServerExportEnum.KAFKA;
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
            ExportType exportType = dep.getExport();
            String exportClientClassName = "org.voltdb.exportclient.ExportToFileClient";
            switch (exportType.getTarget()) {
                case JDBC:
                    exportClientClassName = "org.voltdb.exportclient.JDBCExportClient";
                    break;
                case KAFKA:
                    exportClientClassName = "org.voltdb.exportclient.KafkaExportClient";
                    break;
                //Validate that we can load the class.
                case CUSTOM:
                    try {
                        CatalogUtil.class.getClassLoader().loadClass(exportType.getExportconnectorclass());
                        exportClientClassName = exportType.getExportconnectorclass();
                    } catch (ClassNotFoundException ex) {
                        System.out.println(
                                "Custom Export failed to configure, failed to load "
                                + " export plugin class: " + exportType.getExportconnectorclass()
                                + " Disabling export.");
                        return;
                    }
                    break;
            }
            StandaloneExportManager.initialize(0, args[1], exportClientClassName, /* TODO */ null);
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
