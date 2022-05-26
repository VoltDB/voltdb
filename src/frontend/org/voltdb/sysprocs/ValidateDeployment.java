/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.sysprocs;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.ByteArrayInputStream;

import static org.voltdb.VoltSystemProcedure.STATUS_FAILURE;
import static org.voltdb.VoltSystemProcedure.STATUS_OK;

/**
 * This procedure just validates xml structure of deployment to catch any syntax errors.
 * This is a SP procedure and should be quick it should not have any knowledge of catalog and any deep validation.
 */
public class ValidateDeployment extends VoltNTSystemProcedure {
    static VoltLogger HOST_LOG;

    public static String STATUS = "STATUS";
    public static String DETAILS = "DETAILS";

    private static Unmarshaller m_unmarshaller;
    static {
        HOST_LOG = new VoltLogger("HOST");
        try {
            m_unmarshaller = CatalogUtil.m_jc.createUnmarshaller();
            m_unmarshaller.setSchema(CatalogUtil.m_schema);
        } catch (Exception ex) {
            // This should never happen.
            HOST_LOG.warn("Failed to initialize deployment schema unmarshaller.", ex);
            m_unmarshaller = null;
        }
    }

    public VoltTable run(String deploymentString) {
        VoltTable vt = new VoltTable(
                new VoltTable.ColumnInfo[] { new VoltTable.ColumnInfo(STATUS, VoltType.BIGINT),
                new VoltTable.ColumnInfo(DETAILS, VoltType.STRING)
            });
        String error = null;
        try {
            DeploymentType deployment = null;
            if (m_unmarshaller != null) {
                ByteArrayInputStream byteIS;
                byteIS = new ByteArrayInputStream(deploymentString.getBytes(Constants.UTF8ENCODING));
                JAXBElement<DeploymentType> result =
                        (JAXBElement<DeploymentType>) m_unmarshaller.unmarshal(byteIS);
                deployment = result.getValue();
            }
            if (deployment == null) {
                error = "Failed to validate deployment.";
            }
        } catch (JAXBException e) {
            HOST_LOG.warn("Failed parse deployment.", e);
            if (e.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                error = e.getLinkedException().getMessage();
            } else {
                error = "Failed to validate deployment: " + e.getMessage();
            }
        }
        if (error == null) {
            vt.addRow(STATUS_OK, "");
        } else {
            vt.addRow(STATUS_FAILURE, error);
        }
        return vt;
    }

}
