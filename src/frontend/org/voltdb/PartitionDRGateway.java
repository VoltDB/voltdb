/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.voltdb.licensetool.LicenseApi;

/**
 * Stub class that provides a gateway to the InvocationBufferServer when
 * DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway {
    protected final boolean m_iv2Enabled;

    /**
     * Load the full subclass if it should, otherwise load the
     * noop stub.
     * @param partitionId partition id
     * @param overflowDir
     * @return Instance of PartitionDRGateway
     */
    public static PartitionDRGateway getInstance(int partitionId,
                                                 NodeDRGateway nodeGateway,
                                                 boolean iv2Enabled)
    {
        final VoltDBInterface vdb = VoltDB.instance();
        LicenseApi api = vdb.getLicenseApi();
        final boolean licensedToDR = api.isDrReplicationAllowed();

        // if this is a primary cluster in a DR-enabled scenario
        // try to load the real version of this class
        PartitionDRGateway pdrg = null;
        if (licensedToDR && nodeGateway != null) {
            pdrg = tryToLoadProVersion(iv2Enabled);
        }
        if (pdrg == null) {
            pdrg = new PartitionDRGateway(iv2Enabled);
        }

        // init the instance and return
        try {
            pdrg.init(partitionId, nodeGateway);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
        }
        return pdrg;
    }

    private static PartitionDRGateway tryToLoadProVersion(boolean iv2Enalbed)
    {
        try {
            Class<?> pdrgiClass = Class.forName("org.voltdb.dr.PartitionDRGatewayImpl");
            Constructor<?> constructor = pdrgiClass.getConstructor(boolean.class);
            Object obj = constructor.newInstance(iv2Enalbed);
            return (PartitionDRGateway) obj;
        } catch (Exception e) {
        }
        return null;
    }

    public PartitionDRGateway(boolean iv2Enabled)
    {
        m_iv2Enabled = iv2Enabled;
    }

    // empty methods for community edition
    protected void init(int partitionId,
                        NodeDRGateway gateway) throws IOException {}
    public void onSuccessfulProcedureCall(long txnId, long uniqueId, int hash,
                                          StoredProcedureInvocation spi,
                                          ClientResponseImpl response) {}
    public void onSuccessfulMPCall(long spHandle, long txnId, long uniqueId, int hash,
                                   StoredProcedureInvocation spi,
                                   ClientResponseImpl response) {}
    public void tick(long txnId) {}
}
