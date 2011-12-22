/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;

/**
 * Stub class that provides a gateway to the InvocationBufferServer when
 * WAN-based DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway {

    /**
     * Load the full subclass if it should, otherwise load the
     * noop stub.
     * @param partitionId partition id
     * @param overflowDir
     * @return Instance of PartitionDRGateway
     */
    public static PartitionDRGateway getInstance(int partitionId, boolean rejoiningAtStartup, File overflowDir) {

        PartitionDRGateway pdrg = null;

        VoltDBInterface vdb = VoltDB.instance();

        // if this is a primary cluster in a DR-enabled scenario
        //  try to load the real version of this class
        if (vdb.getReplicationRole() == ReplicationRole.MASTER) {
            try {
                Class<?> pdrgiClass = Class.forName("org.voltdb.dr.PartitionDRGatewayImpl");
                Object obj = pdrgiClass.newInstance();
                pdrg = (PartitionDRGateway) obj;

            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(
                        "Configured as master cluster but unable to load DR code",
                        false, null);
            }
        }

        // create a stub instance
        if (pdrg == null) {
            pdrg = new PartitionDRGateway();
        }

        // init the instance and return
        try {
            pdrg.init(partitionId, rejoiningAtStartup, overflowDir);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
        }
        return pdrg;
    }

    // empty methods for community edition
    protected void init(int partitionId, boolean rejoiningAtStartup, File overflowDir) throws IOException {}
    public void onSuccessfulProcedureCall(long txnId, StoredProcedureInvocation spi, ClientResponseImpl response) {}
    public void tick(long txnId) {}
    public void shutdown() {}

}
