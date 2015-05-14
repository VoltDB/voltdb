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

package org.voltdb.importer;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 *
 * @author akhanzode
 */

public interface ImportContext {

    public enum Format {
        CSV
    }
    /**
     * This is called to configure properties. Just save or configure anything you want here.
     * The readyForData() will be called to actually start processing the data.
     * @param p properties specified in the deployment.
     */
    public void configure(Properties p);
    /**
     * Called when cluster is ready to ingest data.
     */
    public void readyForData();
    /**
     * Called when stopping the node so the importer will cleanup
     */
    public void stop();

    /**
     * Call this to get the ingested data passed to procedure.
     * @param ic Import Context invoking the procedure.
     * @param procName procedure to invoke.
     * @param fieldList parameters to the procedure.
     * @return true if successfully accepted the work.
     */
    public boolean callProcedure(ImportContext ic, String procName, Object... fieldList);

    /**
     * Convert passed string to params for procedure based on format.
     * @param format
     * @param data
     * @return array ob objects to pass to callProcedure.
     */
    public List<Object> decodeParameters(Format format, String data);

    /**
     * Returns max time in nanoseconds a call to callProcedure waits in backpressure.
     * @return
     */
    public long getBackpressureTimeout();

    /**
     * This is the real handler dont need to call or extend anything
     * @param handler
     */
    public void setHandler(Object handler);

    /**
     * Give a friendly name for the importer.
     * @return
     */
    public String getName();

    /**
     * This is to tell importer system that this importer runs on all nodes. Default is true.
     * If this is false importer system will ask and allocate resources that you are responsible for
     * @return
     */
    public boolean isRunEveryWhere();

    /**
     * This is asked if runEveryWhere is false and importer knows what resources its can act on.
     * Based on the configuration of cluster and partitions present it will divide the work and hand down the importer
     * list of resources it should handle. The implementation should honor and act on those resources. In case if node
     * failure a update notification will be sent to start acting on more resources.
     * @return list of resources identifier that will be persisted in Volt ZK for watching.
     */
    public Set<String> getAllResponsibleResources();

    /**
     * This is called after importer system has allocated part of the responsible resources to the importer running on this node.
     * This may get invoked during node failuers which may distribute additional resources this importer is now responsible for.
     * @param allocated
     */
    public void setAllocatedResources(Set<String> allocated);

    /**
     * Return true if you are handling partitioned data and any data that does not belong to partitions on this node should be dropped.
     * @return
     */
    public boolean isPartitionedData();

    /**
     * log info message
     * @param message message to log to Volt server logging system.
     */
    public void info(String message);

    /**
     * log error message
     * @param message message to log to Volt server logging system.
     */
    public void error(String message);

}
