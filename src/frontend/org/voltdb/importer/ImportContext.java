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

package org.voltdb.importer;

import java.net.URI;
import java.util.Properties;
import java.util.Set;

import org.voltdb.InternalConnectionContext;
import org.voltdb.client.ProcedureCallback;

/**
 *
 * @author akhanzode
 */

public interface ImportContext extends InternalConnectionContext {

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
     * Call this to see if you need to exit.
     * @return
     */
    public boolean canContinue();

    /**
     * Call this to get the ingested data passed to procedure.
     * @param invocation indicating what kind of data is passed in for this.
     * @return true if successfully accepted the work.
     */
    public boolean callProcedure(Invocation invocation);

    /**
     * Call this to get the ingested data passed to procedure.
     * @param cb callback for results.
     * @param invocation indicating what kind of data is passed in for this.
     * @return true if successfully accepted the work.
     */
    public boolean callProcedure(ProcedureCallback cb, Invocation invocation);

    /**
     * This is the real handler dont need to call or extend anything
     * @param handler
     * @throws java.lang.Exception
     */
    public void setHandler(Object handler) throws Exception;

    /**
     * Give a friendly name for the importer.
     * @return
     */
    @Override
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
     * TODO make sure to get correct naming.
     */
    public Set<URI> getAllResponsibleResources();

    /**
     * log info message
     * @param message message to log to Volt server logging system.
     */
    public void info(String message);

    /**
     * log error message
     * @param format message to log to Volt server logging system.
     * @param t throwable to be logged.
     * @param args arguments to formatter
     */
    public void error(Throwable t, String format, Object...args);

    /**
     * log error message
     * @param message message to log to Volt server logging system.
     */
    public void error(String message);

    /**
     * log error message
     * @param message message to log to Volt server logging system.
     * @param t cause of error
     */
    public void error(String message, Throwable t);

    /**
     * log warn message
     * @param message message to log to Volt server logging system.
     */
    public void warn(String message);

    public void warn(Throwable t, String format, Object...args);

    /**
     * log debug message
     * @param message message to log to Volt server logging system.
     */
    public void debug(String message);

    public boolean isDebugEnabled();
    public boolean isTraceEnabled();
    public boolean isInfoEnabled();

    public void trace(String message);
}
