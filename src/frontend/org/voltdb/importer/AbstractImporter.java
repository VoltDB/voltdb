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
import java.util.function.Predicate;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.InternalConnectionContext;
import org.voltdb.client.ProcedureCallback;


/**
 * Abstract class that must be extended to create custom importers in VoltDB server.
 * The sequence of calls when the importer is started up is:
 * <ul>
 * <li> Find the importer factory in the OSGi bundle as a service </li>
 * <li> Validate and setup configuration using factory.createImporterConfigurations </li>
 * <li> Create an importer instance using factory.createImporter for every resource that must be
 *      run on this server </li>
 * <li> Start the importers by calling accept. Each of this will be called in its own thread.
 *      Importers should do their work in the implementation of accept.
 *      Importer implementations should do their work in a <code>while (shouldRun())</code> loop,
 *      which will make sure that that the importer will stop its work when the framework calls stop. </li>
 * </ul>
 *
 * <p>The framework stops the importer by calling <code>stopImporter</code>, which will stop the executor service and
 * call <code>stop</code> on the importer instance to close resources used by the specific importer.
 * <code>stop(resourceID)</code> will also be called on the importer instances when the resources are redistributed
 * because of addition/deletion of nodes to the cluster.
 */
public abstract class AbstractImporter
    implements InternalConnectionContext, ImporterLifecycle, ImporterLogger {

    private static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;

    private final VoltLogger m_logger;
    private int m_priority;
    private ImporterServerAdapter m_importServerAdapter;
    private volatile boolean m_stopping;
    private final Predicate<Integer> m_backPressurePredicate = (x) -> shouldRun();


    protected AbstractImporter() {
        m_logger = new VoltLogger(getName());
    }

    @Override
    public boolean hasTransaction() {
        return true;
    }

    public final void setPriority(int priority) {
        m_priority = priority;
    }

    @Override
    public int getPriority() {
        return m_priority;
    }

    /**
     * Passes in the server adapter that may be used by this importer to access the server,
     * like calling a procedure.
     *
     * @param adapter the server adapter that may be used by this to access the server.
     */
    public final void setImportServerAdapter(ImporterServerAdapter adapter) {
        m_importServerAdapter = adapter;
    }

    /**
     * This method indicates if the importer has been stopped or if it should continue running.
     * This should be checked by importer implementations regularly to determine if the importer
     * should continue its execution.
     *
     * @return returns true if the importer execution should continue; false otherwise
     */
    @Override
    public final boolean shouldRun()
    {
        return !m_stopping;
    }

    /**
     * This should be used importer implementations to execute a stored procedure.
     *
     * @param invocation Invocation object with procedure name and parameter information
     * @return returns true if the procedure execution went through successfully; false otherwise
     */
    protected boolean callProcedure(Invocation invocation)
    {
        return callProcedure(invocation, null);
    }

    /**
     * This should be used importer implementations to execute a stored procedure.
     *
     * @param invocation Invocation object with procedure name and parameter information
     * @param callback the callback that will receive procedure invocation status
     * @return returns true if the procedure execution went through successfully; false otherwise
     */
    public boolean callProcedure(Invocation invocation, ProcedureCallback callback)
    {
        try {
            boolean result = m_importServerAdapter.callProcedure(this,
                                                                 m_backPressurePredicate,
                                                                 callback, invocation.getProcedure(), invocation.getParams());
            reportStat(result, invocation.getProcedure());
            return result;
        } catch (Exception ex) {
            rateLimitedLog(Level.ERROR, ex, "%s: Error trying to import", getName());
            reportFailureStat(invocation.getProcedure());
            return false;
        }
    }

    /**
     * Called to stop the importer from processing more data.
     */
    public void stopImporter()
    {
        m_stopping = true;
        stop();
    }

    private void reportStat(boolean result, String procName) {
        if (result) {
            m_importServerAdapter.reportQueued(getName(), procName);
        } else {
            m_importServerAdapter.reportFailure(getName(), procName, false);
        }
    }

    public void reportInitializedStat(String procName){
        m_importServerAdapter.reportInitialized(getName(), procName);
    }

    private void reportFailureStat(String procName) {
        m_importServerAdapter.reportFailure(getName(), procName, false);
    }

    /**
     * This rate limited log must be used by the importers to log messages that may
     * happen frequently and must be rate limited.
     *
     * @param level the log level
     * @param cause cause exception, if there is one
     * @param format error message format
     * @param args arguments to format the error message
     */
    @Override
    public void rateLimitedLog(Level level, Throwable cause, String format, Object... args)
    {
        m_logger.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, level, cause, format, args);
    }

    @Override
    public boolean isDebugEnabled()
    {
        return m_logger.isDebugEnabled();
    }

    protected boolean isInfoEnabled()
    {
        return m_logger.isInfoEnabled();
    }

    protected boolean isTraceEnabled()
    {
        return m_logger.isTraceEnabled();
    }

    /**
     * Log a DEBUG level log message.
     *
     * @param msgFormat Format
     * @param t Throwable to log
     */
    @Override
    public void debug(Throwable t, String msgFormat, Object... args)
    {
        m_logger.debug(String.format(msgFormat, args), t);
    }

    /**
     * Log a ERROR level log message.
     *
     * @param msgFormat Format
     * @param t Throwable to log
     */
    @Override
    public void error(Throwable t, String msgFormat, Object... args)
    {
        m_logger.error(String.format(msgFormat, args), t);
    }

    /**
     * Log a INFO level log message.
     *
     * @param msgFormat Format
     * @param t Throwable to log
     */
    @Override
    public void info(Throwable t, String msgFormat, Object... args)
    {
        m_logger.info(String.format(msgFormat, args), t);
    }

    /**
     * Log a TRACE level log message.
     *
     * @param msgFormat Format
     * @param t Throwable to log
     */
    protected void trace(Throwable t, String msgFormat, Object... args)
    {
        m_logger.trace(String.format(msgFormat, args), t);
    }

    /**
     * Log a WARN level log message.
     *
     * @param msgFormat Format
     * @param t Throwable to log
     */
    @Override
    public void warn(Throwable t, String msgFormat, Object... args)
    {
        m_logger.warn(String.format(msgFormat, args), t);
    }

    /**
     * Returns the resource id for which this importer was started. There will be unique
     * resource id per importer for each importer type.
     *
     * @return the unique resource id that is used by this importer
     */
    public abstract URI getResourceID();


    public String getTaskThreadName() {
        return null;
    }

    /**
     * Implementation of this should contain the main importer work. This is typically
     * called in its own thread so that each importer can do its work in parallel in its
     * own thread. This implementation should check <code>shouldRun()</code> to check if
     * importer work should be stopped or continued.
     */
    protected abstract void accept();

    /**
     * This is called by the importer framework to stop the importer. Any importer
     * specific resources should be closed and released here.
     */
    @Override
    public abstract void stop();
}
