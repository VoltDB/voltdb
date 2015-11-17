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

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.InternalConnectionContext;


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
    implements InternalConnectionContext {

    private static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;

    private final VoltLogger m_logger;
    private ImporterServerAdapter m_importServerAdapter;
    private volatile boolean m_stopping;
    private AtomicInteger m_backPressureCount = new AtomicInteger(0);

    protected AbstractImporter() {
        m_logger = new VoltLogger(getName());
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
    protected final boolean shouldRun()
    {
        return !m_stopping;
    }

    /**
     * Returns the logger that should be used by this importer.
     *
     * @return logger that should be used by this importer.
     */
    protected final VoltLogger getLogger()
    {
        return m_logger;
    }

    /**
     * This should be used importer implementations to execute a stored procedure.
     *
     * @param invocation Invocation object with procedure name and parameter information
     * @return returns true if the procedure execution went through successfully; false otherwise
     */
    protected final boolean callProcedure(Invocation invocation)
    {
        try {
            boolean result = m_importServerAdapter.callProcedure(this, invocation.getProcedure(), invocation.getParams());
            reportStat(result, invocation.getProcedure());
            applyBackPressureAsNeeded();
            return result;
        } catch (Exception ex) {
            rateLimitedLog(Level.ERROR, ex, "%s: Error trying to import", getName());
            reportFailureStat(invocation.getProcedure());
            return false;
        }
    }

    private void applyBackPressureAsNeeded()
    {
        int count = m_backPressureCount.get();
        if (count > 0) {
            try { // increase sleep time exponentially to a max of 128ms
                if (count > 7) {
                    Thread.sleep(128);
                } else {
                    Thread.sleep(1<<count);
                }
            } catch(InterruptedException e) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Sleep for back pressure interrupted", e);
                }
            }
        }
    }

    /**
     * Called by the internal framework code to indicate if back pressure must
     * be applied on the importer because the server is busy.
     */
    @Override
    public void setBackPressure(boolean hasBackPressure)
    {
        if (hasBackPressure) {
            m_backPressureCount.incrementAndGet();
        } else {
            m_backPressureCount.set(0);
        }
    }

    private void reportStat(boolean result, String procName) {
        if (result) {
            m_importServerAdapter.reportQueued(getName(), procName);
        } else {
            m_importServerAdapter.reportFailure(getName(), procName, false);
        }
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
    protected void rateLimitedLog(Level level, Throwable cause, String format, Object... args)
    {
        m_logger.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, level, cause, format, args);
    }

    /**
     * Returns the resource id for which this importer was started. There will be unique
     * resource id per importer for each importer type.
     *
     * @return the unique resource id that is used by this importer
     */
    public abstract URI getResourceID();

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
    protected abstract void stop();
}
