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

import java.util.concurrent.CountDownLatch;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltdb.compiler.AsyncCompilerAgent;

/**
 * A class that instantiates an ExecutionSite and then waits for notification before
 * running the execution site. Would it be better if this extended Thread
 * so we don't have to have m_runners and m_siteThreads?
 */
public class ExecutionSiteRunner implements Runnable {

    // volatile because they are read by RealVoltDB
    volatile long m_siteId;
    volatile ExecutionSite m_siteObj;

    CountDownLatch m_siteIsLoaded = new CountDownLatch(1);
    CountDownLatch m_shouldStartRunning = new CountDownLatch(1);

    private final String m_serializedCatalog;
    private final boolean m_recovering;
    private final boolean m_replicationActive;
    private final long m_txnId;
    private final Mailbox m_mailbox;
    private final int m_configuredNumberOfPartitions;
    private final AsyncCompilerAgent m_agent;

    public ExecutionSiteRunner(
            Mailbox mailbox,
            final CatalogContext context,
            final String serializedCatalog,
            boolean recovering,
            boolean replicationActive,
            VoltLogger hostLog,
            int configuredNumberOfPartitions,
            AsyncCompilerAgent agent) {
        m_mailbox = mailbox;
        m_serializedCatalog = serializedCatalog;
        m_recovering = recovering;
        m_replicationActive = replicationActive;
        m_txnId = context.m_transactionId;
        m_configuredNumberOfPartitions = configuredNumberOfPartitions;
        m_agent = agent;
    }

    @Override
    public void run() {
        m_siteId = m_mailbox.getHSId();

        try {
            m_siteObj = new ExecutionSite(VoltDB.instance(),
                                          m_mailbox,
                                          m_serializedCatalog,
                                          null,
                                          m_recovering,
                                          m_replicationActive,
                                          m_txnId,
                                          m_configuredNumberOfPartitions,
                                          m_agent);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        // notify this site is created
        m_siteIsLoaded.countDown();

        // wait for the go-ahead signal
        try {
            m_shouldStartRunning.await();
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("EE Runnner thread interrupted while waiting to start.", true, e);
        }

        try
        {
            m_siteObj.run();
        }
        catch (OutOfMemoryError e)
        {
            // Even though OOM should be caught by the Throwable section below,
            // it sadly needs to be handled seperately. The goal here is to make
            // sure VoltDB crashes.
            e.printStackTrace();
            String errmsg = "ExecutionSite: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) + " ran out of Java memory. " +
                "This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, true, e);
        }
        catch (Throwable t)
        {
            String errmsg = "ExecutionSite: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) + " encountered an " +
                "unexpected error and will die, taking this VoltDB node down.";
            VoltDB.crashLocalVoltDB(errmsg, true, t);
        }
    }

}
