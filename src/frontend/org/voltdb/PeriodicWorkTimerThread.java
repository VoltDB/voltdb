/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.voltdb.logging.VoltLogger;

/**
 * This thread fires a timer every five milliseconds
 * which ultimately fires the tick to each execution
 * site in the cluster.
 *
 */
public class PeriodicWorkTimerThread extends Thread {

    ArrayList<ClientInterface> m_clientInterfaces;
    StatsManager m_statsManager;
    private long m_lastTime;
    private static final VoltLogger log = new VoltLogger("HOST");

    public PeriodicWorkTimerThread(ArrayList<ClientInterface> clientInterfaces,
                                   StatsManager statsManager) {
        m_clientInterfaces = clientInterfaces;
        m_statsManager = statsManager;
        m_lastTime = System.currentTimeMillis();
    }

    @Override
    public void run() {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        Thread.currentThread().setName("PeriodicWork");

        LinkedBlockingDeque<Object> foo = new LinkedBlockingDeque<Object>();
        while(true) {
            try {
                //long beforeTime = System.nanoTime();
                try {
                    foo.poll(5, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    return;
                }
                for (ClientInterface ci : m_clientInterfaces) {
                    ci.processPeriodicWork();
                }

                // Ask the statistics manager to send out change notifications if
                // enough time has passed
                final long currentTime = System.currentTimeMillis();
                if (m_statsManager != null
                        && (currentTime - m_lastTime) >= StatsManager.POLL_INTERVAL) {
                    m_lastTime = currentTime;
                    m_statsManager.sendNotification();
                }

                //long duration = System.nanoTime() - beforeTime;
                //double millis = duration / 1000000.0;
                //System.out.printf("TICK %.2f\n", millis);
                //System.out.flush();
            }
            catch (Exception ex) {
                log.warn(ex.getMessage(), ex);
            }
        }
    }
}
