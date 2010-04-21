/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

/**
 * This thread fires a timer every five milliseconds
 * which ultimately fires the tick to each execution
 * site in the cluster.
 *
 */
public class PeriodicWorkTimerThread extends Thread {

    ArrayList<ClientInterface> m_clientInterfaces;

    public PeriodicWorkTimerThread(ArrayList<ClientInterface> clientInterfaces) {
        m_clientInterfaces = clientInterfaces;
    }

    @Override
    public void run() {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        Thread.currentThread().setName("PeriodicWork");

        LinkedBlockingDeque<Object> foo = new LinkedBlockingDeque<Object>();
        while(true) {
            //long beforeTime = System.nanoTime();
            try {
                foo.poll(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return;
            }
            for (ClientInterface ci : m_clientInterfaces) {
                ci.processPeriodicWork();
            }
            //long duration = System.nanoTime() - beforeTime;
            //double millis = duration / 1000000.0;
            //System.out.printf("TICK %.2f\n", millis);
            //System.out.flush();
        }
    }

}
