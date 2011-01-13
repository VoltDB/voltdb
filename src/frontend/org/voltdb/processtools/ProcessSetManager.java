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

package org.voltdb.processtools;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltdb.processtools.ProcessData.OutputLine;

public class ProcessSetManager {

    LinkedBlockingQueue<OutputLine> m_output = new LinkedBlockingQueue<OutputLine>();
    Map<String, ProcessData> m_processes = new HashMap<String, ProcessData>();
    SimpleProgressHandler m_handler = new SimpleProgressHandler(m_output);

    class SimpleProgressHandler implements OutputHandler {
        private final LinkedBlockingQueue<OutputLine> m_output;

        public SimpleProgressHandler(LinkedBlockingQueue<OutputLine> output) {
            m_output = output;
        }

        @Override
        public void update(OutputLine line) {
            m_output.add(line);
        }
    }

    static Set<ProcessData> createdProcesses = new HashSet<ProcessData>();
    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            synchronized(createdProcesses) {
                for (ProcessData p : createdProcesses)
                    p.kill();
            }
        }
    }
    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    }

    public String[] getProcessNames() {
        String[] retval = new String[m_processes.size()];
        int i = 0;
        for (String clientName : m_processes.keySet())
            retval[i++] = clientName;
        return retval;
    }

    public void startProcess(String processName, String[] cmd) {
        ProcessData pd;
        try {
            pd = new ProcessData(processName, cmd, null, m_handler);
            synchronized(createdProcesses) {
                createdProcesses.add(pd);
            }
            assert(m_processes.containsKey(processName) == false);
            m_processes.put(processName, pd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public OutputLine nextBlocking() {
        try {
            return m_output.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public OutputLine nextNonBlocking() {
        return m_output.poll();
    }

    public void writeToProcess(String processName, String data) throws IOException {
        ProcessData pd = m_processes.get(processName);
        assert(pd != null);
        pd.write(data);
    }

    public int joinProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        assert(pd != null);

        return pd.join();
    }

    public int killProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        int retval = pd.kill();
        synchronized(createdProcesses) {
            createdProcesses.remove(pd);
        }

        return retval;
    }

    public void killAll() {
        for (String name : m_processes.keySet()) {
            killProcess(name);
        }
    }

    public int size() {
        return m_processes.size();
    }

    public static void main(String[] args) {
        ProcessSetManager psm = new ProcessSetManager();
        psm.startProcess("ping4c", new String[] { "ping", "volt4c" });
        psm.startProcess("ping3c", new String[] { "ping", "volt3c" });
        while(true) {
            OutputLine line = psm.nextBlocking();
            System.out.printf("(%s:%s): %s\n", line.processName, line.stream.name(), line.message);
        }
    }

}
