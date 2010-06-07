/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.processtools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessSetManager {

    LinkedBlockingQueue<OutputLine> m_output = new LinkedBlockingQueue<OutputLine>();
    Map<String, ProcessData> m_processes = new HashMap<String, ProcessData>();
    Map<String, StreamWatcher> m_watchers = new HashMap<String, StreamWatcher>();

    public enum Stream { STDERR, STDOUT; }

    static class ProcessData {
        Process process;
        StreamWatcher out;
        StreamWatcher err;
    }

    /**
     *
     *
     */
    public final class OutputLine {
        OutputLine(String processName, Stream stream, String value) {
            assert(value != null);
            this.processName = processName;
            this.stream = stream;
            this.value = value;
        }

        public final String processName;
        public final Stream stream;
        public final String value;
    }

    static Set<Process> createdProcesses = new HashSet<Process>();
    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            synchronized(createdProcesses) {
                for (Process p : createdProcesses)
                    p.destroy();
            }
        }
    }
    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    }

    class StreamWatcher extends Thread {
        final BufferedReader m_reader;
        final String m_processName;
        final Stream m_stream;
        final AtomicBoolean m_expectDeath = new AtomicBoolean(false);

        StreamWatcher(BufferedReader reader, String processName, Stream stream) {
            assert(reader != null);
            m_reader = reader;
            m_processName = processName;
            m_stream = stream;
        }

        void setExpectDeath(boolean expectDeath) {
            m_expectDeath.set(expectDeath);
        }

        @Override
        public void run() {
            while (true) {
                String line = null;
                try {
                    line = m_reader.readLine();
                } catch (IOException e) {
                    if (!m_expectDeath.get()) {
                        e.printStackTrace();
                        System.err.print("Err Stream monitoring thread exiting.");
                        System.err.flush();
                    }
                    return;
                }
                if (line != null) {
                    OutputLine ol = new OutputLine(m_processName, m_stream, line);
                    final long now = (System.currentTimeMillis() / 1000) - 1256158053;
                    System.out.println("(" + now + ")" + m_processName + ": " + line);
                    m_output.add(ol);
                }
                else
                    Thread.yield();
            }
        }
    }

    public String[] getProcessNames() {
        String[] retval = new String[m_processes.size()];
        int i = 0;
        for (String clientName : m_processes.keySet())
            retval[i++] = clientName;
        return retval;
    }

    public void startProcess(String processName, String[] cmd) {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        ProcessData pd = new ProcessData();
        try {
            pd.process = pb.start();
            synchronized(createdProcesses) {
                createdProcesses.add(pd.process);
            }
            assert(m_processes.containsKey(processName) == false);
            m_processes.put(processName, pd);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        BufferedReader out = new BufferedReader(new InputStreamReader(pd.process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(pd.process.getErrorStream()));
        pd.out = new StreamWatcher(out, processName, Stream.STDOUT);
        pd.err = new StreamWatcher(err, processName, Stream.STDERR);
        pd.out.start();
        pd.err.start();
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
        OutputStreamWriter out = new OutputStreamWriter(pd.process.getOutputStream());
        out.write(data);
        out.flush();
    }

    public int joinProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        assert(pd != null);
        pd.out.m_expectDeath.set(true);
        pd.err.m_expectDeath.set(true);

        try {
            pd.process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return killProcess(processName);
    }

    public int killProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        pd.out.m_expectDeath.set(true);
        pd.err.m_expectDeath.set(true);
        int retval = -255;

        pd.process.destroy();
        try {
            pd.process.waitFor();
            retval = pd.process.exitValue();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        synchronized(createdProcesses) {
            createdProcesses.remove(pd.process);
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
            System.out.printf("(%s:%s): %s\n", line.processName, line.stream.name(), line.value);
        }
    }

}
