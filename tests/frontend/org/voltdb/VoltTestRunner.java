/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

public class VoltTestRunner extends RunListener {

    class MultiFileOutputStream extends OutputStream {

        HashMap<String, OutputStream> m_streamMap = new HashMap<String, OutputStream>();;
        volatile OutputStream[] m_streams = new OutputStream[0];

        void rebuildStreamArray() {
            OutputStream[] streams = new OutputStream[m_streamMap.size()];
            int i = 0;
            for (OutputStream s : m_streamMap.values()) {
                streams[i++] = s;
            }
            m_streams = streams;
        }

        public void addOutputStream(File f) throws IOException {
            FileOutputStream fos = new FileOutputStream(f);
            m_streamMap.put(f.getCanonicalPath(), fos);
            rebuildStreamArray();
        }

        public void removeOutputStream(File f) throws IOException {
            OutputStream os = m_streamMap.get(f.getCanonicalPath());
            if (os == null) return;
            os.flush();
            os.close();
            m_streamMap.remove(f.getCanonicalPath());
            rebuildStreamArray();
        }

        @Override
        public void close() throws IOException {
            for (int i = 0; i < m_streams.length; i++) {
                m_streams[i].close();
            }
        }

        @Override
        public void flush() throws IOException {
            for (int i = 0; i < m_streams.length; i++) {
                m_streams[i].flush();
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            for (int i = 0; i < m_streams.length; i++) {
                m_streams[i].write(b, off, len);
            }
        }

        @Override
        public void write(int arg0) throws IOException {
            for (int i = 0; i < m_streams.length; i++) {
                m_streams[i].write(arg0);
            }
        }


    }

    PrintStream STDOUT = System.out;
    PrintStream STDERR = System.err;
    final MultiFileOutputStream m_out = new MultiFileOutputStream();
    final Class<?> m_testClazz;
    final String m_timestamp;
    String m_dir = null;

    public VoltTestRunner(Class<?> testClazz, String timestamp) {
        super();
        m_testClazz = testClazz;
        m_timestamp = timestamp;
    }

    public boolean run() throws IOException {
        try {
            m_dir = "testout/junit-" + m_timestamp + "/" + m_testClazz.getCanonicalName() + "/";
            new File(m_dir).mkdirs();

            // redirect std out/err to files
            m_out.addOutputStream(new File(m_dir + "fulloutput.txt"));
            System.setOut(new PrintStream(m_out, true));
            System.setErr(new PrintStream(m_out, true));

            JUnitCore junit = new JUnitCore();
            junit.addListener(this);
            Result r = junit.run(m_testClazz);
            STDOUT.printf("RESULTS: %d/%d\n", r.getRunCount() - r.getFailureCount(), r.getRunCount());
            return true;
        }
        catch (Exception e) {
            return false;
        }
        finally {
            m_out.flush();
            System.setOut(STDOUT);
            System.setErr(STDERR);
            m_out.close();
        }
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        STDOUT.println("FAILED: " + failure.getDescription().getMethodName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        STDOUT.println("FINISHED: " + description.getMethodName());
        m_out.removeOutputStream(new File(m_dir + description.getMethodName() + ".txt"));
    }

    @Override
    public void testStarted(Description description) throws Exception {
        STDOUT.println("STARTED: " + description.getMethodName());
        m_out.addOutputStream(new File(m_dir + description.getMethodName() + ".txt"));
    }

    static void configLog4J() {
        File log4jFile = new File("tests/runner/log4j.properties");
        if (log4jFile.exists() == false) {
            log4jFile = new File("log4j.properties");
            if (log4jFile.exists() == false) {
                System.err.println("Unable to locate log4j properties file. Exiting.");
                System.exit(-1);
            }
        }
        URL url = null;
        try {
            url = new URL("file://" + log4jFile.getAbsolutePath());
        } catch (MalformedURLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.setProperty("log4j.configuration", url.toString());
    }

    /**
     *
     * @param args The class to test, followed by the timestamp of the test
     */
    public static void main(String[] args) {
        configLog4J();

        if (args.length == 0) {
            System.out.println("TESTERROR: No Arguments");
            System.exit(-1);
        }

        String className = args[0].trim();
        DateFormat df = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss");
        String timestamp = df.format(new Date());
        if (args.length >= 2) {
            // format of the timestamp is "yyyy.MM.dd-HH.mm.ss"
            timestamp = args[1].trim();
        }

        try {
            Class<?> testClazz = Class.forName(className);
            VoltTestRunner vtr = new VoltTestRunner(testClazz, timestamp);
            vtr.run();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
