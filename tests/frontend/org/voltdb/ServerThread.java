/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

/**
 * Wraps VoltDB in a Thread
 */
public class ServerThread extends Thread {
    VoltDB.Configuration m_config;
    boolean initialized = false;

    public ServerThread(VoltDB.Configuration config) {
        m_config = config;

        if (!m_config.validate()) {
            m_config.usage();
            System.exit(-1);
        }

        setName("ServerThread");
    }

    public ServerThread(String pathToCatalog, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_backend = target;
    }

    public ServerThread(String pathToCatalog, String pathToDeployment, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_pathToDeployment = pathToDeployment;
        m_config.m_backend = target;

        if (!m_config.validate()) {
            m_config.usage();
            System.exit(-1);
        }
    }

    @Override
    public void run() {
        VoltDB.initialize(m_config);
        VoltDB.instance().run();
    }

    public void waitForInitialization() {
        // Wait until the server has actually started running.
        while (!VoltDB.instance().isRunning()) {
            Thread.yield();
        }
    }

    public void shutdown() throws InterruptedException {
        assert Thread.currentThread() != this;
        VoltDB.instance().shutdown(this);
        this.join();
    }
}
