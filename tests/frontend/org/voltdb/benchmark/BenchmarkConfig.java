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

package org.voltdb.benchmark;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class BenchmarkConfig {

    public final String benchmarkClient;
    public final String projectBuilderName;
    public final String backend;
    public final InetSocketAddress[] hosts;
    public final int sitesPerHost;
    public final int k_factor;
    public final String[] clients;
    public final int processesPerClient;
    public final long interval;
    public final long duration;
    public final String remotePath;
    public final String remoteUser;
    public final boolean listenForDebugger;
    public final int serverHeapSize;
    public final int clientHeapSize;
    public final boolean localmode;
    public final String useProfile;
    public final float checkTransaction;
    public final boolean checkTables;
    public final String voltRoot;
    public final String snapshotPath;
    public final String snapshotPrefix;
    public final String snapshotFrequency;
    public final int snapshotRetain;
    public final String statsDatabaseURL;
    public final String resultsDatabaseURL;
    public final String statsTag;//Identifies the result set
    public final String applicationName;
    public final String subApplicationName;
    public final boolean showConsoleOutput;
    public final String pushfiles;
    public final Integer maxOutstanding;

    public final Map<String, String> parameters = new HashMap<String, String>();

    public BenchmarkConfig(
            String benchmarkClient,
            String projectBuilderName,
            String backend,
            InetSocketAddress[] hosts,
            int sitesPerHost,
            int kFactor,
            String[] clients,
            int processesPerClient,
            long interval,
            long duration,
            String remotePath,
            String remoteUser,
            boolean listenForDebugger,
            int serverHeapSize,
            int clientHeapSize,
            boolean localmode,
            String useProfile,
            float checkTransaction,
            boolean checkTables,
            String voltRoot,
            String snapshotPath,
            String snapshotPrefix,
            String snapshotFrequency,
            int snapshotRetain,
            String statsDatabaseURL,
            String resultsDatabaseURL,
            String statsTag,
            String applicationName,
            String subApplicationName,
            boolean showConsoleOutput,
            String pushfiles,
            Integer maxOutstanding
        ) {

        this.benchmarkClient = benchmarkClient;
        this.projectBuilderName = projectBuilderName;
        this.backend = backend;
        this.hosts = new InetSocketAddress[hosts.length];
        for (int i = 0; i < hosts.length; i++)
            this.hosts[i] = hosts[i];
        this.sitesPerHost = sitesPerHost;
        this.k_factor = kFactor;
        this.clients = new String[clients.length];
        for (int i = 0; i < clients.length; i++)
            this.clients[i] = clients[i];
        this.processesPerClient = processesPerClient;
        this.interval = interval;
        this.duration = duration;
        this.remotePath = remotePath;
        this.remoteUser = remoteUser;
        this.listenForDebugger = listenForDebugger;
        this.serverHeapSize = serverHeapSize;
        this.clientHeapSize = clientHeapSize;
        this.localmode = localmode;
        this.useProfile = useProfile;
        this.checkTransaction = checkTransaction;
        this.checkTables = checkTables;
        this.voltRoot = voltRoot;
        this.snapshotPath = snapshotPath;
        this.snapshotPrefix = snapshotPrefix;
        this.snapshotFrequency = snapshotFrequency;
        this.snapshotRetain = snapshotRetain;
        this.resultsDatabaseURL = resultsDatabaseURL;
        this.statsDatabaseURL = statsDatabaseURL;
        this.statsTag = statsTag;
        this.applicationName = applicationName;
        this.subApplicationName = subApplicationName;
        this.showConsoleOutput = showConsoleOutput;
        this.pushfiles = pushfiles;
        this.maxOutstanding = maxOutstanding;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HOSTS:");
        for (InetSocketAddress host : hosts)
            sb.append(" ").append(host);
        sb.append("\n");
        sb.append("SITES PER HOST: ").append(sitesPerHost).append("\n");
        sb.append("K-FACTOR: ").append(k_factor).append("\n");
        sb.append("CLIENTS:");
        for (String client : clients)
            sb.append(" ").append(client);
        sb.append("\n");

        return sb.toString();
    }
}
