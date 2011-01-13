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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.benchmark.BenchmarkResults.Result;
import org.voltdb.processtools.SSHTools;

public class ResultsUploader implements BenchmarkController.BenchmarkInterest {

    Connection m_conn = null;
    Statement m_stmt = null;
    final BenchmarkConfig m_config;
    final String m_benchmarkName;
    String m_benchmarkOptions;
    final HashMap<String, String> m_hostIdCache = new HashMap<String, String>();
    final HashMap<String, String[]> m_hostDistroCache = new HashMap<String, String[]>();
    final HashMap<String, String> m_clientArgs = new HashMap<String, String>();
    final HashMap<String, String> m_hostArgs = new HashMap<String, String>();
    final SSHTools m_ssh;

    ResultsUploader(String benchmarkName, BenchmarkConfig config) {
        assert(config != null);
        m_config = config;
        m_benchmarkName = benchmarkName;

        m_benchmarkOptions = "";
        for (Entry<String, String> param : config.parameters.entrySet())
            m_benchmarkOptions += param.getKey() + "=" + param.getValue() + " ";
        m_benchmarkOptions = m_benchmarkOptions.trim();

        m_ssh = new SSHTools(m_config.remoteUser);
    }

    public void setCommandLineForClient(String clientAndIndex, String commandLine) {
        m_clientArgs.put(clientAndIndex, commandLine.trim());
    }

    public void setCommandLineForHost(String host, String commandLine) {
        m_hostArgs.put(host, commandLine.trim());
    }

    private void addToHostsTableIfMissing(String host) throws SQLException {
        assert m_stmt != null;
        String hostid = getHostIdForHostName(host);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT hostid from hosts where hostid='").append(hostid).append("';");
        java.sql.ResultSet rs = m_stmt.executeQuery(sql.toString());

        // not present - insert entry
        if (!rs.first()) {
            sql = new StringBuilder();
            sql.append("INSERT INTO `hosts` (`hostid`, `hostname`, `description`) values (");
            sql.append("'").append(hostid).append("', ");
            sql.append("'").append(host).append("', ");
            sql.append("'").append(getDescriptionForHostName(host)).append("');");
            m_stmt.executeUpdate(sql.toString());
        }
    }

    @Override
    public void benchmarkHasUpdated(BenchmarkResults results,
            long[] clusterLatencies, long[] clientLatencies) {
        int pollIndex = results.getCompletedIntervalCount();
        long duration = results.getTotalDuration();
        long interval = results.getIntervalDuration();

        // don't do anything if not finished
        if ((pollIndex * interval) < duration)
            return;

        // connect to the server
        try {
            m_conn =  DriverManager.getConnection(m_config.resultsDatabaseURL);
            m_stmt = m_conn.createStatement();
        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("Unable to connect to MySQL results recording server.");
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("Timeout was " + DriverManager.getLoginTimeout());
            System.out.println("Trying again");
            try {
                m_conn =  DriverManager.getConnection(m_config.resultsDatabaseURL);
                m_stmt = m_conn.createStatement();
            }
            catch (SQLException ex2) {
                System.out.println("Still can't connect.");
                System.out.println("SQLException: " + ex.getMessage());
                return;
            }
        }

        // upload
        try {
            StringBuilder sql = null;

            // safest thing possible
            m_conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // commit everything or nothing
            m_conn.setAutoCommit(false);

            // insert the main data
            sql = new StringBuilder();
            sql.append("INSERT INTO results (`userid`, `benchmarkname`, `benchmarkoptions`, " +
                    "`duration`, `interval`, `sitesperhost`, `remotepath`, `hostcount`, `clientcount`, `totalhosts`, `totalclients`, `processesperclient`) values (");
            sql.append("'").append(getCurrentUserId()).append("', ");
            sql.append("'").append(m_benchmarkName).append("', ");
            sql.append("'").append(m_benchmarkOptions).append("', ");
            sql.append(m_config.duration).append(", ");
            sql.append(m_config.interval).append(", ");
            sql.append(m_config.sitesPerHost).append(", ");
            sql.append("'").append(m_config.remotePath).append("', ");
            sql.append(m_config.hosts.length).append(", ");
            sql.append(m_config.clients.length).append(", ");
            sql.append(m_config.hosts.length * m_config.sitesPerHost).append(", ");
            sql.append(m_config.clients.length * m_config.processesPerClient).append(", ");
            sql.append(m_config.processesPerClient).append(");");
            //System.out.println(sql.toString());
            m_stmt.executeUpdate(sql.toString());

            // get the result primary key
            int resultid = -1;
            java.sql.ResultSet rs = m_stmt.executeQuery("SELECT LAST_INSERT_ID()");
            if (rs.next()) {
                resultid = rs.getInt(1);
            } else {
                throw new RuntimeException();
            }

            // add all of the server participants in the benchmark
            for (InetSocketAddress host : m_config.hosts) {
                String hostname = host.getHostName();
                String args = m_hostArgs.get(hostname);
                if (args == null) args = "";
                addToHostsTableIfMissing(hostname);
                String distro[] = getHostDistroForHostName(hostname);

                sql = new StringBuilder();
                sql.append("INSERT INTO participants (`resultid`, `hostid`, `distributor`, `release`, `role`, `commandline`) values (");
                sql.append(String.valueOf(resultid)).append(", ");
                sql.append("'").append(getHostIdForHostName(hostname)).append("', ");
                sql.append("'").append(distro[0]).append("', ");
                sql.append("'").append(distro[1]).append("', ");
                sql.append("'SERVER', ");
                sql.append("'").append(args).append("');");
                m_stmt.executeUpdate(sql.toString());
            }

            // add of the actual benchmark data
            for (String clientName : results.getClientNames()) {
                // insert all the client participants
                String[] clientParts = clientName.split(":");
                String clientHostId = getHostIdForHostName(clientParts[0].trim());
                String processIndex = clientParts[1].trim();
                String distro[] = getHostDistroForHostName(clientParts[0].trim());

                String args = m_clientArgs.get(clientName);
                if (args == null) args = "";
                addToHostsTableIfMissing(clientParts[0].trim());

                sql = new StringBuilder();
                sql.append("INSERT INTO participants (`resultid`, `hostid`, `distributor`, `release`, `processindex`, `role`, `commandline`) values (");
                sql.append(String.valueOf(resultid)).append(", ");
                sql.append("'").append(clientHostId).append("', ");
                sql.append("'").append(distro[0]).append("', ");
                sql.append("'").append(distro[1]).append("', ");
                sql.append(processIndex).append(", ");
                sql.append("'CLIENT', ");
                sql.append("'").append(args).append("');");
                m_stmt.executeUpdate(sql.toString());

                for (String txnName : results.getTransactionNames()) {
                    Result[] rset = results.getResultsForClientAndTransaction(clientName, txnName);

                    for (int i = 0; i < rset.length; i++) {
                        Result r = rset[i];

                        sql = new StringBuilder();
                        sql.append("INSERT INTO resultparts (`resultid`, `clienthost`, `processindex`, `transaction`, `interval`, `count`) values (");
                        sql.append(String.valueOf(resultid)).append(", ");
                        sql.append("'").append(clientHostId).append("', ");
                        sql.append(processIndex).append(", ");
                        sql.append("'").append(txnName).append("', ");
                        sql.append(i).append(", ");
                        sql.append(r.transactionCount).append(");");
                        m_stmt.executeUpdate(sql.toString());
                    }
                }
            }

            // create rolled up information by interval
            sql = new StringBuilder();
            sql.append("insert into resultintervals (`resultid`, `interval`, `seconds`, `intervaltxn`, `intervaltxnpersecond`) ");
            sql.append("select r.resultid, ");
            sql.append("       rp.interval, ");
            sql.append("       ((rp.interval + 1) * r.interval / 1000) seconds, ");
            sql.append("       sum(rp.count) intervaltxn, ");
            sql.append("       sum(rp.count) / (r.interval / 1000) intervaltxnpersecond ");
            sql.append("from results r, ");
            sql.append("     resultparts rp ");
            sql.append("where rp.resultid = r.resultid and ");
            sql.append("      r.resultid = ").append(String.valueOf(resultid)).append(" ");
            sql.append("group by rp.interval, r.interval;");
            m_stmt.executeUpdate(sql.toString());

            // Update main data (total transactions and transactions per second)
            sql = new StringBuilder();
            sql.append("update results r ");
            sql.append("set r.totaltxn = (select sum(rp.count) from resultparts rp where rp.resultid = r.resultid), ");
            sql.append("    r.txnpersecond = (select sum(rp.count) from resultparts rp where rp.resultid = r.resultid) / r.duration * 1000 ");
            sql.append("where r.resultid = ").append(String.valueOf(resultid)).append(";");
            m_stmt.executeUpdate(sql.toString());

            m_conn.commit();

        } catch (SQLException e) {
            System.err.println("Unable to save results to results server.");
            System.err.println("  Consider uncommenting debugging output in ResultsUploader.java.");
            System.err.flush();

            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public String getDescriptionForHostName(String hostname) {
        /*
         * This is a total hack pending code to feed hostname
         * descriptions through benchmark controller main
         */

        if (hostname.contains("amazonaws.com"))
            return "amazonws";  // really want m1.large or m1.xlarge
        if (hostname.contains("-bl"))
            return "desktop";
        if (hostname.contains("-gr"))
            return "desktop";
        if (hostname.contains("localhost"))
            return "desktop";
        else
            return "unknown";
    }

    public String getHostIdForHostName(String hostname) {
        String mac = m_hostIdCache.get(hostname);
        if (mac == null) {
            mac = m_ssh.cmd(hostname, m_config.remotePath, "./getmac.py");
            mac = mac.trim();
            m_hostIdCache.put(hostname, mac);
        }
        return mac;
    }

    public String[] getHostDistroForHostName(String hostname) {
        String[] retval = m_hostDistroCache.get(hostname);

        if (retval != null)
            return retval;

        retval = new String[2];
        String distro = m_ssh.cmd(hostname,
                                  m_config.remotePath, "lsb_release -ir");
        String[] lines = distro.trim().split("\n");
        for (String l : lines) {
            String[] kv = l.split(":");
            if (kv[0].startsWith("Distributor"))
                retval[0] = kv[1].trim();
            else if (kv[0].startsWith("Release"))
                retval[1] = kv[1].trim();
        }
        m_hostDistroCache.put(hostname, retval);

        return retval;
    }

    public String getCurrentUserId() {
        String username = System.getProperty("user.name");
        java.sql.ResultSet rs;
        try {
            rs = m_stmt.executeQuery(
                    "select count(*) from `users` where `username` = '" + username + "';");
            if (rs.next()) {
                if (rs.getInt(1) != 1) {
                    int rows = m_stmt.executeUpdate(
                            "insert into `users` (`username`) values ('" + username + "')");
                    if (rows != 1)
                        throw new RuntimeException();
                }
            }
            else {
                throw new RuntimeException();
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return username;
    }
}
