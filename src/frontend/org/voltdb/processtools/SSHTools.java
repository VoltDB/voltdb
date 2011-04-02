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

import java.io.File;
import java.io.FileOutputStream;

public class SSHTools {

    private final String m_username;
    private final String m_keyFile;
    private final String m_password;

    public SSHTools() {
        this(null, null, null);
    }

    public SSHTools(String username) {
        this(username, null, null);
    }

    public SSHTools(String username, String key, String password) {
        if (username != null && username.isEmpty()) {
            m_username = null;
        } else {
            m_username = username;
        }
        m_keyFile = key;
        m_password = password;
    }

    public String generatePasswordScript() {
        try {
            final File passwordScript = File.createTempFile("foo", "bar");
            passwordScript.setExecutable(true);
            passwordScript.deleteOnExit();
            //Make sure it is deleted eventually
            final Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                    }
                    passwordScript.delete();
                }
            };
            t.setDaemon(true);
            t.start();
            final String sep = System.getProperty("line.separator");
            StringBuilder sb = new StringBuilder();
            if (m_password != null) {
                sb.append("#!/bin/sh").append(sep);
                sb.append("echo \"" + m_password + "\"").append(sep);
                sb.append("rm $0");
            } else {
                sb.append("#!/bin/sh").append(sep);
                sb.append("echo \"\"").append(sep);
                sb.append("rm $0");
            }
            FileOutputStream fos = new FileOutputStream(passwordScript);
            fos.write(sb.toString().getBytes("UTF-8"));
            fos.flush();
            fos.close();
            return passwordScript.getAbsolutePath();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String createUrl(String hostname, String path) {
        StringBuilder sb = new StringBuilder();
        if (m_username != null)
            sb.append(m_username + "@");

        if (hostname != null)
           sb.append(hostname);

        sb.append(":");
        if (path != null)
            sb.append(path);

        return sb.toString();
    }

    public boolean copyFromLocal(String src, String hostNameTo, String pathTo) {
        // scp -r -i identity -q src.getPath remoteUser@hostNameTo:/pathTo
        int len = 1;
        if (m_keyFile != null)
            len += 10;
        else
            len += 8;
        String[] command = new String[len];
        int i = 0;
        command[i++] = "scp";
        i = buildArgs(command);
        command[i++] = "-r";
        command[i++] = src;
        command[i++] = createUrl(hostNameTo, pathTo);
        assert(i == len);
        String output = ShellTools.cmd(null, command, generatePasswordScript());
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public boolean copyFromRemote(String dst, String hostNameFrom, String pathFrom) {
        // scp -r -i identity -q fromhost:path tohost:path
        int len = 1;
        if (m_keyFile != null)
            len += 10;
        else
            len += 8;
        String[] command = new String[len];
        int i = 0;
        command[i++] = "scp";
        i = buildArgs(command);
        command[i++] = "-r";
        command[i++] = createUrl(hostNameFrom, pathFrom);
        command[i++] = dst;
        assert(i == len);
        String output = ShellTools.cmd(null, command, generatePasswordScript());
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public boolean copyBetweenRemotes(String hostNameFrom, String pathFrom,
            String hostNameTo, String pathTo) {
        // scp -r -i identity -q fromhost:path tohost:path
        int len = 1;
        if (m_keyFile != null)
            len += 10;
        else
            len += 8;
        String[] command = new String[len];
        int i = 0;
        command[i++] = "scp";
        i = buildArgs(command);
        command[i++] = "-r";
        command[i++] = createUrl(hostNameFrom, pathFrom);
        command[i++] = createUrl(hostNameTo, pathTo);
        assert(i == len);

        String output = ShellTools.cmd(null, command, generatePasswordScript());
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public ProcessData command(String hostname, String remotePath, String command[], String processName, OutputHandler handler) {
        String passwordScript = generatePasswordScript();
        return ShellTools.command(null, convert(hostname, remotePath, command),
                                  passwordScript, processName, handler);
    }

    public String cmd(String hostname, String remotePath, String command) {
        return ShellTools.cmd(null, convert(hostname, remotePath, command), generatePasswordScript());
    }

    public String cmd(String hostname, String remotePath, String[] command) {
        return ShellTools.cmd(null, convert(hostname, remotePath, command), generatePasswordScript());
    }

    public String[] convert(String hostname, String remotePath, String command) {
        String[] command2 = command.split(" ");
        return convert(hostname, remotePath, command2);
    }

    public String[] convert(String hostname, String remotePath, String[] command) {
        assert(hostname != null);
        int sshArgCount = 7 + (remotePath == null ? 0 : 1) + (m_keyFile == null ? 0 : 2);

        String[] retval = new String[command.length + sshArgCount];
        int i = 0;
        retval[i++] = "ssh";
        i = buildArgs(retval);
        retval[i] = "";
        if (m_username != null)
            retval[i] = retval[i].concat(m_username + "@");
        retval[i] = retval[i].concat(hostname);
        i++;
        if (remotePath != null)
            retval[i] = "cd " + remotePath + ";";
        for (int j = 0; j < command.length; j++) {
            assert(command[j] != null);
            retval[sshArgCount + j] = command[j];
        }

        return retval ;
    }

    /**
     * This fills the few entries after the first one in the command list with
     * the common options for SSH. Currently takes 5 or 7 entries depends on
     * whether identity files are used.
     *
     * @param command
     */
    private int buildArgs(String[] command) {
        int i = 1;
        if (m_keyFile != null) {
            command[i++] = "-i";
            command[i++] = m_keyFile;
        }
        command[i++] = "-q";
        command[i++] = "-o";
        command[i++] = "UserKnownHostsFile=/dev/null";
        command[i++] = "-o";
        command[i++] = "StrictHostKeyChecking=no";

        return i;
    }

    public static void main(String args[]) throws Exception {
        SSHTools tools = new SSHTools(args[0], null, args[1]);
        System.out.println(tools.cmd( args[2], args[3], args[4]));
    }
}
