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

import java.io.File;

public abstract class SSHTools {


    public static String createUrl(String user, String hostname, String path) {
        String url = new String();
        if (user != null)
            url = url.concat(user + "@");

        if (hostname != null)
           url = url.concat(hostname);

        if (path != null)
            url = url.concat(":" + path);

        return url;
    }

    public static boolean copyFromLocal(File src, String remoteUser, String hostNameTo, String pathTo) {
        // scp -q src.getPath remoteUser@hostNameTo:/pathTo
        String[] command = new String[8];
        command[0] = "scp";
        command[1] = "-q";
        command[2] = "-o";
        command[3] = "UserKnownHostsFile=/dev/null";
        command[4] = "-o";
        command[5] = "StrictHostKeyChecking=no";
        command[6] = src.getPath();
        command[7] = createUrl(remoteUser, hostNameTo, pathTo);
        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public static boolean copyFromRemote(File dst, String remoteUser, String hostNameFrom, String pathFrom) {
        // scp -q fromhost:path tohost:path
        String[] command = new String[8];
        command[0] = "scp";
        command[1] = "-q";
        command[2] = "-o";
        command[3] = "UserKnownHostsFile=/dev/null";
        command[4] = "-o";
        command[5] = "StrictHostKeyChecking=no";
        command[6] = createUrl(remoteUser, hostNameFrom, pathFrom);
        command[7] = dst.getPath();

        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public static boolean copyBetweenRemotes(String remoteUser, String hostNameFrom, String pathFrom,
            String hostNameTo, String pathTo) {
        // scp -q fromhost:path tohost:path
        String[] command = new String[8];
        command[0] = "scp";
        command[1] = "-q";
        command[2] = "-o";
        command[3] = "UserKnownHostsFile=/dev/null";
        command[4] = "-o";
        command[5] = "StrictHostKeyChecking=no";
        command[6] = createUrl(remoteUser, hostNameFrom, pathFrom);
        command[7] = createUrl(remoteUser, hostNameTo, pathTo);

        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public static String cmd(String username, String hostname, String remotePath, String command) {
        return ShellTools.cmd(convert(username, hostname, remotePath, command));
    }

    public static String cmd(String username, String hostname, String remotePath, String[] command) {
        return ShellTools.cmd(convert(username, hostname, remotePath, command));
    }

    public static String[] convert(String username, String hostname, String remotePath, String command) {
        String[] command2 = command.split(" ");
        return convert(username, hostname, remotePath, command2);
    }

    public static String[] convert(String username, String hostname, String remotePath, String[] command) {
        assert(hostname != null);
        int sshArgCount = 7 + (remotePath == null ? 0 : 1);

        String[] retval = new String[command.length + sshArgCount];
        retval[0] = "ssh";
        retval[1] = "-q";
        retval[2] = "-o";
        retval[3] = "UserKnownHostsFile=/dev/null";
        retval[4] = "-o";
        retval[5] = "StrictHostKeyChecking=no";
        retval[6] = "";
        if (username != null)
            retval[6] = retval[6].concat(username + "@");
        retval[6] = retval[6].concat(hostname);
        if (remotePath != null)
            retval[7] = "cd " + remotePath + ";";
        for (int i = 0; i < command.length; i++) {
            assert(command[i] != null);
            retval[sshArgCount + i] = command[i];
        }

        return retval ;
    }

    public static void main(String[] args) {
        System.out.print(cmd(null, "volt3b", null, "echo foo"));
        System.out.println(copyFromLocal(new File("build.py"), null, "volt3b", "."));
    }
}
