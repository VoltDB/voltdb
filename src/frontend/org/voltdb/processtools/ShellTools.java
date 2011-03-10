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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

public abstract class ShellTools {

    private static Process createProcess(String dir, String command[], String passwordScript) {
        ProcessBuilder pb = new ProcessBuilder(command);
        if (dir != null) {
            File wd = new File(dir);
            pb.directory(wd);
        }
        pb.redirectErrorStream(true);
        if (passwordScript != null) {
            pb.environment().put("SSH_ASKPASS", passwordScript);
        }
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            return null;
        }
        return p;
    }

    public static String cmd(String command) {
        return cmd(null, command, null);
    }

    public static String cmd(String dir, String command) {
        return cmd(dir, command, null);
    }

    public static String cmd(String dir, String command, String input) {
        String[] command2 = command.split(" ");
        return cmd(dir, command2, input);
    }

    public static String cmd(String dir, String[] command, String passwordScript) {
        StringBuilder retval = new StringBuilder();
        Process p = createProcess(dir, command, passwordScript);
        if (p == null) {
            return null;
        }

        BufferedInputStream in = new BufferedInputStream(p.getInputStream());
        int c;
        try {
            while((c = in.read()) != -1) {
                retval.append((char)c);
            }
        }
        catch (Exception e) {
            p.destroy();
        }
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        p.destroy();
        return retval.toString();
    }

    public static ProcessData command(String dir, String command[], String passwordScript,
                                      String processName, OutputHandler handler) {
        Process p = createProcess(dir, command, passwordScript);
        if (p == null) {
            return null;
        }
        return new ProcessData(processName, handler, p);
    }

    public static boolean cmdToStdOut(String[] command, String passwordScript) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        if (passwordScript != null) {
            pb.environment().put("SSH_ASKPASS", passwordScript);
        }
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        BufferedInputStream in = new BufferedInputStream(p.getInputStream());
        int c;
        try {
            while((c = in.read()) != -1) {
                System.out.print((char)c);
            }
        }
        catch (Exception e) {
            p.destroy();
        }
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        p.destroy();
        return p.exitValue() == 0;
    }

    public static boolean cmdToStdOut(String command[]) {
        return cmdToStdOut( command, null);
    }

    public static boolean cmdToStdOut(String command, String input) {
        String[] command2 = command.split(" ");
        return cmdToStdOut(command2, input);
    }

    public static boolean cmdToStdOut(String command) {
        return cmdToStdOut(command, null);
    }

    public static void main(String[] args) {
        System.out.println(cmd("cat build.py"));
    }
}
