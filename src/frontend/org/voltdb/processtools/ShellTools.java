/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
import java.io.IOException;

public abstract class ShellTools {

    public static String cmd(String command) {
        String[] command2 = command.split(" ");
        return cmd(command2);
    }

    public static String cmd(String[] command) {
        StringBuilder retval = new StringBuilder();
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
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

    public static boolean cmdToStdOut(String[] command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
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

    public static boolean cmdToStdOut(String command) {
        String[] command2 = command.split(" ");
        return cmdToStdOut(command2);
    }

    public static void main(String[] args) {
        System.out.println(cmd("cat build.py"));
    }
}
