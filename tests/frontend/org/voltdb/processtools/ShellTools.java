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
