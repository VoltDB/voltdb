/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NCProcess {

    Process m4_process;
    Process m6_process;

    public NCProcess(int port) {
        try {
            m4_process = new ProcessBuilder("nc", "-l", "-4", String.valueOf(port)).start();
            m6_process = new ProcessBuilder("nc", "-l", "-6", String.valueOf(port)).start();
            Thread.sleep(2000);
        } catch (IOException ex) {
            Logger.getLogger(NCProcess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(NCProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public NCProcess(int port, boolean loopback) {
        try {
            m4_process = new ProcessBuilder("nc", "-l", "-4", "127.0.0.1", String.valueOf(port)).start();
            m6_process = new ProcessBuilder("nc", "-l", "-6", "127.0.0.1", String.valueOf(port)).start();
            Thread.sleep(2000);
        } catch (IOException ex) {
            Logger.getLogger(NCProcess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(NCProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public int close() {
        int eval = 2;
        if (m4_process != null) {
            try {
                m4_process.destroy();
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(NCProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (m6_process != null) {
            try {
                m6_process.destroy();
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(NCProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return eval;
    }
}
