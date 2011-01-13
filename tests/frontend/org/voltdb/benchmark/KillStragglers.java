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

import org.voltdb.processtools.SSHTools;
import org.voltdb.processtools.ShellTools;

public class KillStragglers extends Thread {

    final String m_username;
    final String m_hostname;
    final String m_remotePath;

    public KillStragglers(String username, String hostname, String remotePath) {
        m_username = username;
        m_hostname = hostname;
        m_remotePath = remotePath;
    }

    public KillStragglers() {
        m_username = null;
        m_hostname = "localhost";
        m_remotePath = null;
    }

    @Override
    public void run() {
        if (m_remotePath != null) {
            final SSHTools ssh = new SSHTools(m_username);
            ShellTools.cmdToStdOut(ssh.convert(m_hostname, m_remotePath,
                    "/bin/bash killstragglers.sh"));
        }
        else {
            String[] cmd = { "/bin/bash", "test/killstragglers.sh" };
            ShellTools.cmdToStdOut(cmd);
        }
    }
}
