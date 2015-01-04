/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltcore.utils.PortGenerator;

public class PortGeneratorForTest extends PortGenerator {

    public PortProvider pprovider = null;

    class PortProvider {

        public int nClient = -1;
        public int nAdmin = -1;
        public int nZkport = -1;
        public int nReplicationPort = -1;
        public int nJMXPort = -1;
        public int nInternalPort = -1;
        public int nHttp = -1;

        public int nextInternalPort() {
            return nInternalPort;
        }

        public void setInternalPort(int nc) {
            nInternalPort = nc;
        }

        public int nextJMXPort() {
            return nJMXPort;
        }

        public void setJMXPort(int nc) {
            nJMXPort = nc;
        }

        public int nextReplicationPort() {
            return nReplicationPort;
        }

        public void setReplicationPort(int nc) {
            nReplicationPort = nc;
        }

        public int nextZkPort() {
            return nZkport;
        }

        public void setZkPort(int nc) {
            nZkport = nc;
        }

        public int nextClient() {
            return nClient;
        }

        public void setNextClient(int nc) {
            nClient = nc;
        }

        public int nextAdmin() {
            return nAdmin;
        }

        public void setAdmin(int nc) {
            nAdmin = nc;
        }

        public int nextHttp() {
            return nHttp;
        }

        public void setHttp(int nc) {
            nHttp = nc;
        }
    }

    public void enablePortProvider() {
        pprovider = new PortProvider();
    }

    @Override
    public int nextClient() {
        if (pprovider != null) {
            int rport = pprovider.nextClient();
            if (rport != -1) {
                return rport;
            }
        }
        return super.nextClient();
    }

    @Override
    public int nextAdmin() {
        if (pprovider != null) {
            int rport = pprovider.nextAdmin();
            if (rport != -1) {
                return rport;
            }
        }
        return super.nextAdmin();
    }

    @Override
    public int nextHttp() {
        if (pprovider != null) {
            int rport = pprovider.nextHttp();
            if (rport != -1) {
                return rport;
            }
        }
        return super.nextHttp();
    }

    public int nextZkPort() {
        if (pprovider != null) {
            int rport = pprovider.nextZkPort();
            if (rport != -1) {
                return rport;
            }
        }
        return super.next();
    }

    public int nextReplicationPort() {
        if (pprovider != null) {
            int rport = pprovider.nextReplicationPort();
            if (rport != -1) {
                return rport;
            }
        }
        return super.next();
    }

    public int nextInternalPort() {
        if (pprovider != null) {
            int rport = pprovider.nextInternalPort();
            if (rport != -1) {
                return rport;
            }
        }
        return super.next();
    }
}
