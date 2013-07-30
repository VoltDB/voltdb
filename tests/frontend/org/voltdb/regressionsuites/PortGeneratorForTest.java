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

import org.voltcore.utils.PortGenerator;

public class PortGeneratorForTest extends PortGenerator {

    public PortProvider pprovider = null;

    class PortProvider {

        public int nClient;
        public int nAdmin;

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
    }

    public void enablePortProvider() {
        pprovider = new PortProvider();
    }

    @Override
    public int nextClient() {
        if (pprovider != null) {
            return pprovider.nextClient();
        }
        return super.nextClient();
    }

    @Override
    public int nextAdmin() {
        if (pprovider != null) {
            return pprovider.nextAdmin();
        }
        return super.nextAdmin();
    }
}
