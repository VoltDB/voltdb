/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package txnIdSelfCheck;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

class BenchmarkThread extends Thread {

    VoltLogger log;
    boolean m_slowFlight = false;
    int slowDownDelayMs = 1000;

    BenchmarkThread() {
        super();
        setDaemon(true);
        setUncaughtExceptionHandler(Benchmark.h);
        log = new VoltLogger(getClass().getSimpleName());
    }

    public void hardStop(String msg) {
        Benchmark.hardStop(msg);
    }

    public void hardStop(Exception e) {
        Benchmark.hardStop(e);
    }

    public void hardStop(String msg, Exception e) {
        Benchmark.hardStop(msg, e);
    }

    public void hardStop(String msg, ClientResponse resp) {
        Benchmark.hardStop(msg, (ClientResponseImpl) resp);
    }

    public void hardStop(String msg, ClientResponseImpl resp) {
        Benchmark.hardStop(msg, resp);
    }
}
