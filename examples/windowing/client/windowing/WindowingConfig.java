/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


package windowing;

import org.voltdb.CLIConfig;

/**
 * Uses included {@link CLIConfig} class to
 * declaratively state command line options with defaults
 * and validation.
 */
class WindowingConfig extends CLIConfig {
    @Option(desc = "Interval for performance feedback, in seconds.")
    long displayinterval = 5;

    @Option(desc = "Benchmark duration, in seconds.")
    int duration = 2000;

    @Option(desc = "Warmup duration in seconds.")
    int warmup = 2;

    @Option(desc = "Comma separated list of the form server[:port] to connect to.")
    String servers = "localhost";

    @Option(desc = "")
    long maxrows = 0;

    @Option(desc = "")
    long historyseconds = 30;

    @Option(desc = "")
    long maxrss = 0;

    @Option(desc = "Maximum TPS rate for benchmark.")
    int ratelimit = 15000;

    @Option(desc = "Report latency for async benchmark run.")
    boolean latencyreport = false;

    @Option(desc = "User name for connection.")
    String user = "";

    @Option(desc = "Password for connection.")
    String password = "";

    @Override
    public void validate() {
        if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
        if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
        if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
        if (maxrows < 0) exitWithMessageAndUsage("maxrows must be >= 0");
        if (historyseconds < 0) exitWithMessageAndUsage("historyseconds must be >= 0");
        if (historyseconds < 0) exitWithMessageAndUsage("historyseconds must be >= 0");
        if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
    }
}
