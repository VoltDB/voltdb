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

package udfbenchmark;

import org.voltdb.ClientAppBase;

public class UDFBenchmarkConfig extends ClientAppBase.AppClientConfig
{

    @Option(desc = "the type of table to be tested")
    String table = "partitioned";

    @Option(desc = "Comma separated list of the form server[:port] to connect to.")
    String servers = "localhost";

    @Option(desc = "Number of rows inserted for the benchmark.")
    int datasize = 10000000;

    @Option(desc = "Interval for performance feedback, in seconds.")
    long displayinterval = 60;

    @Option(desc = "Warmup duration in seconds.")
    int warmup = 2;

    @Option(desc = "Filename to write raw summary statistics to.")
    String statsfile = "udf-stats";

    @Override
    public void validateParameters() {
        if (datasize < 0) {
            exitWithMessageAndUsage("datasize must be 0 or a positive integer");
        }
        if (displayinterval < 0) {
            exitWithMessageAndUsage("displayinterval must be 0 or a positive integer");
        }
        if (warmup < 0) {
            exitWithMessageAndUsage("warmup must be 0 or a positive integer");
        }
    }
}
