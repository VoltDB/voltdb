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

package org.voltdb.twitter.hadoop.mr;

import java.net.URI;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobRunner extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(JobRunner.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new JobRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            LOG.error("usage: [twitter dir]");
            System.exit(1);
        }

        String twitterDir = args[0];

        Configuration config = new Configuration();
        config.setInt("mapred.job.reuse.jvm.num.tasks", -1);

        Calendar cal = Calendar.getInstance();
        cal.roll(Calendar.DATE, false);
        config.setLong("DISCARD_POINT", cal.getTime().getTime());

        ToolRunner.run(config, new Bucket(twitterDir + "/input", twitterDir + "/tmp"), args);
        ToolRunner.run(config, new Count(twitterDir + "/tmp", twitterDir + "/output"), args);

        //FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000" + twitterDir), config);
        //fs.delete(new Path(twitterDir + "/tmp"), true);
        //fs.close();

        return 0;
    }

}
