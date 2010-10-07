/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.twitter.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.voltdb.VoltDB;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.logging.VoltLogger;

public class ExportToHDFSClient extends ExportClientBase {

    private static final VoltLogger LOG = new VoltLogger("ExportToHDFSClient");

    private FileSystem hdfs;
    private String uri;
    private Map<String, ExportToHDFSDecoder> decoders;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            LOG.fatal("usage: [hdfs output dir] [server list (comma seperated)]");
            System.exit(1);
        }

        String servers = args[0];
        String hdfsOutputDir = args[1];

        new ExportToHDFSClient(servers, hdfsOutputDir).run();
    }

    public ExportToHDFSClient(String hdfsOutputDir, String servers) throws IOException {
        String uri = "hdfs://localhost:9000" + hdfsOutputDir;
        Path path = new Path(uri);
        FileSystem hdfs = FileSystem.get(URI.create(uri), new Configuration());
        if (hdfs.exists(path)) {
            LOG.fatal("Directory already exists: " + hdfsOutputDir);
            System.exit(1);
        }
        hdfs.mkdirs(path);

        List<InetSocketAddress> serversList = new LinkedList<InetSocketAddress>();
        for (String server : servers.split(",")) {
            serversList.add(new InetSocketAddress(server, VoltDB.DEFAULT_PORT));
        }
        setServerInfo(serversList);

        this.hdfs = hdfs;
        this.uri = uri;
        this.decoders = new HashMap<String, ExportToHDFSDecoder>();

        try {
            connectToExportServers("", "");
        } catch (IOException e) {
            LOG.fatal("Unable to connect to VoltDB servers for export");
            System.exit(1);
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        if (!decoders.containsKey(source.tableName())) {
            decoders.put(source.tableName(), new ExportToHDFSDecoder(source, hdfs, uri));
        }
        return decoders.get(source.tableName());
    }

}
