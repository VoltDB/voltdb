/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
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
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
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
            connectToELServers("", "");
        } catch (IOException e) {
            LOG.fatal("Unable to connect to VoltDB servers for export");
            System.exit(1);
        }
    }

    @Override
    public ExportDecoderBase constructELTDecoder(AdvertisedDataSource source) {
        if (!decoders.containsKey(source.tableName())) {
            decoders.put(source.tableName(), new ExportToHDFSDecoder(source, hdfs, uri));
        }
        return decoders.get(source.tableName());
    }

}
