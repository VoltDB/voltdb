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

package osmimport;

import java.io.File;

import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.FastXmlReader;
import org.voltdb.CLIConfig;

public class OSMImport implements Runnable {
    public File input;
    private OSMConfig config;

    public OSMImport(OSMConfig config) {
        this.config = config;
    }

    public void run() {
        CompressionMethod compressionMethod = CompressionMethod.None;
        Sink sink = new VoltDBOsmSink(config.server);
        FastXmlReader fxr = new FastXmlReader(input, false, compressionMethod);
        fxr.setSink(sink);
        fxr.run();

    }

    /**
     * Uses included {@link CLIConfig} class to declaratively state command line
     * options with defaults and validation.
     */
    static class OSMConfig extends CLIConfig {

        @Option(desc = "the server to connect to.")
        String server = "localhost";

        @Option(desc = "The path to the .osm file to import")
        String file = "";

        @Override
        public void validate() {
            if ("".equals(file)) {
                System.err.println("you need to specify a valid path to an .osm file");
                System.exit(1);
            }
        }
    }

    public static void main(String[] args) {
        OSMConfig config = new OSMConfig();
        config.parse(OSMImport.class.getName(), args);

        OSMImport osm = new OSMImport(config);

        osm.input = new File(config.file);
        osm.run();
    }

}
