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
            if ( "".equals(file)) {
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
