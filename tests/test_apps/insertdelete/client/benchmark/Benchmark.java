package benchmark;

import java.util.Random;

import org.voltdb.*;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.*;

public class Benchmark {

    protected final InsertDeleteConfig config;
    private Client client;
    private Random rand = new Random();
    private BenchmarkStats stats;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class InsertDeleteConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        int displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 300;

        @Option(desc = "Comma separated list of the form server[:port] to connect to database for queries.")
        String servers = "localhost";

        @Option(desc = "If true (default), leave the database empty; otherwise, seed it with data.")
        boolean empty = true;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "insertdelete.csv";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (servers.length() == 0) servers = "localhost";
            if (statsfile.length() == 0) statsfile = "insertdelete.csv";
        }
    }

    public Benchmark(String[] args, boolean emptyDefault) throws Exception {
        config = new InsertDeleteConfig();
        config.empty = emptyDefault;
        config.parse(Benchmark.class.getName(), args);

        client = ClientFactory.createClient();
        String[] serverArray = config.servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }

        stats = new BenchmarkStats(client);
    }

    public Benchmark(String[] args) throws Exception {
        this(args, true);
    }


    public void init() throws Exception {

        if (!config.empty) {
            SeedTables.seedTables(config.servers);
        }

    }


    public void runBenchmark() throws Exception {

        stats.startBenchmark(config.displayinterval);

        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000)+500; // add 500 to avoid low values that may be used for seed data
            client.callProcedure(new BenchmarkCallback("InsertDeleteWithString"),
                                 "InsertDeleteWithString",
                                 appid,
                                 deviceid,
                                 "They’re selling postcards of the hanging. They’re painting the passports brown. The beauty parlor is filled with sailors. The circus is in town"
                                 );


            appid = rand.nextInt(50);
            deviceid = rand.nextInt(1000000)+500; // add 500 to avoid low values that may be used for seed data
            client.callProcedure(new BenchmarkCallback("InsertDelete"),
                                 "InsertDelete",
                                 appid,
                                 deviceid
                                 );

        }

        stats.endBenchmark(config.statsfile);

        client.drain();
        BenchmarkCallback.printAllResults();

        client.close();
    }


    public static void main(String[] args) throws Exception {

        Benchmark benchmark = new Benchmark(args);
        benchmark.init();
        benchmark.runBenchmark();

    }
}
