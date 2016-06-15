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
        @Option(desc = "Comma separated list of the form server[:port] to connect to database for queries.")
        String servers = "localhost";

        @Option(desc = "If true (default), leave the database empty; otherwise, seed it with data.")
        boolean empty = true;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "insertdelete.csv";
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


    public void init(boolean empty, String servers) throws Exception {

        if (!empty) {
            SeedTables.seedTables(servers);
        }

    }


    public void runBenchmark(String statsfile) throws Exception {

        stats.startBenchmark();

        for (int i=0; i<1000000; i++) {

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

        stats.endBenchmark(statsfile);

        client.drain();
        BenchmarkCallback.printAllResults();

        client.close();
    }


    public static void main(String[] args) throws Exception {

        Benchmark benchmark = new Benchmark(args);
        benchmark.init(benchmark.config.empty, benchmark.config.servers);
        benchmark.runBenchmark(benchmark.config.statsfile);

    }
}
