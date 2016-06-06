package benchmark;

import java.util.Random;
import org.voltdb.*;
import org.voltdb.client.*;

public class Benchmark {

    private Client client;
    private Random rand = new Random();
    private BenchmarkStats stats;


    public Benchmark(String servers) throws Exception {
        client = ClientFactory.createClient();
        String[] serverArray = servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }
        stats = new BenchmarkStats(client);
    }


    public void init() throws Exception {

        // any initial setup can go here

    }


    public void runBenchmark() throws Exception {

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

        stats.endBenchmark();

        client.drain();
        BenchmarkCallback.printAllResults();

        client.close();
    }


    public static void main(String[] args) throws Exception {

        String serverlist = "localhost";
        if (args.length > 0) {
            serverlist = args[0];
        }
        Benchmark benchmark = new Benchmark(serverlist);
        benchmark.init();
        benchmark.runBenchmark();

    }
}
