/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package fantasysports;

import java.util.Random;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.voltdb.*;
import org.voltdb.client.*;

public class Benchmark {

    // CONSTANTS
    private int nflPlayers = 300;
    private int userCount = 1000000;
    private int largeContests = 1;
    private int smallContests = 0;
    private int threads = 1;


    private Random rand = new Random();
    private Client client;


    public Benchmark(String servers) throws Exception {
        client = ClientFactory.createClient();
        String[] serverArray = servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }
    }


    public void init() throws Exception {

        // generate large contests
        System.out.println("Generating " + largeContests + " large contests...");
        for (int i=0; i<largeContests; i++) {
            client.callProcedure(new BenchmarkCallback("NFL_CONTEST_LARGE.upsert"),
                                 "NFL_CONTEST_LARGE.upsert",
                                 i,
                                 1
                                 );
        }

        // generate small contests
        // for (int i=0; i<smallContests; i++) {
        //     client.callProcedure(new BenchmarkCallback("NFL_CONTEST_SMALL.upsert"),
        //                          "NFL_CONTEST_SMALL.upsert",
        //                          i,
        //                          1
        //                          );
        // }

        // generate users
        System.out.println("Generating " + userCount + " users...");
        for (int i=0; i<userCount; i++) {
            client.callProcedure(new BenchmarkCallback("FANTASY_USER.upsert"),
                                 "FANTASY_USER.upsert",
                                 i,
                                 "User " + i
                                 );

            // TODO: roster may need to be a different table
            //int smallContest = rand.nextInt(smallContests);
            // each user has 9 (randomly selected) players on their roster
            // for (int j=0; j<9; j++) {
            //     client.callProcedure(new BenchmarkCallback("USER_CONTEST_ROSTER.upsert"),
            //                          "USER_CONTEST_ROSTER.upsert",
            //                          smallContest,
            //                          i,
            //                          rand.nextInt(nflPlayers)
            //                          );
            // }

            int largeContest = rand.nextInt(largeContests);
            // each user has 9 (randomly selected) players on their roster
            for (int j=0; j<9; j++) {
                client.callProcedure(new BenchmarkCallback("USER_CONTEST_ROSTER.upsert"),
                                     "USER_CONTEST_ROSTER.upsert",
                                     largeContest,
                                     i,
                                     rand.nextInt(nflPlayers),
                                     0
                                     );
            }
        }


    }


    public void runBenchmark() throws Exception {

        // for Run Everywhere procedures
        ArrayList<Integer> partitionKeys = new ArrayList<Integer>();
        VoltTable partitions = client.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];
        while (partitions.advanceRow()) {
            int p = (int)partitions.getLong(1);
            partitionKeys.add(p);
            System.out.println("Partition " + partitions.getLong(0) + " has key " + p);
        }

        // Run Ranker threads (see below) in a pool
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int i=0; i<1; i++) {

            // generate player stats
            System.out.println("Updating NFL player stats...");
            for (int p=0; p<nflPlayers; p++) {
                client.callProcedure(new BenchmarkCallback("NFL_PLAYER_GAME_SCORE.upsert"),
                                     "NFL_PLAYER_GAME_SCORE.upsert",
                                     p,
                                     1,
                                     rand.nextInt(50)
                                     );
            }


            // run everywhere
            // for (int partVal : partitionKeys) {
            //     client.callProcedure(new BenchmarkCallback("SelectAllScoresInPartition"),
            //                          "SelectAllScoresInPartition",
            //                          partVal);
            // }
            // for (int partVal : partitionKeys) {
            //     for (int c=0; c<largeContests; c++) {

            //         client.callProcedure(new BenchmarkCallback("SelectContestScoresInPartition"),
            //                              "SelectContestScoresInPartition",
            //                              partVal,
            //                              c);

                    // client.callProcedure(new BenchmarkCallback("UpsertUserScores"),
                    //                      "UpsertUserScores",
                    //                      partVal,
                    //                      c);

            //     }
            // }
            client.drain();

            System.out.println("Updating scores and rankings...");

            for (int c=0; c<largeContests; c++) {

                // run a Ranker
                Runnable r = new Ranker(partitionKeys, c, client);
                //r.run();
                executor.execute(r);

            }
        }

        // stop the executor & wait for any threads to finish
        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        client.drain();

        BenchmarkCallback.printAllResults();

        client.close();
    }


    public static void main(String[] args) throws Exception {

        String serverlist = "localhost";
        if (args.length > 0) { serverlist = args[0]; }
        Benchmark benchmark = new Benchmark(serverlist);
        if (args.length <= 1) {
            benchmark.init();
            benchmark.runBenchmark();
        } else {
            for (int i=1; i<args.length; i++) {
                String arg = args[i];
                if (arg.equals("init")) {
                    benchmark.init();
                }
                if (arg.equals("benchmark")) {
                    benchmark.runBenchmark();
                }
            }
        }
    }
}
