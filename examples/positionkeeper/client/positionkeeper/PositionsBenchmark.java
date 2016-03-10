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

package positionkeeper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class PositionsBenchmark extends BaseBenchmark {

    private Random rand = new Random();
    private int order_id = 1;
    private int trade_id = 1;
    private int price_id = 1;
    private final int numSecs = 3000;
    private double[] prices;
    private int[][] positions;

    // constructor
    public PositionsBenchmark(BenchmarkConfig config) {
        super(config);

        prices = new double[numSecs];
        positions = new int[config.traders][config.secpercnt];

    }

    @Override
    public void initialize() throws Exception {

        // EXC
        for (int e=0; e<5; e++) {
            client.callProcedure(new BenchmarkCallback("EXC.insert"),"EXC.insert",e);
        }
        System.out.println("  loaded 5 records into EXC");

        // SEC
        for (int s=0; s<numSecs; s++) {
            client.callProcedure(new BenchmarkCallback("SEC.insert"),"SEC.insert",s,rand.nextInt(5));
            prices[s] = rand.nextInt(100);
        }
        System.out.println("  loaded "+numSecs+" records into SEC");

        // CNT
        for (int t=0; t<config.traders; t++) {
            client.callProcedure(new BenchmarkCallback("CNT.insert"),"CNT.insert",t);
        }
        System.out.println("  loaded "+config.traders+" records into CNT");

        // positions (client side only)
        for (int t=0; t<config.traders; t++) {

            // make a list of random securities with no duplicates
            List<Integer> secs = new ArrayList<Integer>();
            while(secs.size() < config.secpercnt) {
                int sec = rand.nextInt(numSecs);
                if(!secs.contains(sec))
                    secs.add(sec);
            }

            // put securities into an two-dimension array: positions[trader][index] = sec_id
            for (int i=0; i<config.secpercnt; i++) {
                positions[t][i] = secs.get(i);
            }
        }
        System.out.println("  initialized client with array of "+ config.traders * config.secpercnt +" possible positions");

    }

    @Override
    public void iterate() throws Exception {

        // always do a new price
        newPrice();

        // 1/10th of the time
        if (rand.nextInt(10) == 0)
            newOrder();

        // 1/50th of the time
        if (rand.nextInt(50) == 0)
            newTrade();

    }


    private void newOrder() throws Exception {
        // get a random trader
        int cnt = rand.nextInt(config.traders);

        // retrieve the sec of one of the positions for that trader
        // (so the total # of positions isn't growing beyond what was intended)
        int sec = positions[cnt][rand.nextInt(config.secpercnt)];

        // get the last price, modify it randomly
        double price = prices[sec] * (1+rand.nextGaussian()/100);
        // store the new price
        prices[sec] = price;

        // insert the order with a random qty
        client.callProcedure(new BenchmarkCallback("OrderInsert"),
                             "OrderInsert",
                             order_id++,
                             cnt,
                             sec,
                             (rand.nextInt(9)+1) * 10,
                             price);
    }

    private void newTrade() throws Exception {

        // get a random trader
        int cnt = rand.nextInt(config.traders);

        // retrieve the sec of one of the positions for that trader
        // (so the total # of positions is as planned)
        int sec = positions[cnt][rand.nextInt(config.secpercnt)];

        // get the last price, modify it randomly
        double price = prices[sec] * (1+rand.nextGaussian()/100);
        // store the new price
        prices[sec] = price;

        // insert the trade, with a random qty
        client.callProcedure(new BenchmarkCallback("TradeInsert"),
                             "TradeInsert",
                             trade_id++,
                             cnt,
                             sec,
                             (rand.nextInt(9)+1) * 10,
                             price);
    }

    private void newPrice() throws Exception {
        int sec = rand.nextInt(numSecs);
        double price = prices[sec] * (1+rand.nextGaussian()/100); // modify price randomly

        client.callProcedure(new BenchmarkCallback("PriceInsert"),
                             "PriceInsert",
                             price_id++,
                             sec,
                             price,
                             new Date());
    }


    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("PositionsBenchmark",args);

        BaseBenchmark benchmark = new PositionsBenchmark(config);
        benchmark.runBenchmark();
    }
}
