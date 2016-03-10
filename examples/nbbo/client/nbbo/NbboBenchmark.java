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

package nbbo;

import java.util.Random;
import java.math.BigDecimal;
import java.math.MathContext;
import org.voltdb.types.TimestampType;

public class NbboBenchmark extends BaseBenchmark {

    private Random rand = new Random();
    private MathContext mc = new MathContext(2);
    private BigDecimal bd0 = new BigDecimal(0);
    private long startTime = new TimestampType(System.currentTimeMillis()*1000).getTime();

    Symbols symbols;
    String[] exchanges = {"AM",
                          "BO",
                          "CI",
                          "IS",
                          "JA",
                          "KX",
                          "MW",
                          "NA",
                          "ND",
                          "NY",
                          "PB",
                          "PC",
                          "WT",
                          "YB",
                          "ZB"
    };
    int[] sizes = {100,200,500,1000,2000};
    long seq = 0l;

    // constructor
    public NbboBenchmark(BenchmarkConfig config) {
        super(config);

    }

    public void initialize() throws Exception {
        symbols = new Symbols();
        symbols.loadFile("data/NYSE.csv");
        symbols.loadFile("data/NASDAQ.csv");
        symbols.loadFile("data/AMEX.csv");
    }

    public void iterate() throws Exception {

        Symbols.Symbol s = symbols.getRandom();
        int ask = (int)Math.round(s.price * (1+rand.nextFloat()/20));
        int bid = (int)Math.round(s.price * (1-rand.nextFloat()/20));

        String exch = exchanges[rand.nextInt(exchanges.length)];

        client.callProcedure(new BenchmarkCallback("ProcessTick"),
                             "ProcessTick",
                             s.symbol,
                             new TimestampType(),
                             seq++, //seq_number
                             exch, // exchange
                             bid, //bid_price
                             sizes[rand.nextInt(sizes.length)], //bid_size
                             ask, //ask_price
                             sizes[rand.nextInt(sizes.length)] //ask_size
                             );
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("NbboBenchmark",args);

        BaseBenchmark benchmark = new NbboBenchmark(config);
        benchmark.runBenchmark();

    }
}
