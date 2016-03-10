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

package bankoffers;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.voltdb.types.TimestampType;

public class OfferBenchmark extends BaseBenchmark {

    private Random rand = new Random();
    private long txnId = 0;
    private Long[] accounts;
    private String[] acct_states;
    private int[] amounts = {25,50,75,100,150,200,250,300};
    private PersonGenerator gen = new PersonGenerator();
    private String[] offers = {"$5 off any purchase over $25","20% off any purchase over $50","Extra 25% off sale items"};

    // constructor
    public OfferBenchmark(BenchmarkConfig config) {
        super(config);
    }

    // this gets run once before the benchmark begins
    @Override
    public void initialize() throws Exception {

        List<Long> acctList = new ArrayList<Long>(config.custcount*2);
        List<String> stList = new ArrayList<String>(config.custcount*2);

        // generate customers
        System.out.println("generating " + config.custcount + " customers...");
        for (int c=0; c<config.custcount; c++) {

            if (c % 10000 == 0) {
                System.out.println("  "+c);
            }

            PersonGenerator.Person p = gen.newPerson();
            //int ac = rand.nextInt(areaCodes.length);

            client.callProcedure(new BenchmarkCallback("CUSTOMER.insert"),
                                 "CUSTOMER.insert",
                                 c,
                                 p.firstname,
                                 p.lastname,
                                 "Anytown",
                                 p.state,
                                 p.phonenumber,
                                 p.dob,
                                 p.sex
                                 );

            int accts = rand.nextInt(5);
            for (int a=0; a<accts; a++) {

                int acct_no = (c*100)+a;
                client.callProcedure(new BenchmarkCallback("ACCOUNT.insert"),
                                     "ACCOUNT.insert",
                                     acct_no,
                                     c,
                                     rand.nextInt(10000),
                                     rand.nextInt(10000),
                                     new Date(),
                                     "Y"
                                     );
                acctList.add(Long.valueOf(acct_no));
                stList.add(p.state);
            }
        }

        accounts = acctList.toArray(new Long[acctList.size()]);
        acct_states = stList.toArray(new String[stList.size()]);

        // generate vendor offers
        System.out.println("generating " + config.vendorcount + " vendors...");
        for (int v=0; v<config.vendorcount; v++) {
            if (v % 10000 == 0) {
                System.out.println("  "+v);
            }

            client.callProcedure(new BenchmarkCallback("VENDOR_OFFERS.insert"),
                                 "VENDOR_OFFERS.insert",
                                 v,
                                 rand.nextInt(5)+1,
                                 0,
                                 rand.nextInt(5)+1,
                                 (double)rand.nextInt(100),
                                 0,
                                 offers[rand.nextInt(offers.length)]
                                 );
        }
    }

    @Override
    public void iterate() throws Exception {

        // pick a random account and generate a transaction
        int i = rand.nextInt(accounts.length);
        txnId++;
        long acctNo = accounts[i];
        double txnAmt = amounts[rand.nextInt(amounts.length)];
        String txnState = acct_states[i];
        String txnCity = "Some City";
        TimestampType txnTS = new TimestampType();
        int vendorId = rand.nextInt(config.vendorcount);
        // generate "out of state" fraud
        // a small % of the time, use a random state
        if (rand.nextInt(50000) == 0) {
            txnState = gen.randomState();
        }

        client.callProcedure(new BenchmarkCallback("CheckForOffers"),
                             "CheckForOffers",
                             txnId,acctNo,txnAmt,txnState,txnCity,txnTS,vendorId);

    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("OfferBenchmark",args);

        BaseBenchmark c = new OfferBenchmark(config);
        c.runBenchmark();
    }


}
