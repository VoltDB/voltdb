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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class Symbols {


    private Random rand = new Random();

    public class Symbol {
        public String symbol;
        public int price;

        @Override
        public String toString() {
            return symbol + " = " + price;
        }
    }

    private ArrayList<Symbol> symbols = new ArrayList<Symbol>();

    public Symbol getRandom() {
        int i = rand.nextInt(symbols.size());
        return incrementAndGet(i);
    }

    public Symbol incrementAndGet(int index) {
        Symbol s = symbols.get(index);
        if (rand.nextInt(10) == 0) {
            s.price = (int)Math.round(s.price * (1+rand.nextGaussian()/2000));

            // don't allow price to fall to zero
            if (s.price < 100) {
                s.price = 100;
            }
        }
        return s;
    }

    public void loadFile(String filename) {
        try {
            FileReader fileReader = new FileReader(filename);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            CsvLineParser parser = new CsvLineParser();

            // skip first line (headers)
            String line = bufferedReader.readLine();

            // read remaining lines
            int i=0;
            Iterator<String> it;
            BigDecimal bd10000 = new BigDecimal(10000);
            while ((line = bufferedReader.readLine()) != null) {
                i++;
                it = parser.parse(line);
                Symbol s = new Symbol();
                s.symbol = it.next();
                it.next(); // skip name
                String price = it.next();
                if (price.equals("n/a")) {
                    price = "20";
                }
                BigDecimal priceBD = new BigDecimal(price);
                s.price = priceBD.multiply(bd10000).intValue();

                symbols.add(s);
            }
            bufferedReader.close();
            System.out.println("read " + i + " lines");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        Symbols s = new Symbols();
        s.loadFile("data/NYSE.csv");

        for (int i=0; i<20; i++) {
            Symbol sym = s.incrementAndGet(100);
            System.out.println(sym);
        }
    }
}
