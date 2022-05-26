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

package nbbo;

import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

public class Symbols {

    static final BigDecimal BD10000 = new BigDecimal(10000);

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

        // Schema for CSV file
        final CellProcessor[] processors = new CellProcessor[] {
                new UniqueHashCode(), // Symbol
                new NotNull(), // Name
                new NotNull(), // LastSale
                new NotNull(), // MarketCap
                new NotNull(), // ADR TSO
                new NotNull(), // IPOyear
                new NotNull(), // Sector
                new NotNull(), // Industry
                new NotNull(), // Summary Quote
                new Optional() // blank column
        };

        ICsvMapReader mapReader = null;
        try {
            mapReader = new CsvMapReader(new FileReader(filename), CsvPreference.STANDARD_PREFERENCE);

            // the header columns are used as the keys to the Map
            final String[] header = mapReader.getHeader(true);

            Map<String, Object> tuple;
            int rowsRead = 0;
            while( (tuple = mapReader.read(header, processors)) != null ) {

                Symbol s = new Symbol();
                s.symbol = (String) tuple.get("Symbol");
                String price = (String) tuple.get("LastSale");
                if (price.equals("n/a")) {
                    price = "20";
                }
                BigDecimal priceBD = new BigDecimal(price);
                s.price = priceBD.multiply(BD10000).intValue();

                symbols.add(s);
                rowsRead++;
            }

            System.out.printf("Read %d rows from CSV file at: %s\n", rowsRead, filename);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        finally {
            if( mapReader != null ) {
                try { mapReader.close(); } catch (Exception e) {}
            }
        }
    }
}
