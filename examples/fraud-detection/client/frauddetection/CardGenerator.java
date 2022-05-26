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

package frauddetection;

import org.voltdb.CLIConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class CardGenerator {
    public static class Config extends CLIConfig {
        @Option(desc = "Number of Cards.")
        int cardcount = 500000;

        @Option(desc = "File output location.")
        String output = "data/cards.csv";

        @Override
        public void validate() {
            if (cardcount < 0) exitWithMessageAndUsage("cardcount must be > 0");
        }
    }

    private static void gencards(Config config) throws IOException
    {
        final BufferedWriter writer = new BufferedWriter(new FileWriter(config.output));
        for (int i = 0; i < config.cardcount; i++) {
            final StringBuilder sb = new StringBuilder();
            sb.append(i).append(",1,0,100000,2020-01-01 12:00:00,Rider").append(i).append(",7815551212,Rider").append(i).append("@test.com,0\n");
            i++;
            sb.append(i).append(",1,0,50000,2020-01-01 12:00:00,Rider").append(i).append(",7815551212,Rider").append(i).append("@test.com,0\n");
            i++;
            sb.append(i).append(",1,0,5000,2020-01-01 12:00:00,Rider").append(i).append(",7815551212,Rider").append(i).append("@test.com,0\n");
            i++;
            sb.append(i).append(",1,0,2000,2020-01-01 12:00:00,Rider").append(i).append(",7815551212,Rider").append(i).append("@test.com,0\n");
            i++;
            sb.append(i).append(",1,0,1000,2020-01-01 12:00:00,Rider").append(i).append(",7815551212,Rider").append(i).append("@test.com,0\n");
            i++;
            sb.append(i).append(",1,0,500,2020-01-01 12:00:00,Rider").append(i).append(",7815551212,Rider").append(i).append("@test.com,0\n");
            writer.write(sb.toString());
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception
    {
        final Config config = new Config();
        config.parse(CardGenerator.class.getName(), args);

        gencards(config);
    }
}
