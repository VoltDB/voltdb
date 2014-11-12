/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package adhocddl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.voltdb.CLIConfig;

public class Initializer {

    final InitConfig config;

    /**
     * Config to generate initial catalog
     * @author yhe
     *
     */
    static class InitConfig extends CLIConfig {
        @Option(desc = "Number of tables in server")
        int numOfTables = 200;

        @Option(desc = "Number of SPs per table in server")
        int numOfSPs = 4;

        @Option(desc = "Table name prefix")
        String prefix = "T";

        @Option(desc = "Number of Columns in each table")
        int numOfCols = 10;

        @Option(desc = "Percentage of indexed columns in the ramdonly generated table")
        double idxPercent = 0.1;
    }

    /**
     * Default constructor
     * @param config
     * @throws IOException
     */
    public Initializer(InitConfig config) throws IOException {
        this.config = config;
        System.out.println(config.getConfigDumpString());
    }

    /**
     * Generate the init ddl.sql file
     * @throws IOException
     */
    public void init() throws IOException {
        DDLGenerator DDLGen = new DDLGenerator(config.numOfCols, config.idxPercent);
        FileOutputStream fos = new FileOutputStream(new File("ddl.sql"));

        String ddl;
        for(int i = 0; i < config.numOfTables; i++)
        {
            ddl = DDLGen.CreateTable(i, config.prefix) + "\n\n";
            fos.write(ddl.getBytes());
            for(int j = 0; j < config.numOfSPs; j++)
            {
                ddl = DDLGen.CreateProcedure(j, i, config.prefix) + "\n\n";
                fos.write(ddl.getBytes());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        InitConfig config = new InitConfig();
        config.parse(Initializer.class.getName(), args);

        Initializer initializer = new Initializer(config);
        initializer.init();
    }
}
