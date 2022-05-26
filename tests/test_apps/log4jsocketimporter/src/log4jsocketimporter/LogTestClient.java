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

package log4jsocketimporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Starts multiple threads logging to log4j.
 * The log statements are of the form "<timetakenMillis>" to make it easy to use this in a demo app.
 * Also starts a input reader loop that calculates some operation time statistics based on user input.
 */
public class LogTestClient
{
    public static void main(String[] args) throws IOException
    {
        // start 10 threads that log
        int numThreads = 10;
        System.out.println("Starting " + numThreads + " threads logging messages");
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i=0; i<numThreads; i++) {
            executor.submit(new LoggingRunnable(i));
        }

        LogAnalyzer analyzer = new LogAnalyzer(args.length==0 ? "localhost" : args[0]);

        // Read user input and get statistics on operations
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.print("Enter operation to analyze or Exit/Quit to quit: ");
            String input = reader.readLine();
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                break;
            }
            analyzer.analyzeOperation(input);
        }

        System.exit(0);
    }

    /**
     * Runnable that generates logs in different categories and with different messages.
     */
    private static class LoggingRunnable implements Runnable
    {
        private static final Level[] levels =
            { Level.DEBUG, Level.ERROR, Level.FATAL, Level.INFO, Level.TRACE, Level.WARN };

        private static final OperationInfo[] s_operations = {
            new OperationInfo(Logger.getLogger("xs"), 25, 10),
            new OperationInfo(Logger.getLogger("small"), 100, 40),
            new OperationInfo(Logger.getLogger("medium"), 500, 100),
            new OperationInfo(Logger.getLogger("large"), 1000, 300)
        };

        @SuppressWarnings("unused")
        private final int m_id;

        public LoggingRunnable(int id)
        {
            m_id = id;
        }

        @Override
        public void run()
        {
            Random random = new Random();
            int count = 0;
            while (true) {
                /*
                String msg = String.format("From logger %d - Count %d", m_id, count++);
                if (count%5==0) {
                    logger.log(levels[random.nextInt(levels.length)], msg, new Exception("test exception from " + m_id));
                } else {
                */
                // About 20% of the time, generate times that go over the threshold
                boolean exceedThreshold = random.nextInt(5)==0;
                OperationInfo op = s_operations[count%s_operations.length];
                int opTimeTaken = exceedThreshold ? (op.thresholdTime + random.nextInt(op.timeDelta)) : random.nextInt(op.thresholdTime);
                op.logger.log(levels[random.nextInt(levels.length)], opTimeTaken);
                //}
                count++;
            }
        }
    }

    private static class OperationInfo
    {
        final Logger logger;
        final int thresholdTime;
        final int timeDelta;

        public OperationInfo(Logger logger, int thresholdTime, int timeDelta)
        {
            this.logger = logger;
            this.thresholdTime = thresholdTime;
            this.timeDelta = timeDelta;
        }
    }
}
