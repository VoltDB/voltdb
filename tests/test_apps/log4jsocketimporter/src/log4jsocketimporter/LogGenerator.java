/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Starts multiple threads logging to log4j.
 *
 */
public class LogGenerator
{
    private static Logger[] s_loggers= {
        Logger.getLogger("blue"),
        Logger.getLogger("black"),
        Logger.getLogger("yellow"),
        Logger.getLogger("green")
    };

    public static void main(String[] args) throws IOException
    {
        // start 10 threads that log
        int numThreads = 10;
        System.out.println("Starting " + numThreads + " threads logging messages");
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i=0; i<numThreads; i++) {
            executor.submit(new LoggingRunnable(i));
        }
    }

    private static class LoggingRunnable implements Runnable
    {
        private static Level[] levels = { Level.ALL, Level.DEBUG, Level.ERROR, Level.FATAL, Level.INFO, Level.OFF, Level.TRACE, Level.WARN };

        private int m_id;

        public LoggingRunnable(int id)
        {
            m_id = id;
        }

        @Override
        public void run()
        {
            System.out.println("Thread " + m_id + " starting logging...");
            Random random = new Random();
            int count = 0;
            while (true) {
                Logger logger = s_loggers[random.nextInt(s_loggers.length)];
                String msg = String.format("From logger %d - Count %d", m_id, count++);
                if (count%5==0) {
                    logger.log(levels[random.nextInt(levels.length)], msg, new Exception("test exception from " + m_id));
                } else {
                    logger.log(levels[random.nextInt(levels.length)], msg);
                }
                /*
                try { Thread.sleep(random.nextInt(10)*100); } catch(InterruptedException e) {}
                if (count%25==0) {
                    System.out.println("Thread " + m_id + " logged " + count + " messages");
                }
                */
            }
        }
    }
}
