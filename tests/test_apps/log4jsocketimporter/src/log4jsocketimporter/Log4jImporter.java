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


/**
 * Main class that starts socket listener to listen for log4j socket log events.
 *
 */
public class Log4jImporter
{
    public static void main(String[] args) throws IOException
    {
        if (args.length < 2) {
            showUsageAndExit();
        }

        int log4jListenerPort = 0;
        try {
            log4jListenerPort = Integer.parseInt(args[0]);
        } catch(NumberFormatException e) {
            System.err.println("Invalid number specified for port: " + args[0]);
            showUsageAndExit();
        }

        new Log4jSocketListener(log4jListenerPort, args[1]).start(); // any startup error gets thrown back to user
    }

    private static void showUsageAndExit() {
        System.err.println("Usage: java log4jsocketimporter.Importer log4jListenerPort voltHost:voltPort");
        System.exit(1);
    }
}
