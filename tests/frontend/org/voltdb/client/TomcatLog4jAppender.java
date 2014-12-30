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

package org.voltdb.client;

import junit.framework.Assert;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableTestHelpers;
import org.voltdb.tomcat.TomcatVoltdbAppender;

public class TomcatLog4jAppender {

	private MessagePrinter printer;
	private ServerThread m_localServer;
	private Client m_client;

	// A class to print out a bunch of messages
	static class MessagePrinter {
		private static Logger log = Logger.getLogger(MessagePrinter.class);

		public void printMessages() {
			log.info("This is a message");
			log.info("This is another message");
		}
	}

	// Configure & start a blank volt instance
	private void startServer() {
		// Create a configuration
		VoltDB.Configuration config = new VoltDB.Configuration();

		// Start the server
		m_localServer = new ServerThread(config);
        m_localServer.start();
        m_localServer.waitForInitialization();

	}

	// Start a volt client
	private void startClient() throws Exception{
		m_client = ClientFactory.createClient();
		m_client.createConnection("localhost");
	}

	// Stop a running volt server
	private void stopServer() throws Exception{
		if (m_localServer != null) {
            m_localServer.shutdown();
            m_localServer.join();
            m_localServer = null;
        }
	}

	// Stop a running volt client
	private void stopClient() throws Exception {
		if (m_client != null) {
            m_client.close();
            m_client = null;
        }
	}

	// Set up the volt instance, writer class and its logger
	@Before
	public void setup() {
		// Volt
		try{
			startServer();
			startClient();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// The printer
		printer = new MessagePrinter();

		// Its logger
		Logger rootLogger = Logger.getRootLogger();
		Appender voltAppender = new TomcatVoltdbAppender();
		rootLogger.removeAllAppenders();
		rootLogger.setLevel(Level.INFO);
		rootLogger.addAppender(voltAppender);
	}

	@Test
	public void test() throws Exception{
		try {
			// Print our messages
			printer.printMessages();
			Thread.sleep(10000);

			// Make sure that we have a bunch of messages in volt
			VoltTable tables = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
	        boolean found = VoltTableTestHelpers.moveToMatchingRow(tables, "TABLE_NAME", "name");
	        Assert.assertTrue(found);
		} finally {
			// We're done, turn off the server
			stopClient();
			stopServer();
		}
	}

}
