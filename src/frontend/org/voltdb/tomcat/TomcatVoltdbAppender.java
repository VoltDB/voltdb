package org.voltdb.tomcat;

import java.util.ArrayList;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.voltdb.client.*;

public /*static*/ class TomcatVoltdbAppender extends AppenderSkeleton implements Appender {
	ClientConfig config = null;
	Client client = null;
	ArrayList<LoggingEvent> cache = new ArrayList<LoggingEvent>();
	
	public TomcatVoltdbAppender() {
		// Create a connection to Volt
		try {
			config = new ClientConfig("", "");
			config.setReconnectOnConnectionLoss(true);
			client = ClientFactory.createClient(config);
			client.createConnection("localhost");
		} catch (java.io.IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	@Override
	public void close() {
		// Close the Volt connection
		try {
			client.drain();
			client.close();
		} catch (InterruptedException | NoConnectionsException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean requiresLayout() {
		return true;
	}

	@Override
	protected void append(LoggingEvent arg0) {
		// Extract the message information we need
		String message = arg0.getMessage().toString();
		
		// Insert the log message into Volt
		try{
			client.callProcedure("VoltdbInsert", message);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	

}