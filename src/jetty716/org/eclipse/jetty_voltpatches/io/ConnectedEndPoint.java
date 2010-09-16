package org.eclipse.jetty_voltpatches.io;

public interface ConnectedEndPoint extends EndPoint
{
    Connection getConnection();
    void setConnection(Connection connection);
}
