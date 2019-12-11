package genqa;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQExchangeDelete {
    public static void main(String[] args) throws IOException
    {
        if (args.length < 5) {
            System.err.println("ERROR: usage: RabbitMQExchangeDelete host user password vhost exhangeName [exchangeName ...]");
            System.exit(1);
        }


        final String host = args[0];
        final String username = args[1];
        final String password = args[2];
        final String vhost = args[3];

        final ConnectionFactory m_connFactory;

        m_connFactory = new ConnectionFactory();
        m_connFactory.setHost(host);
        m_connFactory.setUsername(username);
        m_connFactory.setPassword(password);
        m_connFactory.setVirtualHost(vhost);

        final Connection connection = m_connFactory.newConnection();
        final Channel channel = connection.createChannel();
        for (int p = 4; p < args.length; p++) {
            final String exchangeName = args[p];
            System.out.format("deleting exchange: %s/%s\n", vhost, exchangeName);
            // doesn't complain if one attempts to delete an exchange that doesn't exist yet
            channel.exchangeDelete(exchangeName);
        }
        channel.close();
        connection.close();
    }
}
