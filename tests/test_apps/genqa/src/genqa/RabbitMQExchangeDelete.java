/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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
