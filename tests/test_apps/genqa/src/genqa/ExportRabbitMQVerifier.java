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

package genqa;

import com.google_voltpatches.common.base.Charsets;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

/**
 * A RabbitMQ consumer that verifies the export data.
 */
public class ExportRabbitMQVerifier {
    private static final long VALIDATION_REPORT_INTERVAL = 10000;

    private final ConnectionFactory m_connFactory;
    private volatile long m_verifiedRows = 0;
    private String m_exchangeName;

    public ExportRabbitMQVerifier(String host, String username, String password, String vhost, String exchangename)
            throws IOException, InterruptedException
    {
        m_connFactory = new ConnectionFactory();
        m_connFactory.setHost(host);
        m_connFactory.setUsername(username);
        m_connFactory.setPassword(password);
        m_connFactory.setVirtualHost(vhost);
        m_exchangeName = exchangename;
    }

    public void run() throws IOException, InterruptedException
    {
        final Connection connection = m_connFactory.newConnection();
        final Channel channel = connection.createChannel();

        try {
            channel.exchangeDeclare(m_exchangeName, "topic", true);
            String dataQueue = channel.queueDeclare().getQueue();
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_PARTITIONED_TABLE.#");
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_PARTITIONED_TABLE2.#");
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_REPLICATED_TABLE.#");
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_PARTITIONED_TABLE_FOO.#");
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_PARTITIONED_TABLE2_FOO.#");
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_REPLICATED_TABLE_FOO.#");
            String doneQueue = channel.queueDeclare().getQueue();
            channel.queueBind(doneQueue, m_exchangeName, "EXPORT_DONE_TABLE.#");
            channel.queueBind(doneQueue, m_exchangeName, "EXPORT_DONE_TABLE_FOO.#");

            // Setup callback for data stream
            channel.basicConsume(dataQueue, false, createConsumer(channel));

            // Setup callback for the done message
            QueueingConsumer doneConsumer = new QueueingConsumer(channel);
            channel.basicConsume(doneQueue, true, doneConsumer);

            // Wait until the done message arrives, then verify count
            final QueueingConsumer.Delivery doneMsg = doneConsumer.nextDelivery();
            final long expectedRows = Long.parseLong(ExportOnServerVerifier.RoughCSVTokenizer
                    .tokenize(new String(doneMsg.getBody(), Charsets.UTF_8))[6]);

            while (expectedRows > m_verifiedRows) {
                Thread.sleep(1000);
                System.err.println("Expected " + expectedRows + " " + m_verifiedRows);
            }
        } finally {
            tearDown(channel);
            channel.close();
            connection.close();
        }
    }

    private Consumer createConsumer(final Channel channel)
    {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException
            {
                long deliveryTag = envelope.getDeliveryTag();

                String row[] = ExportOnServerVerifier.RoughCSVTokenizer.tokenize(new String(body, Charsets.UTF_8));
                if (row.length != 29) {
                    return;
                }
                ExportOnServerVerifier.ValidationErr err = null;
                try {
                    err = ExportOnServerVerifier.verifyRow(row);
                } catch (ExportOnServerVerifier.ValidationErr validationErr) {
                    validationErr.printStackTrace();
                }
                if (err != null) {
                    System.out.println("ERROR in validation: " + err.toString());
                }

                if (++m_verifiedRows % VALIDATION_REPORT_INTERVAL == 0) {
                    System.out.println("Verified " + m_verifiedRows + " rows.");
                }

                channel.basicAck(deliveryTag, false);
            }
        };
    }

    private static void tearDown(Channel channel) throws IOException
    {
        channel.exchangeDelete("testExchange");
    }

    private static void usage()
    {
        System.out.println("Command-line arguments: rabbitmq_server username password virtual_host");
    }

    public static void main(String[] args) throws IOException, InterruptedException
    {
        if (args.length != 5) {
            usage();
            System.exit(-1);
        }

        final ExportRabbitMQVerifier verifier =
                new ExportRabbitMQVerifier(args[0], args[1], args[2], args[3], args[4]);
        verifier.run();
    }
}
