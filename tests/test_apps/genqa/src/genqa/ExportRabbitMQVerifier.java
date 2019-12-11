/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import org.voltcore.logging.VoltLogger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * A RabbitMQ consumer that verifies the export data.
 */
public class ExportRabbitMQVerifier {
    static VoltLogger log = new VoltLogger("ExportRabbitMQVerifier");
    private static final long VALIDATION_REPORT_INTERVAL = 10000;
    private final long MAX_STALL_TIMEOUT_SECS;
    private final ConnectionFactory m_connFactory;
    private String m_exchangeName;
    private boolean success = true;
    private final ConcurrentHashMap<Long,AtomicInteger> rowIdCounts = new ConcurrentHashMap<Long,AtomicInteger>();

    public ExportRabbitMQVerifier(String host, String username, String password, String vhost, String exchangename)
            throws IOException, InterruptedException
    {
        m_connFactory = new ConnectionFactory();
        m_connFactory.setHost(host);
        m_connFactory.setUsername(username);
        m_connFactory.setPassword(password);
        m_connFactory.setVirtualHost(vhost);
        m_exchangeName = exchangename;
        MAX_STALL_TIMEOUT_SECS = 1200;
    }

    public void run() throws IOException, InterruptedException, TimeoutException
    {
        final Connection connection = m_connFactory.newConnection();
        final Channel channel = connection.createChannel();

        try {
            channel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException cause) {
                    log.info("shutdownCompleted, cause: " + cause.toString());
                }
            });
            channel.exchangeDeclare(m_exchangeName, "topic", true);
            String dataQueue = channel.queueDeclare().getQueue();
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_PARTITIONED_TABLE_RABBIT.#");
            channel.queueBind(dataQueue, m_exchangeName, "EXPORT_REPLICATED_TABLE_RABBIT.#");
            String doneQueue = channel.queueDeclare().getQueue();
            channel.queueBind(doneQueue, m_exchangeName, "EXPORT_DONE_TABLE_RABBIT.#");

            // Setup callback for data stream
            channel.basicConsume(dataQueue, false, createConsumer(channel));

            // Setup callback for the done message
            QueueingConsumer doneConsumer = new QueueingConsumer(channel);
            channel.basicConsume(doneQueue, true, doneConsumer);

            // Wait until the done message arrives, then verify count
            final QueueingConsumer.Delivery doneMsg = doneConsumer.nextDelivery();
            final long expectedRows = Long.parseLong(RoughCSVTokenizer
                    .tokenize(new String(doneMsg.getBody(), Charsets.UTF_8))[6]);
            log.info(String.format("populator has finished creating %d rows", expectedRows));

            long startTime = System.currentTimeMillis();
            long lastRcvd = 0;
            while (expectedRows > rowIdCounts.size()) {
                long rcvdRows = rowIdCounts.size();
                log.info(String.format(
                                       "Expecting %d rows, received %d",
                                       expectedRows,
                                       rcvdRows));
                long now = System.currentTimeMillis();
                if (rcvdRows > lastRcvd) {
                    lastRcvd = rcvdRows;
                    startTime = now;
                } else if (now - startTime > MAX_STALL_TIMEOUT_SECS*1000) {
                    success = false;
                    log.error(String.format("export stalled for %d minutes, failing", MAX_STALL_TIMEOUT_SECS));
                    break;
                }
                Thread.sleep(5000);
            }

            final long totalRcvd = rowIdCounts.reduceValues(1, AtomicInteger::get,  (l, r) -> l + r);
            log.info(String.format(
                                   "rows expected: %d, verified: %d, received: %d, duplicates: %d",
                                   expectedRows,
                                   rowIdCounts.size(),
                                   totalRcvd,
                                   totalRcvd - rowIdCounts.size()
                                   ));
        } finally {
            log.info("close channel");
            channel.close();
            connection.close();
        }
        if ( ! success ) {
            System.exit(1);
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

                String row[] = RoughCSVTokenizer.tokenize(new String(body, Charsets.UTF_8));
                if (row.length != 29) {
                    return;
                }
                ValidationErr err = null;
                try {
                    err = ExportOnServerVerifier.verifyRow(row);
                } catch (ValidationErr validationErr) {
                    validationErr.printStackTrace();
                }
                if (err != null) {
                    log.info("ERROR in validation: " + err.toString());
                    success = false;
                }

                Long rowid = Long.parseLong(row[7]);
                AtomicInteger counter = rowIdCounts.computeIfAbsent(rowid, k -> new AtomicInteger());
                counter.incrementAndGet();

                int rows_verified = rowIdCounts.size();
                if (rows_verified % VALIDATION_REPORT_INTERVAL == 0) {
                    log.info("Verified " + rows_verified + " rows.");
                }

                channel.basicAck(deliveryTag, false);
            }
        };
    }

    private static void usage()
    {
        log.info("Command-line arguments: rabbitmq_server username password virtual_host exchange_name");
    }

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException
    {
        VoltLogger log = new VoltLogger("ExportRabbitMQVerifier.main");

        if (args.length != 5) {
            usage();
            System.exit(1);
        }

        final ExportRabbitMQVerifier verifier =
                new ExportRabbitMQVerifier(args[0], args[1], args[2], args[3], args[4]);
        verifier.run();
    }
}
