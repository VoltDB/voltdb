/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.importclient.kafka10;

import java.io.PrintWriter;

import org.voltdb.importclient.kafka.util.BaseKafkaLoaderCLIArguments;

public class KafkaLoaderCLIArguments extends BaseKafkaLoaderCLIArguments {

    @Option(shortOpt = "n", desc = "Number of Kafka consumers.")
    public int consumercount = 1;

    @Option(desc = "Maximum delay between polling messages from brokers (default: 300000ms)")
    public int maxpollinterval = 300000;

    @Option(desc = "Maximum number of records returned in a single fetch call (default: 500)")
    public int maxpollrecords = 500;

    @Option(desc = "Consumer session timeout (default: 20000ms)")
    public int maxsessiontimeout = 20000;

    @Option(desc = "Maximum amount of time the client will wait for a response (default: 305000ms)")
    public int maxrequesttimeout = 305000;

    public KafkaLoaderCLIArguments(PrintWriter pw) {
        super(pw);
        this.warningWriter = pw;
    }

    public KafkaLoaderCLIArguments() {
        super();
    }

    public int getConsumerCount() {
        return consumercount;
    }

    public int getMaxPollInterval() {
        return maxpollinterval;
    }

    public int getMaxPollRecords() {
        return maxpollrecords;
    }

    public int getSessionTimeout() {
        return maxsessiontimeout;
    }

    public int getRequestTimeout() {
        return maxrequesttimeout;
    }

    @Override
    public void validate() {
        super.validate();
    }
}
