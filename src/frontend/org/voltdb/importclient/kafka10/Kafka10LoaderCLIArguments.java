/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

public class Kafka10LoaderCLIArguments extends BaseKafkaLoaderCLIArguments {

    @Option(shortOpt = "n", desc = "Number of Kafka consumers.")
    public int consumercount = 1;

    public Kafka10LoaderCLIArguments(PrintWriter pw) {
        super(pw);
        this.warningWriter = pw;
    }

    public Kafka10LoaderCLIArguments() {
        super();
    }

    public int getConsumerCount() {
        return consumercount;
    }

    @Override
    public void validate() {
        super.validate();
    }
}
