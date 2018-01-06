/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.importclient.kafka;

import java.io.PrintWriter;
import org.voltdb.importclient.kafka.util.BaseKafkaLoaderCLIArguments;

/*
 * Process command line arguments and do some validation.
 */
public class KafkaExternalLoaderCLIArguments extends BaseKafkaLoaderCLIArguments {

    public KafkaExternalLoaderCLIArguments(PrintWriter pw) {
        super(pw);
        this.warningWriter = pw;
    }

    public KafkaExternalLoaderCLIArguments() {
       super();
    }

    @Option(shortOpt = "k", desc = "Number of Kafka Topic Partitions. Deprecated; value is ignored.")
    public int kpartitions = 0;

    @Override
    public void validate() {
        super.validate();

        if (kpartitions !=0) {
            warningWriter.println("Warning: --kpartions argument is deprecated, value is ignored.");
        }

    }
}
