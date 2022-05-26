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
package org.voltdb.importclient.kafka.util;

import java.util.regex.Pattern;

public interface KafkaConstants {

    static final String CLIENT_ID = "voltdb-importer";
    static final String GROUP_ID = "voltdb";
    static final int KAFKA_DEFAULT_BROKER_PORT = 9092;

    // We don't allow period in topic names because we construct URIs using it
    static final Pattern TOPIC_LEGAL_NAMES_PATTERN = Pattern.compile("[a-zA-Z0-9\\_-]+");
    static final int TOPIC_MAX_NAME_LENGTH = 255;

    static int KAFKA_TIMEOUT_DEFAULT_MILLIS = 30000;
    static int KAFKA_BUFFER_SIZE_DEFAULT = 65536;

    static int IMPORT_GAP_LEAD = Integer.getInteger("KAFKA_IMPORT_GAP_LEAD", 32_768);

    static int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;
}
